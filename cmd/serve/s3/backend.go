package s3

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/big"
	"mime"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/johannesboyne/gofakes3"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/walk"
	"github.com/rclone/rclone/vfs"
	"github.com/ryszard/goskiplist/skiplist"
)

var (
	emptyPrefix = &gofakes3.Prefix{}
	timeFormat  = "Mon, 2 Jan 2006 15:04:05.999999999"
	add1        = new(big.Int).SetInt64(1)
)

type SimpleBucketBackend struct {
	opt      *Options
	lock     sync.Mutex
	fs       *vfs.VFS
	uploads  map[string]*bucketUploads
	uploadID *big.Int
}

// newBackend creates a new SimpleBucketBackend.
func newBackend(fs *vfs.VFS, opt *Options) gofakes3.Backend {
	return &SimpleBucketBackend{
		fs:       fs,
		opt:      opt,
		uploadID: new(big.Int),
		uploads:  make(map[string]*bucketUploads),
	}
}

// ListBuckets always returns the default bucket.
func (db *SimpleBucketBackend) ListBuckets() ([]gofakes3.BucketInfo, error) {
	dirEntries, err := getDirEntries(filepath.FromSlash("/"), db.fs)
	if err != nil {
		return nil, err
	}
	var response []gofakes3.BucketInfo
	for _, entry := range dirEntries {
		if entry.IsDir() {
			response = append(response, gofakes3.BucketInfo{
				Name:         entry.Name(),
				CreationDate: gofakes3.NewContentTime(entry.ModTime()),
			})
		}
		// todo: handle files in root dir
	}

	return response, nil
}

// ListObjects lists the objects in the given bucket.
func (db *SimpleBucketBackend) ListBucket(bucket string, prefix *gofakes3.Prefix, page gofakes3.ListBucketPage) (*gofakes3.ObjectList, error) {

	_, err := db.fs.Stat(bucket)
	if err != nil {
		return nil, gofakes3.BucketNotFound(bucket)
	}
	if prefix == nil {
		prefix = emptyPrefix
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	// workaround
	if prefix.Prefix == "" {
		prefix.HasPrefix = false
	}
	if prefix.Delimiter == "" {
		prefix.HasDelimiter = false
	}

	result, err := db.getObjectsList(bucket, prefix)
	if err != nil {
		return nil, err
	}

	return db.pager(result, page)
}

// getObjectsList lists the objects in the given bucket.
func (db *SimpleBucketBackend) getObjectsList(bucket string, prefix *gofakes3.Prefix) (*gofakes3.ObjectList, error) {

	prefixPath, prefixPart, delim := prefixParser(prefix)
	if !delim {
		return db.getObjectsListArbitrary(bucket, prefix)
	}

	fp := filepath.Join(bucket, prefixPath)
	dirEntries, err := getDirEntries(fp, db.fs)
	if err != nil {
		return nil, err
	}
	response := gofakes3.NewObjectList()

	for _, entry := range dirEntries {
		object := entry.Name()

		// Expected use of 'path'; see the "Path Handling" subheading in doc.go:
		objectPath := path.Join(prefixPath, object)

		if prefixPart != "" && !strings.HasPrefix(object, prefixPart) {
			continue
		}

		if entry.IsDir() {
			response.AddPrefix(objectPath)

		} else {
			size := entry.Size()
			mtime := entry.ModTime()

			response.Add(&gofakes3.Content{
				Key:          objectPath,
				LastModified: gofakes3.NewContentTime(mtime),
				ETag:         `""`,
				Size:         size,
			})
		}
	}

	return response, nil
}

// getObjectsList lists the objects in the given bucket.
func (db *SimpleBucketBackend) getObjectsListArbitrary(bucket string, prefix *gofakes3.Prefix) (*gofakes3.ObjectList, error) {
	response := gofakes3.NewObjectList()
	walk.ListR(context.Background(), db.fs.Fs(), bucket, true, -1, walk.ListObjects, func(entries fs.DirEntries) error {
		for _, entry := range entries {
			object := strings.TrimPrefix(entry.Remote(), bucket+"/")

			var matchResult gofakes3.PrefixMatch
			if prefix.Match(object, &matchResult) {
				if matchResult.CommonPrefix {
					response.AddPrefix(object)
				}

				size := entry.Size()
				mtime := entry.ModTime(context.Background())

				response.Add(&gofakes3.Content{
					Key:          object,
					LastModified: gofakes3.NewContentTime(mtime),
					ETag:         `""`,
					Size:         size,
				})
			}
		}

		return nil
	})

	return response, nil
}

// HeadObject returns the fileinfo for the given object name.
//
// Note that the metadata is not supported yet.
func (db *SimpleBucketBackend) HeadObject(bucketName, objectName string) (*gofakes3.Object, error) {

	_, err := db.fs.Stat(bucketName)
	if err != nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	fp := filepath.ToSlash(filepath.Join(bucketName, objectName))
	stat, err := db.fs.Stat(fp)
	if err == vfs.ENOENT {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, err
	}

	if stat.IsDir() {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	size := stat.Size()
	hash := getFileHash(stat)
	time := stat.ModTime()
	return &gofakes3.Object{
		Name: objectName,
		Hash: []byte(hash),
		Metadata: map[string]string{
			"Last-Modified": time.Format(timeFormat) + " GMT",
			// fixme: other ways?
			"Content-Type": mime.TypeByExtension(filepath.Ext(objectName)),
		},
		Size:     size,
		Contents: NoOpReadCloser{},
	}, nil
}

// GetObject fetchs the object from the filesystem.
func (db *SimpleBucketBackend) GetObject(bucketName, objectName string, rangeRequest *gofakes3.ObjectRangeRequest) (obj *gofakes3.Object, err error) {
	_, err = db.fs.Stat(bucketName)
	if err != nil {
		return nil, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	fp := filepath.ToSlash(filepath.Join(bucketName, objectName))
	f, err := db.fs.Open(fp)
	if err == vfs.ENOENT {
		return nil, gofakes3.KeyNotFound(objectName)
	} else if err != nil {
		return nil, err
	}

	if f.Node().IsDir() {
		return nil, gofakes3.KeyNotFound(objectName)
	}

	defer func() {
		// If an error occurs, the caller may not have access to Object.Body in order to close it:
		if err != nil && obj == nil {
			f.Close()
		}
	}()

	stat, err := db.fs.Stat(fp)
	if err != nil {
		return nil, err
	}

	size := stat.Size()
	hash := getFileHash(stat)

	var rdr io.ReadCloser = f
	rnge, err := rangeRequest.Range(size)
	if err != nil {
		return nil, err
	}

	if rnge != nil {
		if _, err := f.Seek(rnge.Start, io.SeekStart); err != nil {
			return nil, err
		}
		rdr = limitReadCloser(rdr, f.Close, rnge.Length)
	}

	return &gofakes3.Object{
		Name: objectName,
		Hash: []byte(hash),
		Metadata: map[string]string{
			"Last-Modified": stat.ModTime().Format(timeFormat) + " GMT",
			// fixme: other ways?
			"Content-Type": mime.TypeByExtension(filepath.Ext(objectName)),
		},
		Size:     size,
		Range:    rnge,
		Contents: rdr,
	}, nil
}

// TouchObject creates or updates meta on specified object.
func (db *SimpleBucketBackend) TouchObject(fp string, meta map[string]string) (result gofakes3.PutObjectResult, err error) {

	_, err = db.fs.Stat(fp)
	fmt.Println(err)
	if err == vfs.ENOENT {
		f, err := db.fs.Create(fp)
		if err != nil {
			return result, err
		}
		f.Close()
		return db.TouchObject(fp, meta)
	} else if err != nil {
		return result, err
	}

	if val, ok := meta["X-Amz-Meta-Mtime"]; ok {
		ti, err := parseTimestamp(val)
		if err == nil {
			db.fs.Chtimes(fp, ti, ti)
		}
		// ignore error since the file is successfully created
	}

	_, err = db.fs.Stat(fp)
	if err != nil {
		return result, err
	}

	return result, nil
}

// PutObject creates or overwrites the object with the given name.
func (db *SimpleBucketBackend) PutObject(
	bucketName, objectName string,
	meta map[string]string,
	input io.Reader, size int64,
) (result gofakes3.PutObjectResult, err error) {

	_, err = db.fs.Stat(bucketName)
	if err != nil {
		return result, gofakes3.BucketNotFound(bucketName)
	}

	db.lock.Lock()
	defer db.lock.Unlock()

	fp := filepath.ToSlash(filepath.Join(bucketName, objectName))
	fmt.Printf("PutObject: %s\n\n\n", fp)
	objectDir := filepath.Dir(fp)
	// _, err = db.fs.Stat(objectDir)
	// if err == vfs.ENOENT {
	// 	fs.Errorf(objectDir, "PutObject failed: path not found")
	// 	return result, gofakes3.KeyNotFound(objectName)
	// }

	if objectDir != "." {
		if err := mkdirRecursive(objectDir, db.fs); err != nil {
			return result, err
		}
	}

	if size == 0 {
		// maybe a touch operation
		return db.TouchObject(fp, meta)
	}

	f, err := db.fs.Create(fp)
	if err != nil {
		return result, err
	}

	hasher := md5.New()
	w := io.MultiWriter(f, hasher)
	if _, err := io.Copy(w, input); err != nil {
		return result, err
	}

	if err := f.Close(); err != nil {
		return result, err
	}

	_, err = db.fs.Stat(fp)
	if err != nil {
		return result, err
	}

	if val, ok := meta["X-Amz-Meta-Mtime"]; ok {
		ti, err := parseTimestamp(val)
		if err == nil {
			db.fs.Chtimes(fp, ti, ti)
		}
		// ignore error since the file is successfully created
	}

	return result, nil
}

// DeleteMulti deletes multiple objects in a single request.
func (db *SimpleBucketBackend) DeleteMulti(bucketName string, objects ...string) (result gofakes3.MultiDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	for _, object := range objects {
		if err := db.deleteObjectLocked(bucketName, object); err != nil {
			log.Println("delete object failed:", err)
			result.Error = append(result.Error, gofakes3.ErrorResult{
				Code:    gofakes3.ErrInternal,
				Message: gofakes3.ErrInternal.Message(),
				Key:     object,
			})
		} else {
			result.Deleted = append(result.Deleted, gofakes3.ObjectID{
				Key: object,
			})
		}
	}

	return result, nil
}

// DeleteObject deletes the object with the given name.
func (db *SimpleBucketBackend) DeleteObject(bucketName, objectName string) (result gofakes3.ObjectDeleteResult, rerr error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	return result, db.deleteObjectLocked(bucketName, objectName)
}

// deleteObjectLocked deletes the object from the filesystem.
func (db *SimpleBucketBackend) deleteObjectLocked(bucketName, objectName string) error {

	_, err := db.fs.Stat(bucketName)
	if err != nil {
		return gofakes3.BucketNotFound(bucketName)
	}

	fp := filepath.ToSlash(filepath.Join(bucketName, objectName))
	// S3 does not report an error when attemping to delete a key that does not exist, so
	// we need to skip IsNotExist errors.
	if err := db.fs.Remove(fp); err != nil && !os.IsNotExist(err) {
		return err
	}

	// Remove empty dirs
	dirs := strings.Split(filepath.ToSlash(filepath.Dir(fp)), "/")
	for i := range dirs {
		dir := "/" + strings.Join(dirs[:len(dirs)-i], "/")
		obj, err := db.fs.ReadDir(dir)
		if err != nil {
			return err
		}
		// Do not delete bucket itself
		if len(obj) == 0 && i < len(dirs)-1 {
			db.fs.Remove(dir)
		}
	}

	return nil
}

// CreateBucket creates a new bucket.
func (db *SimpleBucketBackend) CreateBucket(name string) error {
	_, err := db.fs.Stat(name)
	if err != nil && err != vfs.ENOENT {
		return gofakes3.ErrInternal
	}

	if err == nil {
		return gofakes3.ErrBucketAlreadyExists
	}

	if err := db.fs.Mkdir(name, 0755); err != nil {
		return gofakes3.ErrInternal
	}
	return nil
}

// DeleteBucket deletes the bucket with the given name.
func (db *SimpleBucketBackend) DeleteBucket(name string) error {
	_, err := db.fs.Stat(name)
	if err != nil {
		return gofakes3.BucketNotFound(name)
	}

	if err := db.fs.Remove(name); err != nil {
		return gofakes3.ErrBucketNotEmpty
	}

	return nil
}

// BucketExists checks if the bucket exists.
func (db *SimpleBucketBackend) BucketExists(name string) (exists bool, err error) {
	_, err = db.fs.Stat(name)
	if err != nil {
		return false, nil
	}

	return true, nil
}

func (db *SimpleBucketBackend) InitiateMultipartUpload(bucket, object string, meta map[string]string, initiated time.Time) gofakes3.UploadID {
	db.lock.Lock()
	defer db.lock.Unlock()

	db.uploadID.Add(db.uploadID, add1)
	tmpDir, _ := ioutil.TempDir("", "rclone-serve-s3-"+db.uploadID.String()+"-")
	mpu := &multipartUpload{
		ID:        gofakes3.UploadID(db.uploadID.String()),
		Bucket:    bucket,
		Object:    object,
		Meta:      meta,
		Initiated: initiated,
		TmpFolder: tmpDir,
	}

	// FIXME: make sure the uploader responds to DeleteBucket
	bucketUploads := db.uploads[bucket]
	if bucketUploads == nil {
		db.uploads[bucket] = newBucketUploads()
		bucketUploads = db.uploads[bucket]
	}

	bucketUploads.add(mpu)
	return gofakes3.UploadID(db.uploadID.String())
}

func (db *SimpleBucketBackend) ListMultipartUploadParts(bucket, object string, uploadID gofakes3.UploadID, marker int, limit int64) (*gofakes3.ListMultipartUploadPartsResult, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	mpu, err := db.getMultipartUpload(bucket, object, uploadID)
	if err != nil {
		return nil, err
	}

	var result = gofakes3.ListMultipartUploadPartsResult{
		Bucket:           bucket,
		Key:              object,
		UploadID:         uploadID,
		MaxParts:         limit,
		PartNumberMarker: marker,
		StorageClass:     "STANDARD", // FIXME
	}

	var cnt int64
	for partNumber, part := range mpu.parts[marker:] {
		if part == nil {
			continue
		}

		if cnt >= limit {
			result.IsTruncated = true
			result.NextPartNumberMarker = partNumber
			break
		}

		result.Parts = append(result.Parts, gofakes3.ListMultipartUploadPartItem{
			ETag:         part.ETag,
			Size:         part.Size,
			PartNumber:   partNumber,
			LastModified: part.LastModified,
		})

		cnt++
	}

	return &result, nil
}

func (db *SimpleBucketBackend) ListMultipartUploads(bucket string, marker *gofakes3.UploadListMarker, prefix gofakes3.Prefix, limit int64) (*gofakes3.ListMultipartUploadsResult, error) {
	db.lock.Lock()
	defer db.lock.Unlock()

	bucketUploads, ok := db.uploads[bucket]
	if !ok {
		return nil, gofakes3.ErrNoSuchUpload
	}

	var result = gofakes3.ListMultipartUploadsResult{
		Bucket:     bucket,
		Delimiter:  prefix.Delimiter,
		Prefix:     prefix.Prefix,
		MaxUploads: limit,
	}

	// we only need to use the uploadID to start the page if one was actually
	// supplied, otherwise assume we can start from the start of the iterator:
	var firstFound = true

	var iter = bucketUploads.objectIndex.Iterator()
	if marker != nil {
		if iter.Seek(marker.Object) {
			iter.Previous()
		}
		firstFound = marker.UploadID == ""
		result.UploadIDMarker = marker.UploadID
		result.KeyMarker = marker.Object
	}

	// Indicates whether the returned list of multipart uploads is truncated.
	// The list can be truncated if the number of multipart uploads exceeds
	// the limit allowed or specified by MaxUploads.
	//
	// In our case, this could be because there are still objects left in the
	// iterator, or because there are still uploadIDs left in the slice inside
	// the iteration.
	var truncated bool

	var cnt int64
	var seenPrefixes = map[string]bool{}
	var match gofakes3.PrefixMatch

	for iter.Next() {
		object := iter.Key().(string)
		uploads := iter.Value().([]*multipartUpload)

	retry:
		matched := prefix.Match(object, &match)
		if !matched {
			continue
		}

		if !firstFound {
			for idx, mpu := range uploads {
				if mpu.ID == marker.UploadID {
					firstFound = true
					uploads = uploads[idx:]
					goto retry
				}
			}

		} else {
			if match.CommonPrefix {
				if !seenPrefixes[match.MatchedPart] {
					result.CommonPrefixes = append(result.CommonPrefixes, match.AsCommonPrefix())
					seenPrefixes[match.MatchedPart] = true
				}

			} else {
				for idx, upload := range uploads {
					result.Uploads = append(result.Uploads, gofakes3.ListMultipartUploadItem{
						StorageClass: "STANDARD", // FIXME
						Key:          object,
						UploadID:     upload.ID,
						Initiated:    gofakes3.ContentTime{Time: upload.Initiated},
					})

					cnt++
					if cnt >= limit {
						if idx != len(uploads)-1 { // if this is not the last iteration, we have truncated
							truncated = true
							result.NextUploadIDMarker = uploads[idx+1].ID
							result.NextKeyMarker = object
						}
						goto done
					}
				}
			}
		}
	}

done:
	// If we did not truncate while in the middle of an object's upload ID list,
	// we need to see if there are more objects in the outer iteration:
	if !truncated {
		for iter.Next() {
			object := iter.Key().(string)
			if matched := prefix.Match(object, &match); matched && !match.CommonPrefix {
				truncated = true

				// This is not especially defensive; it assumes the rest of the code works
				// as it should. Could be something to clean up later:
				result.NextUploadIDMarker = iter.Value().([]*multipartUpload)[0].ID
				result.NextKeyMarker = object
				break
			}
		}
	}

	result.IsTruncated = truncated

	return &result, nil
}
func (db *SimpleBucketBackend) PutMultipartUploadPart(bucket string, object string, id gofakes3.UploadID, partNumber int, at time.Time, input io.Reader, size int64) (string, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	up, err := db.getMultipartUpload(bucket, object, id)
	if err != nil {
		return "", err
	}
	return up.addPart(partNumber, at, input, size)
}
func (db *SimpleBucketBackend) CompleteMultipartUpload(bucket string, object string, id gofakes3.UploadID, cmup *gofakes3.CompleteMultipartUploadRequest) (*gofakes3.CompleteMultipartUploadData, error) {
	db.lock.Lock()
	defer db.lock.Unlock()
	up, err := db.getMultipartUpload(bucket, object, id)
	if err != nil {
		return nil, err
	}

	// if getUnlocked succeeded, so will this:
	db.uploads[bucket].remove(id)
	return up.reassemble(cmup)
}
func (db *SimpleBucketBackend) AbortMultipartUpload(bucket string, object string, id gofakes3.UploadID) error {
	db.lock.Lock()
	defer db.lock.Unlock()
	upload, err := db.getMultipartUpload(bucket, object, id)
	if err != nil {
		return err
	}

	// if getUnlocked succeeded, so will this:
	db.uploads[bucket].remove(id)
	return os.RemoveAll(upload.TmpFolder)
}

type bucketUploads struct {
	uploads     map[gofakes3.UploadID]*multipartUpload
	objectIndex *skiplist.SkipList
}

func newBucketUploads() *bucketUploads {
	return &bucketUploads{
		uploads:     map[gofakes3.UploadID]*multipartUpload{},
		objectIndex: skiplist.NewStringMap(),
	}
}

// add assumes uploader.mu is acquired
func (bu *bucketUploads) add(mpu *multipartUpload) {
	bu.uploads[mpu.ID] = mpu

	uploads, ok := bu.objectIndex.Get(mpu.Object)
	if !ok {
		uploads = []*multipartUpload{mpu}
	} else {
		uploads = append(uploads.([]*multipartUpload), mpu)
	}
	bu.objectIndex.Set(mpu.Object, uploads)
}

// remove assumes uploader.mu is acquired
func (bu *bucketUploads) remove(uploadID gofakes3.UploadID) {
	upload := bu.uploads[uploadID]
	delete(bu.uploads, uploadID)

	var uploads []*multipartUpload
	{
		upv, ok := bu.objectIndex.Get(upload.Object)
		if !ok || upv == nil {
			return
		}
		uploads = upv.([]*multipartUpload)
	}

	var found = -1
	var v *multipartUpload
	for found, v = range uploads {
		if v.ID == uploadID {
			break
		}
	}

	if found >= 0 {
		uploads = append(uploads[:found], uploads[found+1:]...) // delete the found index
	}

	if len(uploads) == 0 {
		bu.objectIndex.Delete(upload.Object)
	} else {
		bu.objectIndex.Set(upload.Object, uploads)
	}
}

func (db *SimpleBucketBackend) getMultipartUpload(bucket, object string, id gofakes3.UploadID) (mu *multipartUpload, err error) {
	bucketUps, ok := db.uploads[bucket]
	if !ok {
		return nil, gofakes3.ErrNoSuchUpload
	}

	mu, ok = bucketUps.uploads[id]
	if !ok {
		return nil, gofakes3.ErrNoSuchUpload
	}

	if mu.Bucket != bucket || mu.Object != object {
		// FIXME: investigate what AWS does here; essentially if you initiate a
		// multipart upload at '/ObjectName1?uploads', then complete the upload
		// at '/ObjectName2?uploads', what happens?
		return nil, gofakes3.ErrNoSuchUpload
	}

	return mu, nil
}

type multipartUpload struct {
	ID        gofakes3.UploadID
	Bucket    string
	Object    string
	Meta      map[string]string
	Initiated time.Time
	parts     []*multipartUploadPart
	lock      sync.Mutex
	TmpFolder string
}

type multipartUploadPart struct {
	PartNumber   int
	ETag         string
	Size         int64
	LastModified gofakes3.ContentTime
}

func (mpu *multipartUpload) addPart(partNumber int, at time.Time, input io.Reader, size int64) (etag string, err error) {
	if partNumber > gofakes3.MaxUploadPartNumber {
		return "", gofakes3.ErrInvalidPart
	}

	mpu.lock.Lock()
	defer mpu.lock.Unlock()

	// What the ETag actually is is not specified, so let's just invent any old thing
	// from guaranteed unique input:
	hasher := md5.New()

	partFile, err := os.Create(mpu.TmpFolder + "/" + strconv.Itoa(partNumber))
	if err != nil {
		return "", err
	}
	defer partFile.Close()
	w := io.MultiWriter(partFile, hasher)
	written, err := io.Copy(w, input)
	if err != nil {
		return "", err
	}
	if size != written {
		return "", gofakes3.ErrorMessagef(gofakes3.ErrInvalidPart, "size mismatch, expected %d bytes, got %d bytes", size, written)
	}

	etag = fmt.Sprintf(`"%s"`, hex.EncodeToString(hasher.Sum(nil)))

	part := multipartUploadPart{
		PartNumber:   partNumber,
		ETag:         etag,
		LastModified: gofakes3.NewContentTime(at),
		Size:         size,
	}
	if partNumber >= len(mpu.parts) {
		mpu.parts = append(mpu.parts, make([]*multipartUploadPart, partNumber-len(mpu.parts)+1)...)
	}
	mpu.parts[partNumber] = &part
	return etag, nil
}

func (mpu *multipartUpload) reassemble(input *gofakes3.CompleteMultipartUploadRequest) (cmpud *gofakes3.CompleteMultipartUploadData, err error) {
	mpu.lock.Lock()
	defer mpu.lock.Unlock()
	cmpud = &gofakes3.CompleteMultipartUploadData{}
	mpuPartsLen := len(mpu.parts)
	cmpud.Size = 0
	hasher := md5.New()

	// FIXME: what does AWS do when mpu.Parts > input.Parts? Presumably you may
	// end up uploading more parts than you need to assemble, so it should
	// probably just ignore that?
	if len(input.Parts) > mpuPartsLen {
		return nil, gofakes3.ErrInvalidPart
	}

	if !input.PartsAreSorted() {
		return nil, gofakes3.ErrInvalidPartOrder
	}

	for _, inPart := range input.Parts {
		if inPart.PartNumber >= mpuPartsLen || mpu.parts[inPart.PartNumber] == nil {
			return nil, gofakes3.ErrorMessagef(gofakes3.ErrInvalidPart, "unexpected part number %d in complete request", inPart.PartNumber)
		}

		upPart := mpu.parts[inPart.PartNumber]
		if strings.Trim(inPart.ETag, "\"") != strings.Trim(upPart.ETag, "\"") {
			return nil, gofakes3.ErrorMessagef(gofakes3.ErrInvalidPart, "unexpected part etag for number %d in complete request", inPart.PartNumber)
		}

		cmpud.Size += upPart.Size
	}

	tempFile, err := os.Create(mpu.TmpFolder + "/" + "object")
	if err != nil {
		return nil, err
	}

	w := io.MultiWriter(tempFile, hasher)
	for _, part := range input.Parts {
		partFile, err := os.Open(mpu.TmpFolder + "/" + strconv.Itoa(part.PartNumber))
		if err != nil {
			return nil, err
		}

		written, err := io.Copy(w, partFile)
		if err != nil {
			return nil, err
		}
		if written != mpu.parts[part.PartNumber].Size {
			return nil, gofakes3.ErrorMessagef(gofakes3.ErrInvalidPart, "size mismatch, expected %d bytes, got %d bytes", mpu.parts[part.PartNumber].Size, written)
		}
		partFile.Close()
		//os.Remove(mpu.TmpFolder + "/" + strconv.Itoa(part.PartNumber))
	}
	tempFile.Close()
	finalObject, err := os.Open(tempFile.Name())
	if err != nil {
		return nil, err
	}
	cmpud.Etag = fmt.Sprintf("%x", hasher.Sum(nil))
	cmpud.FileBody = finalObject
	cmpud.Meta = mpu.Meta
	cmpud.Cleaner = &multipartCleaner{
		TmpFolder: mpu.TmpFolder,
	}

	return cmpud, nil
}

type multipartCleaner struct {
	TmpFolder string
}

func (mc *multipartCleaner) Cleanup() error {
	return os.RemoveAll(mc.TmpFolder)
}
