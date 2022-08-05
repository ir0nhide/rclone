// Package s3 implements an fake s3 server for rclone

package s3

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"

	"github.com/johannesboyne/gofakes3"
	"github.com/rclone/rclone/cmd"
	"github.com/rclone/rclone/cmd/serve/httplib"
	"github.com/rclone/rclone/cmd/serve/httplib/httpflags"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config/flags"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/vfs"
	"github.com/rclone/rclone/vfs/vfsflags"
	"github.com/spf13/cobra"
)

// Options contains options for the http Server
type Options struct {
	//TODO add more options
	hostBucketMode bool
	hashName       string
	hashType       hash.Type
}

// DefaultOpt is the default values used for Options
var DefaultOpt = Options{
	hostBucketMode: false,
	hashName:       "",
	hashType:       hash.None,
}

// Opt is options set by command line flags
var Opt = DefaultOpt

func init() {
	flagSet := Command.Flags()
	httpflags.AddFlags(flagSet)
	vfsflags.AddFlags(flagSet)
	flags.BoolVarP(flagSet, &Opt.hostBucketMode, "host-bucket", "", Opt.hostBucketMode, "Whether to use bucket name in hostname (such as mybucket.local)")
	flags.StringVarP(flagSet, &Opt.hashName, "etag-hash", "", Opt.hashName, "Which hash to use for the ETag, or auto or blank for off")
}

// Command definition for cobra
var Command = &cobra.Command{
	Use:   "s3 remote:path",
	Short: `Serve remote:path over s3.`,
	Long: `
rclone serve s3 implements a basic s3 server to serve the
remote over s3. This can be viewed with s3 client
or you can make a remote of type s3 to read and write it.
Note that some clients may require https endpoint.
` + httplib.Help + vfs.Help,
	RunE: func(command *cobra.Command, args []string) error {
		cmd.CheckArgs(1, 1, command, args)
		f := cmd.NewFsSrc(args)

		if Opt.hashName == "auto" {
			Opt.hashType = f.Hashes().GetOne()
		} else if Opt.hashName != "" {
			err := Opt.hashType.Set(Opt.hashName)
			if err != nil {
				return err
			}
		}
		cmd.Run(false, false, command, func() error {
			s := newServer(context.Background(), f, &Opt)
			err := s.Serve()
			if err != nil {
				return err
			}
			s.Wait()
			return nil
		})
		return nil
	},
}

// Server is a s3.FileSystem interface
type Server struct {
	*httplib.Server
	f       fs.Fs
	vfs     *vfs.VFS
	faker   *gofakes3.GoFakeS3
	handler http.Handler
	ctx     context.Context // for global config
}

// Make a new S3 Server to serve the remote
func newServer(ctx context.Context, f fs.Fs, opt *Options) *Server {
	w := &Server{
		f:   f,
		ctx: ctx,
		vfs: vfs.New(f, &vfsflags.Opt),
	}

	var newLogger Logger
	w.faker = gofakes3.New(
		newBackend(w.vfs, opt),
		gofakes3.WithHostBucket(opt.hostBucketMode),
		gofakes3.WithLogger(newLogger),
		gofakes3.WithRequestID(rand.Uint64()),
		gofakes3.WithoutVersioning(),
	)

	w.handler = authMiddleware(w.faker.Server())
	w.Server = httplib.NewServer(w.handler, &httpflags.Opt)
	return w
}

func authMiddleware(handler http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		// Currently not supported, direct to the handler
		handler.ServeHTTP(w, rq)
	})
}

// func dumpRequestMiddleware(handler http.Handler) http.Handler {
// 	return http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
// 		var formatted, err = httputil.DumpRequest(rq, true)
// 		if err != nil {
// 			fmt.Fprint(w, err)
// 		}

// 		fmt.Printf("%s\n", formatted)
// 		handler.ServeHTTP(w, rq)
// 	})
// }

// logger output formatted message
type Logger struct{}

// print log message
func (l Logger) Print(level gofakes3.LogLevel, v ...interface{}) {
	switch level {
	case gofakes3.LogErr:
		fs.Errorf(nil, fmt.Sprintln(v...))
	case gofakes3.LogWarn:
		fs.Errorf(nil, fmt.Sprintln(v...))
	case gofakes3.LogInfo:
		fs.Infof(nil, fmt.Sprintln(v...))
	default:
		panic("unknown level")
	}
}
