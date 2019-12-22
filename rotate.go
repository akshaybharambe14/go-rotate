package rotate

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

const (
	flags     = os.O_CREATE | os.O_WRONLY
	fmtLayout = "2006_01_02T15_04_05.999999999Z07_00"
)

type Rotate struct {
	file    *os.File   // file holds the current file
	name    string     // name for current file
	fmtFunc Format     // renames the closed file with this format
	mux     sync.Mutex //
	cnt     int        // holds totals bytes written to the file
	maxSize int        // Max size in MB
}

type Format func(name string, t time.Time) string

type Options struct {
	Name    string
	FmtFunc Format
	MaxSize int
}

func New(opts Options) (*Rotate, error) {
	if err := os.MkdirAll(filepath.Dir(opts.Name), os.ModePerm); err != nil {
		return nil, err
	}

	file, err := os.OpenFile(opts.Name, flags, os.ModePerm)
	if err != nil {
		return nil, err
	}

	end, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return nil, err
	}

	if opts.FmtFunc == nil {
		opts.FmtFunc = FormatFunc
	}

	return &Rotate{
		file:    file,
		fmtFunc: opts.FmtFunc,
		name:    opts.Name,
		cnt:     int(end),
		maxSize: opts.MaxSize,
		mux:     sync.Mutex{},
	}, nil
}

// Write implements io.Writer interface
func (r *Rotate) Write(b []byte) (n int, err error) {
	r.mux.Lock()
	n, err = r.write(b)
	r.mux.Unlock()
	return
}

func FormatFunc(name string, t time.Time) string {
	ext := filepath.Ext(name)
	return strings.TrimSuffix(name, ext) + "_" + t.Format(fmtLayout) + ext
}

func (r *Rotate) write(b []byte) (n int, err error) {
	if (r.cnt > 0) && ((r.cnt + len(b)) > r.maxSize) {
		if err = r.rotate(); err != nil {
			return
		}
	}

	n, err = r.file.Write(b)
	if err != nil {
		return
	}

	// increment write cnt
	r.cnt += n
	return
}

func (r *Rotate) rotate() (err error) {
	if err = r.file.Sync(); err != nil {
		return
	}

	if err = r.file.Close(); err != nil {
		return
	}

	newName := r.fmtFunc(r.name, time.Now())
	if err = os.Rename(r.name, newName); err != nil {
		return
	}

	r.file, err = os.OpenFile(r.name, flags, os.ModePerm)
	r.cnt = 0
	return
}
