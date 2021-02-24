package bootstrap

import (
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/rancher/wrangler/pkg/merr"
)

// The zstd decompressor's Close() method doesn't have a return value and therefore doesn't
// match the ReadCloser interface, so we have to wrap it in our own ReadCloser that
// returns a nil error. We also need to close the underlying filehandle.
func ZstdReadCloser(r *zstd.Decoder, c io.Closer) io.ReadCloser {
	return zstdReadCloser{r, c}
}

type zstdReadCloser struct {
	r *zstd.Decoder
	c io.Closer
}

func (w zstdReadCloser) Read(p []byte) (int, error) {
	return w.r.Read(p)
}

func (w zstdReadCloser) Close() error {
	w.r.Close()
	return w.c.Close()
}

// Some decompressors implement a Close function that needs to be called to clean up resources
// or verify checksums, but we also need to ensure that the underlying file gets closed as well.
func MultiReadCloser(r io.ReadCloser, c io.Closer) io.ReadCloser {
	return multiReadCloser{r, c}
}

type multiReadCloser struct {
	r io.ReadCloser
	c io.Closer
}

func (w multiReadCloser) Read(p []byte) (int, error) {
	return w.r.Read(p)
}

func (w multiReadCloser) Close() error {
	var errs []error
	if err := w.r.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := w.c.Close(); err != nil {
		errs = append(errs, err)
	}
	return merr.NewErrors(errs...)
}

// Some decompressors don't implement a Close function, so we just need to ensure that
// the underlying file gets closed.
func SplitReadCloser(r io.Reader, c io.Closer) io.ReadCloser {
	return splitReadCloser{r, c}
}

type splitReadCloser struct {
	r io.Reader
	c io.Closer
}

func (w splitReadCloser) Read(p []byte) (int, error) {
	return w.r.Read(p)
}

func (w splitReadCloser) Close() error {
	return w.c.Close()
}
