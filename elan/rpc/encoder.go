package rpc

import (
	"io"

	"github.com/klauspost/compress/zstd"
)

func mustNewEncoder(w io.Writer) *zstd.Encoder {
	enc, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.SpeedFastest))
	if err != nil {
		log.Panicf("Failed to create zstd encoder: %v", err)
	}
	return enc
}
