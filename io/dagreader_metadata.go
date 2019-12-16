package io

import (
	"context"

	ft "github.com/TRON-US/go-unixfs"

	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
)

// GetMetaDataFromDagRoot returns the full metadata bytes if available.
// This function is unixfs/io instead of unixfs because of `NewDagReader`
// dependency to read all the bytes.
func GetMetaDataFromDagRoot(ctx context.Context, root ipld.Node, ds ipld.DAGService) ([]byte, error) {
	_, ok := root.(*dag.ProtoNode)
	if !ok {
		return nil, dag.ErrNotProtobuf
	}

	mnd, err := ft.GetMetaSubdagRoot(ctx, root, ds)
	if err != nil {
		return nil, err
	}
	if mnd == nil {
		return nil, nil
	}

	mr, err := NewDagReader(ctx, mnd, ds)
	if err != nil {
		return nil, err
	}

	b := make([]byte, mr.Size())
	_, err = mr.CtxReadFull(ctx, b)
	if err != nil {
		return nil, err
	}

	return b, nil
}
