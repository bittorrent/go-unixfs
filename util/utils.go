package util

import (
	"context"
	"errors"

	ft "github.com/TRON-US/go-unixfs"
	ufile "github.com/TRON-US/go-unixfs/file"
	uio "github.com/TRON-US/go-unixfs/io"

	ipld "github.com/ipfs/go-ipld-format"
	mdag "github.com/ipfs/go-merkledag"
)

// Intersects returns true if the given two maps intersect.
func Intersects(m map[string]interface{}, inputM map[string]interface{}) bool {
	for k, _ := range inputM {
		_, isPresent := m[k]
		if isPresent {
			return true
		}
	}
	return false
}

// KeyIntersects returns true if the key set of the given `m`
// and the string array `inputKeys` intersects.
func KeyIntersects(m map[string]interface{}, inputKeys []string) bool {
	for _, k := range inputKeys {
		_, isPresent := m[k]
		if isPresent {
			return true
		}
	}
	return false
}

// EqualKeySets returns true if the key set of the given `m` equals
// the given key string array `inputKeys`.
func EqualKeySets(m map[string]interface{}, inputKeys []string) bool {
	if len(m) != len(inputKeys) {
		return false
	}

	for _, ik := range inputKeys {
		_, isPresent := m[ik]
		if !isPresent {
			return false
		}
	}

	return true
}

func ReadMetadataBytes(ctx context.Context, root ipld.Node, ds ipld.DAGService, meta bool) ([]byte, error) {
	nd, ok := root.(*mdag.ProtoNode)
	if !ok {
		return nil, errors.New("Expected protobuf Merkle DAG node")
	}
	fsn, err := ft.FSNodeFromBytes(nd.Data())
	if err != nil {
		return nil, err
	}
	typ := fsn.Type()
	if typ != ft.TFile && typ != ft.TDirectory && typ != ft.TTokenMeta {
		return nil, errors.New("unexpected node type")
	}

	_, mnode, err := ufile.CheckAndSplitMetadata(ctx, root, ds, meta)
	if err != nil {
		return nil, err
	}
	if mnode == nil {
		return nil, nil
	}

	r, err := uio.NewDagReader(ctx, mnode, ds)
	if err != nil {
		return nil, err
	}

	b := make([]byte, r.Size())
	_, err = r.CtxReadFull(ctx, b)
	if err != nil {
		return nil, err
	}

	return b, nil
}
