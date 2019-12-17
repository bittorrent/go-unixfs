package util

import (
	"context"
	"errors"
	"strings"

	ft "github.com/TRON-US/go-unixfs"
	uio "github.com/TRON-US/go-unixfs/io"

	ipld "github.com/ipfs/go-ipld-format"
	mdag "github.com/ipfs/go-merkledag"
)

// checkAndSplitMetadata returns both data root node and metadata root node if exists from
// the DAG topped by the given 'nd'.
// Case #1: if 'nd' is dummy root with metadata root node and user data root node being children,
//    return [the second data root child node, the first metadata root child node, nil].
// Case #2: if 'nd' is metadata and the given `meta` is true, return [nil, nd, nil]. Otherwise return error.
// Case #3: if 'nd' is user data, return ['nd', nil, nil].
func CheckAndSplitMetadata(ctx context.Context, nd ipld.Node, ds ipld.DAGService, meta bool) (ipld.Node, ipld.Node, error) {
	n := nd.(*mdag.ProtoNode)

	fsType, err := ft.GetFSType(n)
	if err != nil {
		return nil, nil, err
	}

	if ft.TTokenMeta == fsType {
		if meta {
			return nil, nd, nil
		} else {
			return nil, nil, ft.ErrMetadataAccessDenied
		}
	}

	// Return user data and metadata if first child is of type TTokenMeta.
	if nd.Links() != nil && len(nd.Links()) >= 2 {
		children, err := ft.GetChildrenForDagWithMeta(ctx, nd, ds)
		if err != nil {
			return nil, nil, err
		}
		if children == nil {
			return nd, nil, nil
		}
		return children.DataNode, children.MetaNode, nil
	}

	return nd, nil, nil
}

func ReadMetadataElementFromDag(ctx context.Context, root ipld.Node, ds ipld.DAGService, meta bool) ([]byte, error) {
	b, _, err := ReadMetadataListFromDag(ctx, root, ds, meta)
	return b, err
}

func ReadMetadataListFromDag(ctx context.Context, root ipld.Node, ds ipld.DAGService, meta bool) ([]byte, []byte, error) {
	nd, ok := root.(*mdag.ProtoNode)
	if !ok {
		return nil, nil, errors.New("Expected protobuf Merkle DAG node")
	}
	fsn, err := ft.FSNodeFromBytes(nd.Data())
	if err != nil {
		return nil, nil, err
	}
	typ := fsn.Type()
	if typ != ft.TFile && typ != ft.TDirectory && typ != ft.TTokenMeta {
		return nil, nil, errors.New("unexpected node type")
	}

	_, mnode, err := CheckAndSplitMetadata(ctx, root, ds, meta)
	if err != nil {
		return nil, nil, err
	}
	if mnode == nil {
		return nil, nil, nil
	}

	r, err := uio.NewDagReader(ctx, mnode, ds)
	if err != nil {
		return nil, nil, err
	}

	b := make([]byte, r.Size())
	_, err = r.CtxReadFull(ctx, b)
	if err != nil {
		return nil, nil, err
	}

	return GetMetadataList(b)
}

// CreateMetadataList join the given two byte array element into a final list
// format as BTFS metadata.
// 1. Append the above `dirTreeBytes` byte array to metaBytes so that
//   the final encoded output can ebable strings.SplitAfterN(encodedOutput, "},{", 2) to have
//   two strings, to be processed separately with json.Unmarshal().
// 2. Ensure metaBytes has two sections, each enclosed within curly bracket pair. E.g., `{"price":11.0},{}`.
//   This will make the encoder task easy.
// The precondition is to pass nil to `metaBytes` if no value exist for regular metadata,
// nil to `dirTreeBytes` for directoy tree bytes in case of Reed-Solomon directory inpuyt,
func CreateMetadataList(metaBytes []byte, dirTreeBytes []byte) []byte {
	if metaBytes == nil && dirTreeBytes == nil {
		return nil
	}
	if metaBytes == nil {
		metaBytes = []byte("{}")
	}
	if dirTreeBytes != nil {
		metaBytes = append(append(metaBytes[:len(metaBytes)], '#'), dirTreeBytes[:]...)
	} else {
		metaBytes = append(append(metaBytes[:len(metaBytes)], '#'), []byte("{}")...)
	}

	return metaBytes
}

// GetMetadataSList splits the given encoded metadata `bytes`
// into two byte arrays in JSON format.
// Note that chunker.RsMetaMap.IsDir indicates or true when the second array has contents.
func GetMetadataList(bytes []byte) ([]byte, []byte, error) {
	ts := strings.SplitN(string(bytes), "}#{", 2)

	metaBuf := append([]byte(ts[0]), '}')
	treeBuf := append([]byte("{"), []byte(ts[1])...)

	return metaBuf, treeBuf, nil
}

func GetMetadataElement(bytes []byte) []byte {
	b, _, _ := GetMetadataList(bytes)
	return b
}

func GetSerializedDirectoryElement(bytes []byte) []byte {
	_, b, _ := GetMetadataList(bytes)
	return b
}

func IsMetadataEmpty(b []byte) bool {
	return string(b) == "{}"
}
