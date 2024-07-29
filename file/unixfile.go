package unixfile

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	ft "github.com/bittorrent/go-unixfs"
	uio "github.com/bittorrent/go-unixfs/io"
	"github.com/bittorrent/go-unixfs/util"

	chunker "github.com/bittorrent/go-btfs-chunker"
	files "github.com/bittorrent/go-btfs-files"
	cid "github.com/ipfs/go-cid"

	dag "github.com/ipfs/boxo/ipld/merkledag"
	ipld "github.com/ipfs/go-ipld-format"
)

// Number to file to prefetch in directories
// TODO: should we allow setting this via context hint?
const prefetchFiles = 4

var (
	ErrNilTreeBuf = errors.New("treeBuf is nil for directory node")
)

type ufsDirectory struct {
	ctx   context.Context
	dserv ipld.DAGService
	dir   uio.Directory
	size  int64
}

type ufsIterator struct {
	ctx   context.Context
	files chan *ipld.Link
	dserv ipld.DAGService

	curName string
	curFile files.Node

	err   error
	errCh chan error
}

func (it *ufsIterator) Name() string {
	return it.curName
}

func (it *ufsIterator) Node() files.Node {
	return it.curFile
}

func (it *ufsIterator) Next() bool {
	if it.err != nil {
		return false
	}

	var l *ipld.Link
	var ok bool
	for !ok {
		if it.files == nil && it.errCh == nil {
			return false
		}
		select {
		case l, ok = <-it.files:
			if !ok {
				it.files = nil
			}
		case err := <-it.errCh:
			it.errCh = nil
			it.err = err

			if err != nil {
				return false
			}
		}
	}

	it.curFile = nil

	nd, err := l.GetNode(it.ctx, it.dserv)
	if err != nil {
		it.err = err
		return false
	}

	it.curName = l.Name
	if it.curName == uio.SmallestString {
		return it.err == nil
	}
	it.curFile, it.err = NewUnixfsFile(it.ctx, it.dserv, nd, UnixfsFileOptions{})
	return it.err == nil
}

func (it *ufsIterator) Err() error {
	return it.err
}

func (it *ufsIterator) BreadthFirstTraversal() {
}

func (d *ufsDirectory) Close() error {
	return nil
}

func (d *ufsDirectory) Entries() files.DirIterator {
	fileCh := make(chan *ipld.Link, prefetchFiles)
	errCh := make(chan error, 1)
	// Invoke goroutine to provide links of the current receiver `d` via `fileCh`.
	go func() {
		errCh <- d.dir.ForEachLink(d.ctx, func(link *ipld.Link) error {
			if d.ctx.Err() != nil {
				return d.ctx.Err()
			}
			select {
			case fileCh <- link:
			case <-d.ctx.Done():
				return d.ctx.Err()
			}
			return nil
		})

		close(errCh)
		close(fileCh)
	}()

	return &ufsIterator{
		ctx:   d.ctx,
		files: fileCh,
		errCh: errCh,
		dserv: d.dserv,
	}
}

func (d *ufsDirectory) Size() (int64, error) {
	return d.size, nil
}

func (f *ufsDirectory) SetSize(size int64) error {
	return errors.New("not supported")
}

func (f *ufsDirectory) IsReedSolomon() bool {
	return false
}

type ufsFile struct {
	uio.DagReader
}

func (f *ufsFile) Size() (int64, error) {
	return int64(f.DagReader.Size()), nil
}

func newUnixfsDir(ctx context.Context, dserv ipld.DAGService, nd *dag.ProtoNode) (files.Directory, error) {
	dir, err := uio.NewDirectoryFromNode(dserv, nd)
	if err != nil {
		return nil, err
	}

	size, err := nd.Size()
	if err != nil {
		return nil, err
	}

	return &ufsDirectory{
		ctx:   ctx,
		dserv: dserv,

		dir:  dir,
		size: int64(size),
	}, nil
}

type UnixfsFileOptions struct {
	Meta         bool
	RepairShards []cid.Cid
}

// NewUnixFsFile returns a DagReader for the 'nd' root node.
// If opts.Meta = true, only return a valid metadata node if it exists. If not, return error.
// If opts.Meta = false, return only the data contents.
// If opts.Meta = false && opts.RepairShards != nil,
// the shards would be reconstructed and added on this node.
func NewUnixfsFile(ctx context.Context, dserv ipld.DAGService, nd ipld.Node,
	opts UnixfsFileOptions) (files.Node, error) {
	rawNode := false
	switch dn := nd.(type) {
	case *dag.ProtoNode:
		fsn, err := ft.FSNodeFromBytes(dn.Data())
		if err != nil {
			return nil, err
		}
		if fsn.IsDir() {
			if !opts.Meta {
				return newUnixfsDir(ctx, dserv, dn)
			}
			// Now the current case is when the dir node may contain metadata.
		} else if fsn.Type() == ft.TSymlink {
			return files.NewLinkFile(string(fsn.Data()), nil), nil
		}

	case *dag.RawNode:
		rawNode = true
	default:
		return nil, errors.New("unknown node type")
	}

	var dr uio.DagReader
	// Keep 'nd' if raw node
	if !rawNode {
		// Split metadata node and data node if available
		dataNode, metaNode, err := util.CheckAndSplitMetadata(ctx, nd, dserv, opts.Meta)
		if err != nil {
			return nil, err
		}

		// Return just metadata if available
		if opts.Meta {
			if metaNode == nil {
				return nil, errors.New("no metadata is available")
			}
			nd = metaNode
		} else {
			// Select DagReader based on metadata information
			if metaNode != nil {
				metaStruct, err := ObtainMetadataFromDag(ctx, metaNode, dserv)
				if err != nil {
					if err == ErrNilTreeBuf {
						return nil, fmt.Errorf("treeBuf is nil for [%s]", nd.Cid().String())
					}
					return nil, err
				}
				rsMeta := metaStruct.RsMeta
				if rsMeta.NumData > 0 && rsMeta.NumParity > 0 && rsMeta.FileSize > 0 {
					// TODO: if isDir uio.New..DagForDirectory, else New..File that wraps reader builder
					if rsMeta.IsDir {
						return NewReedSolomonDirectory(ctx, nd, dataNode, dserv, opts, metaStruct)
					} else {
						return NewReedSolomonStandaloneFile(ctx, nd, dataNode, dserv, opts, metaStruct)
					}
				}
			}
			nd = dataNode
		}
	}

	// Use default dag reader if not a special type reader
	if dr == nil {
		var err error
		dr, err = uio.NewDagReader(ctx, nd, dserv)
		if err != nil {
			return nil, err
		}
	}

	return &ufsFile{
		DagReader: dr,
	}, nil
}

type MetadataStruct struct {
	Buff    []byte
	RsMeta  *chunker.RsMetaMap
	DirRoot *uio.DirNode
}

// ObtainMetadataFromDag returns MetadataStruct object.
func ObtainMetadataFromDag(ctx context.Context, metaNode ipld.Node, dserv ipld.NodeGetter) (*MetadataStruct, error) {
	mdr, err := uio.NewDagReader(ctx, metaNode, dserv)
	if err != nil {
		return nil, err
	}

	// Read all metadata
	buf := make([]byte, mdr.Size())
	_, err = mdr.CtxReadFull(ctx, buf)
	if err != nil {
		return nil, err
	}

	// Split the buf into two byte arrays.
	metaBuf, treeBuf, err := util.GetMetadataList(buf)
	if err != nil {
		return nil, err
	}

	// Read RsMetaMap
	var rsMeta chunker.RsMetaMap
	err = json.Unmarshal(metaBuf, &rsMeta)
	if err != nil {
		return nil, err
	}

	// Read tree metadata if the Dag is for directory.
	var root *uio.DirNode
	if rsMeta.IsDir {
		if treeBuf == nil {
			return nil, ErrNilTreeBuf
		}
		tmp := uio.DirNode{}
		err = json.Unmarshal(treeBuf, &tmp)
		if err != nil {
			return nil, err
		}
		root = &tmp
	}

	return &MetadataStruct{
		Buff:    buf,
		RsMeta:  &rsMeta,
		DirRoot: root,
	}, nil
}

var _ files.Directory = &ufsDirectory{}
var _ files.File = &ufsFile{}
