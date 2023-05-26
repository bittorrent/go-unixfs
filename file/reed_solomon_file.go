package unixfile

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/bittorrent/go-unixfs/importer/balanced"
	ihelper "github.com/bittorrent/go-unixfs/importer/helpers"
	"github.com/bittorrent/go-unixfs/importer/trickle"
	"github.com/bittorrent/go-unixfs/util"

	uio "github.com/bittorrent/go-unixfs/io"

	chunker "github.com/bittorrent/go-btfs-chunker"
	files "github.com/bittorrent/go-btfs-files"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

// TODO: 12/11 possibly create a pool DP if not possible to have a session..
// But use api.core().getSession(ctx)?

const (
	ReedSolomonDagOff   = 0
	ReedSolomonDagOpen  = 1
	ReedSolomonDagNext  = 2
	ReedSolomonDagClose = 3
)

type ReedSolomonDag struct {
	state     int
	buff      *bytes.Buffer
	offset    uint64
	curFile   *uio.FileNode
	lock      sync.RWMutex
	cidString string
}

type RsDirectory struct {
	ctx       context.Context
	dserv     ipld.DAGService
	dir       *uio.ReedSolomonDirectory
	size      int64
	cidString string
}

func (d *RsDirectory) Close() error {
	return nil
}

func (d *RsDirectory) Entries() files.DirIterator {

	fileCh := make(chan interface{}, prefetchFiles)
	errCh := make(chan error, 1)

	// rsDirectory is retrieved from cache, so ctx may be expired
	ctx := d.ctx
	if ctx == nil || ctx.Err() != nil {
		ctx, _ = context.WithTimeout(context.Background(), time.Hour)
		ctx = context.WithValue(ctx, d.cidString, d.ctx.Value(d.cidString))
	}

	// Invoke goroutine to provide links of the current receiver `d`
	// via `fileCh`.
	go func() {
		errCh <- d.dir.ForEachLink(ctx, func(link interface{}) error {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			select {
			case fileCh <- link:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		})

		// close channels indicating sender side is done.
		close(errCh)
		close(fileCh)
	}()

	return &RsIterator{
		ctx:                   ctx,
		cidString:             d.cidString,
		files:                 fileCh,
		rsDir:                 d,
		errCh:                 errCh,
		dserv:                 d.dserv,
		breadthFirstTraversal: true,
	}
}

func (d *RsDirectory) Size() (int64, error) {
	return d.size, nil
}

func (f *RsDirectory) SetSize(size int64) error {
	return errors.New("not supported")
}

func (f *RsDirectory) IsReedSolomon() bool {
	return true
}

type RsIterator struct {
	state                 int
	ctx                   context.Context
	cidString             string
	files                 chan interface{}
	dserv                 ipld.DAGService
	rsDir                 *RsDirectory
	breadthFirstTraversal bool

	curName string
	curFile files.Node

	err   error
	errCh chan error
}

func (it *RsIterator) Name() string {
	return it.curName
}

func (it *RsIterator) Node() files.Node {
	return it.curFile
}

func (it *RsIterator) Next() bool {
	if it.err != nil {
		return false
	}

	var l interface{}
	var ok bool
	// Loop until getting `l` without an error.
	for !ok { // while ok == false
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

	node, err := GetRsNode(l)
	if err != nil {
		it.err = err
		return false
	}

	it.curName = node.Name()
	if it.curName == uio.SmallestString {
		return it.err == nil
	}

	switch nd := node.(type) {
	case *uio.DirNode:
		it.curFile, it.err = NewReedSolomonSubDirectory(it.ctx, it.dserv, it.cidString, nd)
	case *uio.FileNode:
		it.curFile, it.err =
			NewReedSolomonFileUnderDirectory(it.ctx, it.dserv, it.cidString, nd, it.breadthFirstTraversal)
	case *uio.SymlinkNode:
		it.curFile, it.err = files.NewLinkFile(nd.Data, nil), nil
	default:
		it.err = errors.New("unexpected Node at Next(), possibly program error")
		return false
	}

	return it.err == nil
}

func GetRsNode(l interface{}) (uio.Node, error) {
	m, ok := l.(map[string]interface{})
	if !ok {
		return nil, errors.New("GetRsNode(): unexpected Node format. Probably program error.")
	}
	mm, ok := m["BaseNode"].(map[string]interface{})
	if !ok {
		return nil, errors.New("GetRsNode(): unexpected Node format. Possibly program error.")
	}

	// Marshal mm and Unmarshal BaseNode to get BaseNode.
	var base uio.BaseNode

	b, err := json.Marshal(mm)
	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(b, &base); err != nil {
		return nil, err
	}

	switch base.NodeType {
	case uio.DirNodeType:
		return &uio.DirNode{
			BaseNode: base,
		}, nil
	case uio.FileNodeType:
		return &uio.FileNode{
			BaseNode: base,
		}, nil
	case uio.SymlinkNodeType:
		data := m["Data"].(string)
		return &uio.SymlinkNode{
			BaseNode: base,
			Data:     data,
		}, nil
	default:
		return nil, errors.New("GetRsNode(): unexpected NodeType. Possibly program error.")
	}
}

func (it *RsIterator) Err() error {
	return it.err
}

func (it *RsIterator) BreadthFirstTraversal() {
	it.breadthFirstTraversal = true
}

type rsFile struct {
	uio.DagReader
}

func (f *rsFile) Size() (int64, error) {
	return int64(f.DagReader.Size()), nil
}

func newReedSolomonDir(ctx context.Context, dserv ipld.DAGService, cid string, nd *uio.DirNode) (files.Directory, error) {
	dir, err := uio.NewReedSolomonDirectoryFromNode(dserv, nd)
	if err != nil {
		return nil, err
	}

	size := nd.NodeSize()

	return &RsDirectory{
		ctx:       ctx,
		dserv:     dserv,
		dir:       dir,
		size:      int64(size),
		cidString: cid,
	}, nil
}

// NewReedSolomonDirectory returns files.Node for the root DAG node of a BTFS object in Reed-Solomon encoding.
// This builder function is supposed to be called only one time for `btfs get` for a BTFS merkle DAG.
func NewReedSolomonDirectory(ctx context.Context, root ipld.Node, dataNode ipld.Node, dserv ipld.DAGService, opts UnixfsFileOptions,
	metaStruct *MetadataStruct) (files.Node, error) {
	// Create reader and get root.
	rsMeta := metaStruct.RsMeta
	_, mrs, dataBuf, err := uio.NewReedSolomonDagReader(ctx, dataNode, dserv, rsMeta.NumData, rsMeta.NumParity,
		rsMeta.FileSize, rsMeta.IsDir, opts.RepairShards)
	if err != nil {
		return nil, err
	}

	// Repair designated shards from opts.RepairShards.
	err = checkAndRecoverShards(ctx, root, dserv, opts, mrs)
	if err != nil {
		return nil, err
	}

	// Set root directory node to `dirRoot`.
	dirRoot := metaStruct.DirRoot
	if dirRoot == nil {
		return nil, errors.New("nil root node encountered")
	}

	// Create a Dag instance and set as context value.
	cid := dataNode.Cid().String()
	ctx = InitDag(ctx, dataBuf, cid)

	// Create and return files.Dir
	return newReedSolomonDir(ctx, dserv, cid, dirRoot)
}

func NewReedSolomonDag(dataBuf *bytes.Buffer, cid string) *ReedSolomonDag {
	rsDagInstance := &ReedSolomonDag{
		state:     ReedSolomonDagOff,
		buff:      dataBuf,
		offset:    0,
		curFile:   nil,
		cidString: cid,
		lock:      sync.RWMutex{},
	}

	return rsDagInstance
}

func InitDag(ctx context.Context, dataBuf *bytes.Buffer, cid string) context.Context {
	rsDagInstance := NewReedSolomonDag(dataBuf, cid)
	rsDagInstance.state = ReedSolomonDagOpen

	ctx = context.WithValue(ctx, cid, rsDagInstance)

	return ctx
}

func GetDag(ctx context.Context, cid string) *ReedSolomonDag {
	if v, ok := ctx.Value(cid).(*ReedSolomonDag); ok {
		return v
	}
	return nil
}

// NewReedSolomonSugDirectory creates and returns a files.Dir with the given uio.Node.
func NewReedSolomonSubDirectory(ctx context.Context, dserv ipld.DAGService, cid string, nd uio.Node) (files.Node, error) {
	dNode, ok := nd.(*uio.DirNode)
	if !ok {
		return nil, errors.New("expected DirNode, but did not get it. Possibly a program error.")
	}

	return newReedSolomonDir(ctx, dserv, cid, dNode)
}

// NewReedSolomonFileUnderDirectory returns a files.Node for the given `nd` Node.
// This functioon is called within the context of Reed-Solomon DAG for directory.
// The given `nd` is a uio.FileNode and is used to create a reader.
func NewReedSolomonFileUnderDirectory(
	ctx context.Context, dserv ipld.DAGService, cid string, nd uio.Node, bfs bool) (files.Node, error) {
	// Locking is for synchronizing write access to rsDagInstance.offset.
	// But rsDagInstance.offset may not be necessary. We use this field to verify the offset in `nd`.
	rsDagInstance := GetDag(ctx, cid)
	if rsDagInstance == nil {
		return nil, errors.New("cannot find rsDagInstance from the current Context.context")
	}
	rsDagInstance.lock.Lock()
	defer rsDagInstance.lock.Unlock()

	rsDagInstance.state = ReedSolomonDagNext

	fNode, ok := nd.(*uio.FileNode)
	if !ok {
		return nil, errors.New("expected FileNode, but did not get it. Possibly a program error.")
	}

	b := rsDagInstance.buff.Bytes()
	offset := fNode.StartOffset
	if !bfs && rsDagInstance.offset != offset {
		return nil, errors.New("offset from the given FileNode is invalid.")
	}
	newOffset := offset + uint64(fNode.NodeSize())
	if newOffset > uint64(len(b)) {
		return nil, errors.New("new offset is greater than buffer size.")
	}
	dr := bytes.NewReader(b[offset:newOffset])

	rsDagInstance.offset = newOffset
	return &rsFile{
		DagReader: &uio.ReedSolomonDagReader{dr},
	}, nil
}

func NewReedSolomonStandaloneFile(ctx context.Context, root ipld.Node, dataNode ipld.Node, dserv ipld.DAGService, opts UnixfsFileOptions,
	metaStruct *MetadataStruct) (files.Node, error) {
	rsMeta := metaStruct.RsMeta
	dr, mrs, _, err := uio.NewReedSolomonDagReader(ctx, dataNode, dserv, rsMeta.NumData, rsMeta.NumParity,
		rsMeta.FileSize, rsMeta.IsDir, opts.RepairShards)
	if err != nil {
		return nil, err
	}

	err = checkAndRecoverShards(ctx, root, dserv, opts, mrs)
	if err != nil {
		return nil, err
	}

	return &rsFile{
		DagReader: dr,
	}, nil
}

func checkAndRecoverShards(ctx context.Context, root ipld.Node, dserv ipld.DAGService, opts UnixfsFileOptions, mrs []io.Reader) error {
	// Check which ones need recovery
	var recovered []io.Reader
	var rcids []cid.Cid
	for i, mr := range mrs {
		if mr != nil {
			recovered = append(recovered, mr)
			rcids = append(rcids, opts.RepairShards[i])
		}
	}
	if len(recovered) > 0 {
		err := addRecoveredShards(ctx, root, dserv, recovered, rcids)
		if err != nil {
			return err
		}
	}
	return nil
}

// addRecoveredShards mimics adding reed solomon shards anew according to the
// original adder options.rootNode ipld.Node
func addRecoveredShards(ctx context.Context, rootNode ipld.Node, ds ipld.DAGService,
	recovered []io.Reader, rcids []cid.Cid) error {
	b, err := uio.GetMetaDataFromDagRoot(ctx, rootNode, ds)
	if err != nil {
		return err
	}

	bMeta := util.GetMetadataElement(b)
	sm, err := ihelper.GetOrDefaultSuperMeta(bMeta)
	if err != nil {
		return err
	}

	// Recover shard has a default size-based chunker
	sc := fmt.Sprintf("size-%d", sm.ChunkSize)
	for i, r := range recovered {
		chnk, err := chunker.FromString(r, sc)
		if err != nil {
			return err
		}

		// Re-create (as much as possible) the original params
		params := ihelper.DagBuilderParams{
			Dagserv:   ds,
			Maxlinks:  int(sm.MaxLinks),
			ChunkSize: sm.ChunkSize,
		}

		db, err := params.New(chnk)
		if err != nil {
			return err
		}

		var sn ipld.Node // new shard root node
		if sm.TrickleFormat {
			sn, err = trickle.Layout(db)
		} else {
			sn, err = balanced.Layout(db)
		}
		if err != nil {
			return err
		}

		if !rcids[i].Equals(sn.Cid()) {
			return fmt.Errorf("recovered node [%s] does not match original [%s]",
				sn.Cid().String(), rcids[i].String())
		}
	}

	return nil
}
