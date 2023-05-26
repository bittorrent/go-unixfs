package testu

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	ft "github.com/bittorrent/go-unixfs"
	balanced "github.com/bittorrent/go-unixfs/importer/balanced"
	h "github.com/bittorrent/go-unixfs/importer/helpers"
	trickle "github.com/bittorrent/go-unixfs/importer/trickle"

	chunker "github.com/bittorrent/go-btfs-chunker"
	cid "github.com/ipfs/go-cid"
	u "github.com/ipfs/go-ipfs-util"
	ipld "github.com/ipfs/go-ipld-format"
	mdag "github.com/ipfs/go-merkledag"
	mdagmock "github.com/ipfs/go-merkledag/test"
	rs "github.com/klauspost/reedsolomon"
	mh "github.com/multiformats/go-multihash"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

// SizeSplitterGen creates a generator.
func SizeSplitterGen(size int64) chunker.SplitterGen {
	return func(r io.Reader) chunker.Splitter {
		return chunker.NewSizeSplitter(r, size)
	}
}

func MetaSplitterGen(size int64) chunker.SplitterGen {
	return func(r io.Reader) chunker.Splitter {
		return chunker.NewMetaSplitter(r, uint64(size))
	}
}

// GetDAGServ returns a mock DAGService.
func GetDAGServ() ipld.DAGService {
	return mdagmock.Mock()
}

// NodeOpts is used by GetNode, GetEmptyNode and GetRandomNode
type NodeOpts struct {
	Prefix cid.Prefix
	// ForceRawLeaves if true will force the use of raw leaves
	ForceRawLeaves bool
	// RawLeavesUsed is true if raw leaves or either implicitly or explicitly enabled
	RawLeavesUsed bool
	// Enables reed solomon splitter
	ReedSolomonEnabled bool
	// Enables balanced DAG to be used than trickle.
	Balanced        bool
	rsNumData       uint64
	rsNumParity     uint64
	Metadata        []byte
	ChunkSize       uint64
	MaxLinks        int
	MetadataToMdify []byte
}

// Some shorthands for NodeOpts.
var (
	UseProtoBufLeaves = NodeOpts{Prefix: mdag.V0CidPrefix()}
	UseRawLeaves      = NodeOpts{Prefix: mdag.V0CidPrefix(), ForceRawLeaves: true, RawLeavesUsed: true}
	UseCidV1          = NodeOpts{Prefix: mdag.V1CidPrefix(), RawLeavesUsed: true}
	UseBlake2b256     NodeOpts
)

const (
	TestRsDefaultNumData   = 10
	TestRsDefaultNumParity = 20
)

func UseBalancedWithMetadata(maxLinks int, mdata []byte, chkSize uint64, mdata2 []byte) NodeOpts {
	return NodeOpts{Prefix: mdag.V0CidPrefix(), Balanced: true, Metadata: mdata,
		ChunkSize: chkSize, MaxLinks: maxLinks, MetadataToMdify: mdata2}
}

func UseTrickleWithMetadata(maxLinks int, mdata []byte, chkSize uint64, mdata2 []byte) NodeOpts {
	return NodeOpts{Prefix: mdag.V0CidPrefix(), Metadata: mdata, ChunkSize: chkSize,
		MaxLinks: maxLinks, MetadataToMdify: mdata2}
}

func ReedSolomonMetaBytes(numData, numParity, fileSize uint64) []byte {
	return []byte(fmt.Sprintf(`{"NumData":%d,"NumParity":%d,"FileSize":%d}`,
		numData, numParity, fileSize))
}

func ExtendMetaBytes(existing []byte, extended []byte) []byte {
	var returnBytes []byte
	if existing != nil {
		// Splice two meta json objects
		if extended != nil {
			returnBytes = append(append(existing[:len(existing)-1], ','), extended[1:]...)
		} else {
			returnBytes = existing
		}
	} else {
		returnBytes = extended
	}
	returnBytes = append(append(returnBytes[:len(returnBytes)], '#'), []byte("{}")...)
	return returnBytes
}

func UseReedSolomon(numData, numParity, fileSize uint64, mdata []byte, chkSize uint64) (NodeOpts, []byte) {
	// Reed Solomon have intrinsic metadata, so must merge them
	rsMeta := ReedSolomonMetaBytes(numData, numParity, fileSize)
	metaBytes := ExtendMetaBytes(rsMeta, mdata)
	return NodeOpts{
		Prefix:             mdag.V0CidPrefix(),
		ReedSolomonEnabled: true,
		rsNumData:          numData,
		rsNumParity:        numParity,
		Metadata:           metaBytes,
		ChunkSize:          chkSize,
	}, rsMeta
}

func init() {
	UseBlake2b256 = UseCidV1
	UseBlake2b256.Prefix.MhType = mh.Names["blake2b-256"]
	UseBlake2b256.Prefix.MhLength = -1
}

func GetDagBuilderParams(dserv ipld.DAGService, opts NodeOpts) *h.DagBuilderParams {
	maxLinks := h.DefaultLinksPerBlock
	if opts.MaxLinks != 0 {
		maxLinks = opts.MaxLinks
	}
	return &h.DagBuilderParams{
		Dagserv:       dserv,
		Maxlinks:      maxLinks,
		CidBuilder:    opts.Prefix,
		RawLeaves:     opts.RawLeavesUsed,
		TokenMetadata: opts.Metadata,
		ChunkSize:     opts.ChunkSize,
	}
}

// GetNode returns a unixfs file node with the specified data.
func GetNode(t testing.TB, dserv ipld.DAGService, data []byte, opts NodeOpts) ipld.Node {
	in := bytes.NewReader(data)
	dbp := GetDagBuilderParams(dserv, opts)

	if opts.ReedSolomonEnabled {
		spl, err := chunker.NewReedSolomonSplitter(in, opts.rsNumData, opts.rsNumParity, 500)
		if err != nil {
			t.Fatal(err)
		}
		db, err := dbp.New(spl)
		if err != nil {
			t.Fatal(err)
		}

		if db.IsThereMetaData() && !db.IsMetaDagBuilt() {
			_, err := balanced.BuildMetadataDag(db)
			if err != nil {
				t.Fatal(err)
			}
			db.SetMetaDagBuilt(true)
		}

		node, err := balanced.Layout(db)
		if err != nil {
			t.Fatal(err)
		}

		return node
	}

	db, err := dbp.New(chunker.SizeSplitterGen(500)(in))
	if err != nil {
		t.Fatal(err)
	}
	var node ipld.Node
	if db.IsThereMetaData() && !db.IsMetaDagBuilt() {
		if opts.Balanced {
			_, err = balanced.BuildMetadataDag(db)
		} else {
			err = trickle.BuildMetadataDag(db)
		}
		if err != nil {
			t.Fatal(err)
		}
		db.SetMetaDagBuilt(true)
	}
	if opts.Balanced {
		node, err = balanced.Layout(db)
	} else {
		node, err = trickle.Layout(db)
	}
	if err != nil {
		t.Fatal(err)
	}

	return node
}

// GetEmptyNode returns an empty unixfs file node.
func GetEmptyNode(t testing.TB, dserv ipld.DAGService, opts NodeOpts) ipld.Node {
	return GetNode(t, dserv, []byte{}, opts)
}

// GetRandomNode returns a random unixfs file node.
func GetRandomNode(t testing.TB, dserv ipld.DAGService, size int64, opts NodeOpts) ([]byte, ipld.Node) {
	in := io.LimitReader(u.NewTimeSeededRand(), size)
	buf, err := io.ReadAll(in)
	if err != nil {
		t.Fatal(err)
	}

	node := GetNode(t, dserv, buf, opts)
	return buf, node
}

func GetNodeWithGivenData(t testing.TB, dserv ipld.DAGService, data []byte, opts NodeOpts) ipld.Node {
	node := GetNode(t, dserv, data, opts)
	return node
}

// ArrComp checks if two byte slices are the same.
func ArrComp(a, b []byte) error {
	if len(a) != len(b) {
		return fmt.Errorf("arrays differ in length. %d != %d", len(a), len(b))
	}
	for i, v := range a {
		if v != b[i] {
			return fmt.Errorf("arrays differ at index: %d", i)
		}
	}
	return nil
}

// PrintDag pretty-prints the given dag to stdout.
func PrintDag(nd *mdag.ProtoNode, ds ipld.DAGService, indent int) {
	fsn, err := ft.FSNodeFromBytes(nd.Data())
	if err != nil {
		panic(err)
	}

	for i := 0; i < indent; i++ {
		fmt.Print(" ")
	}
	fmt.Printf("{size = %d, type = %s, children = %d", fsn.FileSize(), fsn.Type().String(), fsn.NumChildren())
	if len(nd.Links()) > 0 {
		fmt.Println()
	}
	for _, lnk := range nd.Links() {
		child, err := lnk.GetNode(context.Background(), ds)
		if err != nil {
			panic(err)
		}
		PrintDag(child.(*mdag.ProtoNode), ds, indent+1)
	}
	if len(nd.Links()) > 0 {
		for i := 0; i < indent; i++ {
			fmt.Print(" ")
		}
	}
	fmt.Println("}")
}

// GetReedSolomonShards gets the shards from the original file
func GetReedSolomonShards(t *testing.T, inbuf []byte, numData, numParity uint64) [][]byte {
	enc, err := rs.New(int(numData), int(numParity))
	if err != nil {
		t.Fatal("unable to create reference reedsolomon object", err)
	}
	shards, err := enc.Split(inbuf)
	if err != nil {
		t.Fatal("unable to split reference reedsolomon shards", err)
	}
	err = enc.Encode(shards)
	if err != nil {
		t.Fatal("unable to encode reference reedsolomon shards", err)
	}
	return shards
}

// RandomRemoveNodes randomly removes n sharded nodes from under the file root
// Can remove from 1-n nodes since rand can repeat index
// Returns shard root node, the list of removed hashes and indices in the original links
func RandomRemoveNodes(t *testing.T, ctx context.Context,
	root ipld.Node, dserv ipld.DAGService, n int) (ipld.Node, []cid.Cid, []int) {
	// Skip metadata, pass the reed solomon root node
	rsnode, err := root.Links()[1].GetNode(ctx, dserv)
	if err != nil {
		t.Fatal(err)
	}
	allLinks := rsnode.Links()
	missingMap := map[int]bool{}
	var removed []cid.Cid
	var removedIndex []int
	for i := 0; i < n; i++ {
		ri := rand.Intn(len(allLinks))
		if _, ok := missingMap[ri]; ok {
			continue
		} else {
			missingMap[ri] = true
		}
		removed = append(removed, allLinks[ri].Cid)
		removedIndex = append(removedIndex, ri)
	}
	err = dserv.RemoveMany(ctx, removed)
	if err != nil {
		t.Fatal(err)
	}
	return rsnode, removed, removedIndex
}
