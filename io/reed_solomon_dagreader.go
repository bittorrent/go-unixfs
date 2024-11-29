package io

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"time"

	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	rs "github.com/klauspost/reedsolomon"
)

// reedSolomonDagReader reads a dag by concurrently merging N shards
// in a N/M encoded UnixFS dag file.
// Due to reed solomon requiring N shards to exist, we cannot perform
// stream Read or Seek on this reader.
// Everything will be pre-filled in memory before supporting DagReader
// operations from a []byte reader.
type ReedSolomonDagReader struct {
	*bytes.Reader // for Reader, Seeker, and WriteTo
	mode          os.FileMode
	modTime       time.Time
}

type nodeBufIndex struct {
	b   *bytes.Buffer
	i   int
	err error
}

// A ReedSolomonDagReader wraps M DagReaders and reads N (data) out of
// M (data + parity) concurrently to decode the original file shards for
// the returned DagReader to use.
// Optionally, accepts a list of missing shard hashes for repair and returns
// the buffered data readers on any missing shards (nil for already existing).
func NewReedSolomonDagReader(ctx context.Context, n ipld.Node, serv ipld.NodeGetter,
	numData, numParity, size uint64, isDir bool, missingShards []cid.Cid) (DagReader, []io.Reader, *bytes.Buffer, error) {
	totalShards := int(numData + numParity)
	if totalShards != len(n.Links()) {
		return nil, nil, nil, fmt.Errorf("number of links under node [%d] does not match set data + parity [%d]",
			len(n.Links()), totalShards)
	}

	// Check if all missing shards are valid
	linkIndexMap := map[string]int{}
	for i, link := range n.Links() {
		linkIndexMap[link.Cid.String()] = i
	}
	missingIndexMap := map[int]int{} // maps from shard index to missing order index
	for i, ms := range missingShards {
		index, ok := linkIndexMap[ms.String()]
		if !ok {
			return nil, nil, nil, fmt.Errorf("missing shard hash [%s] is not found", ms.String())
		}
		missingIndexMap[index] = i
	}

	// Grab at least N nodes then we are ready for re-construction
	// Timeout is set at upper caller level through context.Context
	nodeBufChan := make(chan nodeBufIndex)
	ctxWithCancel, cancel := context.WithCancel(ctx)
	for i, link := range n.Links() {
		go func(ctx context.Context, shardCID cid.Cid, index int) {
			node, err := serv.Get(ctx, shardCID)
			if err != nil {
				nodeBufChan <- nodeBufIndex{nil, index, err}
				return
			}
			dr, err := NewDagReader(ctx, node, serv)
			if err != nil {
				nodeBufChan <- nodeBufIndex{nil, index, err}
				return
			}
			var b bytes.Buffer
			_, err = io.Copy(&b, dr)
			if err != nil {
				nodeBufChan <- nodeBufIndex{nil, index, err}
				return
			}
			nodeBufChan <- nodeBufIndex{&b, index, nil}
		}(ctxWithCancel, link.Cid, i)
	}

	// Context deadline is set so it should exit eventually
	bufs := make([]*bytes.Buffer, totalShards)
	valid := 0
	for nbi := range nodeBufChan {
		if nbi.err != nil {
			continue
		}
		bufs[nbi.i] = nbi.b
		valid += 1
		if valid == int(numData) {
			// No need to get more nodes
			break
		}
	}
	cancel() // Without this, goroutines will continue.
	if valid < int(numData) {
		return nil, nil, nil, fmt.Errorf("unable to obtain at least [%d] shards to join original file", numData)
	}

	// Check if we already have everything
	dataValid := true
	for i := 0; i < int(numData); i++ {
		if bufs[i] == nil {
			dataValid = false
			break
		}
	}

	// Create rs stream
	rss, err := rs.NewStreamC(int(numData), int(numParity), true, true)
	if err != nil {
		return nil, nil, nil, err
	}

	// Reconstruct if missing some data shards
	// Also create readers for returning missing shards
	missingReaders := make([]io.Reader, len(missingShards))
	if !dataValid || len(missingShards) > 0 {
		valid := make([]io.Reader, totalShards)
		fill := make([]io.Writer, totalShards)
		// Make all valid shards
		// Only fill the missing data shards
		for i, b := range bufs {
			if b != nil {
				valid[i] = bytes.NewReader(b.Bytes())
			} else if _, ok := missingIndexMap[i]; ok || i < int(numData) {
				// Need to fill all data + missing shards
				b = &bytes.Buffer{}
				bufs[i] = b
				fill[i] = b
			}
		}
		err = rss.Reconstruct(valid, fill)
		if err != nil {
			return nil, nil, nil, err
		}
		// Only return missing shards that are actually missing
		// (valid means it's already locally available)
		for i, f := range fill {
			if f == nil {
				continue // skip non-filled
			}
			if mi, ok := missingIndexMap[i]; ok {
				missingReaders[mi] = bytes.NewReader(bufs[i].Bytes())
			}
		}
	}

	// Now join to have the final combined file reader
	shards := make([]io.Reader, totalShards)
	for i := 0; i < int(numData); i++ {
		shards[i] = bytes.NewReader(bufs[i].Bytes())
	}
	var dataBuf bytes.Buffer
	err = rss.Join(&dataBuf, shards, int64(size))
	if err != nil {
		return nil, nil, nil, err
	}

	return &ReedSolomonDagReader{Reader: bytes.NewReader(dataBuf.Bytes())}, missingReaders, &dataBuf, nil
}

func (f *ReedSolomonDagReader) Mode() os.FileMode {
	return f.mode
}

func (f *ReedSolomonDagReader) ModTime() time.Time {
	return f.modTime
}

// Size returns the total size of the data from the decoded DAG structured file
// using reed solomon algorithm.
func (rsdr *ReedSolomonDagReader) Size() uint64 {
	return uint64(rsdr.Len())
}

// Close has no effect since the underlying reader is a buffer.
func (rsdr *ReedSolomonDagReader) Close() error {
	return nil
}

// CtxReadFull is just a Read since there is no context for buffer.
func (rsdr *ReedSolomonDagReader) CtxReadFull(ctx context.Context, out []byte) (int, error) {
	return rsdr.Read(out)
}
