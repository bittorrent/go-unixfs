package io

import (
	"context"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

	testu "github.com/TRON-US/go-unixfs/test"

	"github.com/ipfs/go-cid"
	rs "github.com/klauspost/reedsolomon"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func TestReedSolomonRead(t *testing.T) {
	dserv := testu.GetDAGServ()

	rsOpts, _ := testu.UseReedSolomon(testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity,
		1024, nil, 512)
	inbuf, node := testu.GetRandomNode(t, dserv, 1024, rsOpts)
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	// Skip metadata, pass the reed solomon root node
	rsnode, err := node.Links()[1].GetNode(ctx, dserv)
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := NewReedSolomonDagReader(ctx, rsnode, dserv,
		testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity, uint64(len(inbuf)), nil)
	if err != nil {
		t.Fatal(err)
	}

	outbuf, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inbuf, outbuf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReedSolomonWithMetadataRead(t *testing.T) {
	// This is extra metadata, in addition to reed solomon's fixed metadata
	inputMdata := []byte(`{"nodeid":"QmURnhjU6b2Si4rqwfpD4FDGTzJH3hGRAWSQmXtagywwdz","Price":12.4}`)
	dserv := testu.GetDAGServ()

	rsOpts, rsMeta := testu.UseReedSolomon(testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity,
		1024, inputMdata, 512)
	inbuf, node := testu.GetRandomNode(t, dserv, 1024, rsOpts)
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	mnode, err := node.Links()[0].GetNode(ctx, dserv)
	if err != nil {
		t.Fatal(err)
	}
	rsnode, err := node.Links()[1].GetNode(ctx, dserv)
	if err != nil {
		t.Fatal(err)
	}
	reader, _, err := NewReedSolomonDagReader(ctx, rsnode, dserv,
		testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity, uint64(len(inbuf)), nil)
	if err != nil {
		t.Fatal(err)
	}

	mreader, err := NewDagReader(ctx, mnode, dserv)
	if err != nil {
		t.Fatal(err)
	}

	outbuf, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	moutbuf, err := ioutil.ReadAll(mreader)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inbuf, outbuf)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(testu.ExtendMetaBytes(rsMeta, inputMdata), moutbuf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestReedSolomonReadRepair(t *testing.T) {
	dserv := testu.GetDAGServ()

	rsOpts, _ := testu.UseReedSolomon(testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity,
		1024, nil, 512)
	inbuf, node := testu.GetRandomNode(t, dserv, 1024, rsOpts)

	// Get original shards
	enc, err := rs.New(testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity)
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

	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	// Skip metadata, pass the reed solomon root node
	rsnode, err := node.Links()[1].GetNode(ctx, dserv)
	if err != nil {
		t.Fatal(err)
	}

	// Randomly remove some sharded nodes and repair the missing
	// Can remove from 1-10 nodes since rand can repeat index
	allLinks := rsnode.Links()
	missingMap := map[int]bool{}
	var removed []cid.Cid
	var removedIndex []int
	for i := 0; i < 10; i++ {
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

	reader, repaired, err := NewReedSolomonDagReader(ctx, rsnode, dserv,
		testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity, uint64(len(inbuf)), removed)
	if err != nil {
		t.Fatal(err)
	}

	outbuf, err := ioutil.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inbuf, outbuf)
	if err != nil {
		t.Fatal(err)
	}

	// Check repaired shards
	for i, rr := range repaired {
		rbuf, err := ioutil.ReadAll(rr)
		if err != nil {
			t.Fatal(err)
		}

		err = testu.ArrComp(shards[removedIndex[i]], rbuf)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TODO: Currently we don't test the rest of the functions since everything
// is performed against a standard bytes.Buffer (implementation).
// Eventually, for completeness, we should.
