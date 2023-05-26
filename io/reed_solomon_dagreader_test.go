package io

import (
	"context"
	"io/ioutil"
	"testing"

	testu "github.com/bittorrent/go-unixfs/test"
)

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
	reader, _, _, err := NewReedSolomonDagReader(ctx, rsnode, dserv,
		testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity, uint64(len(inbuf)), false, nil)
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
	reader, _, _, err := NewReedSolomonDagReader(ctx, rsnode, dserv,
		testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity, uint64(len(inbuf)), false, nil)
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

	shards := testu.GetReedSolomonShards(t, inbuf, testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity)

	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	// Randomly remove some sharded nodes and repair the missing
	rsnode, removed, removedIndex := testu.RandomRemoveNodes(t, ctx, node, dserv, 10)

	reader, repaired, _, err := NewReedSolomonDagReader(ctx, rsnode, dserv,
		testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity, uint64(len(inbuf)), false, removed)
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
