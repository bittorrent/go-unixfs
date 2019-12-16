package unixfile

import (
	"context"
	"io/ioutil"
	"testing"

	files "github.com/TRON-US/go-btfs-files"
	"github.com/TRON-US/go-unixfs/importer/helpers"
	uio "github.com/TRON-US/go-unixfs/io"
	testu "github.com/TRON-US/go-unixfs/test"
)

func TestUnixFsFileRead(t *testing.T) {
	dserv := testu.GetDAGServ()
	inbuf, node := testu.GetRandomNode(t, dserv, 1024, testu.UseProtoBufLeaves)
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	n, err := NewUnixfsFile(ctx, dserv, node, UnixfsFileOptions{})
	if err != nil {
		t.Fatal(err)
	}

	file := files.ToFile(n)

	outbuf, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inbuf, outbuf)
	if err != nil {
		t.Fatal(err)
	}

	// Should have no metadata
	_, err = NewUnixfsFile(ctx, dserv, node, UnixfsFileOptions{Meta: true})
	if err == nil {
		t.Fatal("no metadata error should be returned")
	}
}

func TestUnixFsFileReadWithMetadata(t *testing.T) {
	inputMeta := []byte(`{"hello":1,"world":["33","11","22"]},{}`)
	dserv := testu.GetDAGServ()
	inbuf, node := testu.GetRandomNode(t, dserv, 1024,
		testu.UseBalancedWithMetadata(helpers.DefaultLinksPerBlock, inputMeta, 512, nil))
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	// Read only data
	n, err := NewUnixfsFile(ctx, dserv, node, UnixfsFileOptions{})
	if err != nil {
		t.Fatal(err)
	}

	file := files.ToFile(n)

	outbuf, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inbuf, outbuf)
	if err != nil {
		t.Fatal(err)
	}

	// Read only metadata
	n, err = NewUnixfsFile(ctx, dserv, node, UnixfsFileOptions{Meta: true})
	if err != nil {
		t.Fatal(err)
	}

	file = files.ToFile(n)

	outbuf, err = ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inputMeta, outbuf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnixFsFileReedSolomonRead(t *testing.T) {
	dserv := testu.GetDAGServ()

	rsOpts, rsMeta := testu.UseReedSolomon(testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity,
		1024, nil, 512)
	inbuf, node := testu.GetRandomNode(t, dserv, 1024, rsOpts)
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	// Read only joined data
	n, err := NewUnixfsFile(ctx, dserv, node, UnixfsFileOptions{})
	if err != nil {
		t.Fatal(err)
	}

	file := files.ToFile(n)

	outbuf, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inbuf, outbuf)
	if err != nil {
		t.Fatal(err)
	}

	// Read only reed solomon fixed metadata
	n, err = NewUnixfsFile(ctx, dserv, node, UnixfsFileOptions{Meta: true})
	if err != nil {
		t.Fatal(err)
	}

	file = files.ToFile(n)

	outbuf, err = ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	rsMeta = append(append(rsMeta[:len(rsMeta)], ','), []byte("{}")...)
	err = testu.ArrComp(rsMeta, outbuf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnixFsFileReedSolomonMetadataRead(t *testing.T) {
	inputMeta := []byte(`{"hello":1,"world":["33","11","22"]},{}`)
	dserv := testu.GetDAGServ()

	rsOpts, rsMeta := testu.UseReedSolomon(testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity,
		1024, inputMeta, 512)
	inbuf, node := testu.GetRandomNode(t, dserv, 1024, rsOpts)
	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	// Read only joined data
	n, err := NewUnixfsFile(ctx, dserv, node, UnixfsFileOptions{})
	if err != nil {
		t.Fatal(err)
	}

	file := files.ToFile(n)

	outbuf, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inbuf, outbuf)
	if err != nil {
		t.Fatal(err)
	}

	// Read reed solomon fixed metadata + custom metadata
	n, err = NewUnixfsFile(ctx, dserv, node, UnixfsFileOptions{Meta: true})
	if err != nil {
		t.Fatal(err)
	}

	file = files.ToFile(n)

	outbuf, err = ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(testu.ExtendMetaBytes(rsMeta, inputMeta), outbuf)
	if err != nil {
		t.Fatal(err)
	}
}

func TestUnixFsFileReedSolomonReadRepair(t *testing.T) {
	dserv := testu.GetDAGServ()

	rsOpts, _ := testu.UseReedSolomon(testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity,
		1024, nil, 512)
	inbuf, node := testu.GetRandomNode(t, dserv, 1024, rsOpts)

	shards := testu.GetReedSolomonShards(t, inbuf, testu.TestRsDefaultNumData, testu.TestRsDefaultNumParity)

	ctx, closer := context.WithCancel(context.Background())
	defer closer()

	// Randomly remove some sharded nodes and repair the missing
	_, removed, removedIndex := testu.RandomRemoveNodes(t, ctx, node, dserv, 10)

	// Read only joined data (repair mode available)
	n, err := NewUnixfsFile(ctx, dserv, node, UnixfsFileOptions{RepairShards: removed})
	if err != nil {
		t.Fatal(err)
	}

	file := files.ToFile(n)

	outbuf, err := ioutil.ReadAll(file)
	if err != nil {
		t.Fatal(err)
	}

	err = testu.ArrComp(inbuf, outbuf)
	if err != nil {
		t.Fatal(err)
	}

	// Make sure the shards are really recovered
	for i, rcid := range removed {
		rn, err := dserv.Get(ctx, rcid)
		if err != nil {
			t.Fatal(err)
		}
		rr, err := uio.NewDagReader(ctx, rn, dserv)
		if err != nil {
			t.Fatal(err)
		}
		out, err := ioutil.ReadAll(rr)
		if err != nil {
			t.Fatal(err)
		}
		err = testu.ArrComp(shards[removedIndex[i]], out)
		if err != nil {
			t.Fatalf("recovering [%s] at index [%d] failed: %v", rcid.String(), i, err)
		}
	}
}
