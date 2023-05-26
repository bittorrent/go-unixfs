package io

import (
	"context"
	"errors"
	files "github.com/bittorrent/go-btfs-files"
	ipld "github.com/ipfs/go-ipld-format"
)

type Node interface {
	Path() string
	NodeSize() int64
	Name() string
}

const (
	DirNodeType     = 1
	FileNodeType    = 2
	SymlinkNodeType = 3
)

type BaseNode struct {
	NodeType    int
	NodePath    string
	NodeName    string
	Siz         uint64
	StartOffset uint64
	Links       []interface{}
}

// files.Directory is input directory
type DirNode struct {
	BaseNode        `json:"BaseNode"`
	files.Directory `json:"files.Directory"`
}
type FileNode struct {
	BaseNode `json:"BaseNode"`
	//files.File `json:"files.File"`
}
type SymlinkNode struct {
	BaseNode `json:"BaseNode"`
	Data     string
	//files.Symlink `json:"files.Symlink"`
}

// ReedSolomonDirectory is the implementation of `Directory for Reed-Solomon
// BTFS file. All the entries are stored in a single node.
type ReedSolomonDirectory struct {
	DNode *DirNode
	dserv ipld.DAGService
}

func (n *BaseNode) Path() string {
	return n.NodePath
}
func (n *BaseNode) Name() string {
	return n.NodeName
}
func (n *BaseNode) NodeSize() int64 {
	return int64(n.Siz)
}

// ForEachLink implements the `Directory` interface.
func (d *ReedSolomonDirectory) ForEachLink(ctx context.Context, f func(interface{}) error) error {
	if d.DNode == nil {
		return errors.New("nil d.Dnode encountered")
	}
	for _, l := range d.DNode.Links {
		if err := f(l); err != nil {
			return err
		}
	}
	return nil
}

// Links implements the `Directory` interface.
func (d *ReedSolomonDirectory) Links(ctx context.Context) []interface{} {
	if d.DNode == nil {
		return nil
	}
	return d.DNode.Links
}

func NewReedSolomonDirectory(dserv ipld.DAGService) *ReedSolomonDirectory {
	return &ReedSolomonDirectory{
		DNode: nil,
		dserv: dserv,
	}
}

// NewReedSolomonDirectoryFromNode loads a ReedSolomon directory
// from the given DirNode and DAGService.
func NewReedSolomonDirectoryFromNode(dserv ipld.DAGService, dn *DirNode) (*ReedSolomonDirectory, error) {
	dir := new(ReedSolomonDirectory)
	dir.DNode = dn
	dir.dserv = dserv
	return dir, nil
}
