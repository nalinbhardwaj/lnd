package routing

import (
	"fmt"

	"github.com/boltdb/bolt"
	"github.com/lightningnetwork/lnd/channeldb"
	"github.com/roasbeef/btcd/btcec"
)

// TODO(roasbeef): abstract out graph to interface
//  * add in-memory version of graph for tests

// TODO(nalinbhardwaj): move routing.ChannelGraphSource here.

// Vertex is a simple alias for the serialization of a compressed Bitcoin
// public key.
type Vertex [33]byte

// NewVertex returns a new Vertex given a public key.
func NewVertex(pub *btcec.PublicKey) Vertex {
	var v Vertex
	copy(v[:], pub.SerializeCompressed())
	return v
}

// String returns a human readable version of the Vertex which is the
// hex-encoding of the serialized compressed public key.
func (v Vertex) String() string {
	return fmt.Sprintf("%x", v[:])
}

// Edge is a struct holding the edge info we need when performing path
// finding using a graphSource.
type Edge struct {
	edgeInfo *channeldb.ChannelEdgeInfo
	outEdge  *channeldb.ChannelEdgePolicy
	inEdge   *channeldb.ChannelEdgePolicy
}

// graphSource is a wrapper around a graph that can either be in memory or
// on disk, like the regular channeldb.ChannelGraph.
type graphSource interface {
	ForEachNode(func(Vertex) error) error
	ForEachChannel(Vertex, func(*Edge) error) error

	// Async versions of same functions
	AsyncForEachNode(func(Vertex, chan error)) error
	AsyncForEachChannel(Vertex, func(*Edge, chan error)) error
}

// memChannelGraph is a implementation of the routing.graphSource backed by
// an in-memory graph.
type memChannelGraph struct {
	nodes []Vertex
	graph map[Vertex][]*Edge
}

// A compile time assertion to ensure memChannelGraph meets the
// routing.graphSource interface.
var _ graphSource = (*memChannelGraph)(nil)

// newMemChannelGraphFromDatabase returns an instance of memChannelGraph
// from an instance of channeldb.ChannelGraph.
func newMemChannelGraphFromDatabase(db *channeldb.ChannelGraph) (*memChannelGraph, error) {
	res := memChannelGraph{
		graph: make(map[Vertex][]*Edge),
	}

	// Create entry for each node into graph.
	err := db.ForEachNode(nil,
		func(tx *bolt.Tx, node *channeldb.LightningNode) error {
			nodeVertex := Vertex(node.PubKeyBytes)
			res.nodes = append(res.nodes, nodeVertex)

			var adjList []*Edge

			err := node.ForEachChannel(nil, func(tx *bolt.Tx,
				edgeInfo *channeldb.ChannelEdgeInfo,
				outEdge, inEdge *channeldb.ChannelEdgePolicy) error {
				e := &Edge{
					edgeInfo: edgeInfo,
					outEdge:  outEdge,
					inEdge:   inEdge,
				}
				adjList = append(adjList, e)
				return nil
			})
			if err != nil {
				return err
			}
			res.graph[nodeVertex] = adjList

			return nil
		})
	if err != nil {
		return nil, err
	}

	return &res, nil
}

// ForEachNode is a function that should be called once for each connected
// node within the channel graph. If the passed callback returns an
// error, then execution should be terminated.
//
// NOTE: Part of the routing.graphSource interface.
func (m memChannelGraph) ForEachNode(cb func(Vertex) error) error {
	for _, node := range m.nodes {
		if err := cb(node); err != nil {
			return err
		}
	}

	return nil
}

// AsyncForEachNode is a function that should be called once for each connected
// node within the channel graph. It runs on upto 20 nodes concurrently. If
// the passed callback returns an error, then execution should be terminated.
//
// NOTE: Part of the routing.graphSource interface.
func (m memChannelGraph) AsyncForEachNode(cb func(Vertex, chan error)) error {
	concurrency, cnt := 20, 0
	err := make(chan error, len(m.nodes))

	for _, node := range m.nodes {
		go cb(node, err)
		cnt++

		for cnt > concurrency {
			cnt--
			if res := <-err; res != nil {
				return res
			}
		}
	}

	for cnt > 0 {
		cnt--
		if res := <-err; res != nil {
			return res
		}
	}

	return nil
}

// ForEachChannel is a function that will be used to iterate through all edges
// emanating from/to the target node. For each active channel, this function
// should be called with the populated memEdge that
// describes the active channel.
//
// NOTE: Part of the routing.graphSource interface.
func (m memChannelGraph) ForEachChannel(target Vertex, cb func(*Edge) error) error {
	for _, channel := range m.graph[target] {
		if err := cb(channel); err != nil {
			return err
		}
	}

	return nil
}

// AsyncForEachChannel is a function that will be used to iterate through all edges
// emanating from/to the target node concurrently. For each active channel, this
// function should be called with the populated memEdge that
// describes the active channel.
//
// NOTE: Part of the routing.graphSource interface.
func (m memChannelGraph) AsyncForEachChannel(target Vertex, cb func(*Edge, chan error)) error {
	concurrency, cnt := 20, 0
	err := make(chan error, len(m.graph[target]))

	for _, channel := range m.graph[target] {
		go cb(channel, err)
		cnt++

		for cnt > concurrency {
			cnt--
			if res := <-err; res != nil {
				return res
			}
		}
	}

	for cnt > 0 {
		cnt--
		if res := <-err; res != nil {
			return res
		}
	}

	return nil
}
