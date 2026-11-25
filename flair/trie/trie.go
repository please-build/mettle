// Package trie provides a trie implementation specialised to Flair's requirements.
//
// Not every possible setup is supported. Maybe eventually it will get cleverer.
package trie

import (
	"fmt"
	"strings"

	apb "github.com/bazelbuild/remote-apis/build/bazel/remote/asset/v1"
	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bs "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"

	ppb "github.com/please-build/mettle/proto/purity"
)

// A Trie provides fast lookups on hash keys.
type Trie struct {
	dialCallback DialCallback
	root         node
	nodes        []node // premature optimisation
	servers      []Server
}

// A Server holds several gRPC connections to the same server.
type Server struct {
	CAS    pb.ContentAddressableStorageClient
	AC     pb.ActionCacheClient
	BS     bs.ByteStreamClient
	Exe    pb.ExecutionClient
	Fetch  apb.FetchClient
	GC     ppb.GCClient
	Health grpc_health_v1.HealthClient
	Start  string
	End    string
	Failed int64
}

// A node represents a single node within the trie.
type node struct {
	children [16]struct {
		server *Server
		node   *node
	}
}

// A DialCallback is called when we need to dial a new gRPC server.
type DialCallback func(address string) (*grpc.ClientConn, error)

// New creates a new trie.
func New(callback DialCallback) *Trie {
	return &Trie{dialCallback: callback}
}

// Add adds a node to this trie.
// It returns an error on any failure, including overlapping ranges.
func (t *Trie) Add(rangeSpec, address string) error {
	start, end, err := t.parseRange(rangeSpec)
	if err != nil {
		return err
	}
	return t.AddRange(start, end, address)
}

func (t *Trie) parseRange(rangeSpec string) (string, string, error) {
	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("Bad format for range: %s", rangeSpec)
	}
	return parts[0], parts[1], nil
}

// AddAll adds everything from the given map to this trie.
// It produces a mildly better memory layout than repeatedly calling Add.
func (t *Trie) AddAll(spec map[string]string) error {
	t.servers = make([]Server, 0, len(spec))
	// Find the deepest range, which tells us how many nodes we'll need
	depth := 1
	for rangeSpec := range spec {
		if from, _, _ := t.parseRange(rangeSpec); len(from) > depth {
			depth = len(from)
		}
	}
	nodes := 0
	size := 1
	for i := 0; i < depth; i++ {
		i += size
		size *= 16
	}
	t.nodes = make([]node, 0, nodes)
	for rangeSpec, address := range spec {
		if err := t.Add(rangeSpec, address); err != nil {
			return err
		}
	}
	return nil
}

// AddRange adds a specific hash range to this trie.
func (t *Trie) AddRange(start, end, address string) error {
	if len(start) != len(end) {
		return fmt.Errorf("Mismatching range lengths: %s / %s", start, end)
	}
	conn, err := t.dialCallback(address)
	if err != nil {
		return err
	}
	// We always set up all clients here, even if they won't all be used for this connection.
	s := Server{
		CAS:    pb.NewContentAddressableStorageClient(conn),
		AC:     pb.NewActionCacheClient(conn),
		BS:     bs.NewByteStreamClient(conn),
		Exe:    pb.NewExecutionClient(conn),
		Fetch:  apb.NewFetchClient(conn),
		GC:     ppb.NewGCClient(conn),
		Health: grpc_health_v1.NewHealthClient(conn),
		Start:  start,
		End:    end,
	}
	t.servers = append(t.servers, s)
	return t.add(&t.root, start, end, &s)
}

func (t *Trie) add(n *node, start, end string, server *Server) error {
	for i := toInt(start[0]); i <= toInt(end[0]); i++ {
		if child := &n.children[i]; len(start) == 1 {
			child.server = server
		} else {
			if child.node == nil {
				t.nodes = append(t.nodes, node{})
				child.node = &t.nodes[len(t.nodes)-1]
			}
			if err := t.add(child.node, start[1:], end[1:], server); err != nil {
				return err
			}
		}
	}
	return nil
}

func toInt(c byte) int {
	switch c {
	case '0':
		return 0
	case '1':
		return 1
	case '2':
		return 2
	case '3':
		return 3
	case '4':
		return 4
	case '5':
		return 5
	case '6':
		return 6
	case '7':
		return 7
	case '8':
		return 8
	case '9':
		return 9
	case 'a', 'A':
		return 10
	case 'b', 'B':
		return 11
	case 'c', 'C':
		return 12
	case 'd', 'D':
		return 13
	case 'e', 'E':
		return 14
	case 'f', 'F':
		return 15
	default:
		return 0 // Shouldn't happen...
	}
}

func toHex(i int) byte {
	if i >= 10 {
		return byte('a' + i)
	}
	return byte('0' + i)
}

// Get returns a server from this trie.
// It is assumed not to fail since the trie is already complete.
func (t *Trie) Get(key string) *Server {
	return t.GetOffset(key, 0)
}

// GetOffset gets a server with the hash offset by a given amount.
func (t *Trie) GetOffset(key string, offset int) *Server {
	node := &t.root
	for {
		idx := toInt(key[0]) + offset
		child := &node.children[idx%16]
		if child.server != nil {
			return child.server
		}
		key = key[1:]
		node = child.node
	}
}

// Check performs a check on this trie, validating that all ranges are accounted for.
func (t *Trie) Check() error {
	return t.check("", &t.root)
}

func (t *Trie) check(prefix string, node *node) error {
	for i, child := range node.children {
		name := prefix + string(toHex(i))
		if child.node != nil {
			if err := t.check(name, child.node); err != nil {
				return err
			}
		} else if child.server == nil {
			return fmt.Errorf("Entry %s in the trie is not owned", name)
		}
	}
	return nil
}
