package trie

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReplicatedSequentialSuccess(t *testing.T) {
	called := 0
	trie := testTrie(t)
	r := NewReplicator(trie, 2, false)
	// This should succeed on the second call.
	assert.NoError(t, r.Sequential(t.Context(), "0000", func(s *Server) error {
		called++
		if s == trie.Get("0000") {
			return status.Errorf(codes.Unavailable, "Server down")
		}
		return nil
	}))
	assert.Equal(t, 2, called)
}

func TestReplicatedSequentialFailure(t *testing.T) {
	called := 0
	r := NewReplicator(testTrie(t), 2, false)
	// This should fail on all calls.
	assert.Error(t, r.Sequential(t.Context(), "0000", func(s *Server) error {
		called++
		return status.Errorf(codes.Unavailable, "Server down")
	}))
	assert.Equal(t, 2, called)
}

func TestReplicatedSequentialNotRetryable(t *testing.T) {
	called := 0
	r := NewReplicator(testTrie(t), 4, false)
	// This should fail on the first call but not retry.
	assert.Error(t, r.Sequential(t.Context(), "0000", func(s *Server) error {
		called++
		return status.Errorf(codes.InvalidArgument, "Your call is bad and you should feel bad")
	}))
	assert.Equal(t, 1, called)
}

func TestReplicatedParallelSuccess(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(2)
	var called int64
	trie := testTrie(t)
	r := NewReplicator(trie, 2, false)
	// This should succeed on the second server, and hence overall
	assert.NoError(t, r.Parallel(t.Context(), "0000", func(s *Server) error {
		defer wg.Done()
		atomic.AddInt64(&called, 1)
		if s == trie.Get("0000") {
			return status.Errorf(codes.Unavailable, "Server down")
		}
		return nil
	}))
	wg.Wait() // Make sure both have completed before we check
	assert.EqualValues(t, 2, called)
}

func TestReplicatedParallelFailure(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(3)
	var called int64
	r := NewReplicator(testTrie(t), 3, false)
	// This should fail since all writes fail
	assert.Error(t, r.Parallel(t.Context(), "0000", func(s *Server) error {
		defer wg.Done()
		atomic.AddInt64(&called, 1)
		return status.Errorf(codes.Unavailable, "Server down")
	}))
	wg.Wait() // Make sure both have completed before we check
	assert.EqualValues(t, 3, called)
}

func TestSingleServerDown(t *testing.T) {
	trie := testTrie(t)
	r := NewReplicator(trie, 2, false)

	// Fail it enough times to mark the first one down.
	for i := 0; i < failureThreshold; i++ {
		assert.NoError(t, r.Sequential(t.Context(), "0000", func(s *Server) error {
			if s == trie.Get("0000") {
				return status.Errorf(codes.Unavailable, "Server down")
			}
			return nil
		}))
	}

	// Now we should still be able to succeed on the replica
	assert.NoError(t, r.Sequential(t.Context(), "0000", func(s *Server) error {
		return nil
	}))
}

func TestServerDeadIsContinuable(t *testing.T) {
	r := NewReplicator(testTrie(t), 3, false)
	assert.True(t, r.isContinuable(serverDead))
}

func testTrie(t *testing.T) *Trie {
	trie := New(callback)
	assert.NoError(t, trie.AddAll(map[string]string{
		"00-3f": "127.0.0.1:443",
		"40-7f": "127.0.0.1:443",
		"80-af": "127.0.0.1:443",
		"b0-ff": "127.0.0.1:443",
	}))
	return trie
}

func TestErrorCode(t *testing.T) {
	for _, testcase := range []struct {
		Output codes.Code
		Inputs []codes.Code
	}{
		{
			Output: codes.Unknown,
			Inputs: []codes.Code{codes.Unknown, codes.Unknown},
		},
		{
			Output: codes.NotFound,
			Inputs: []codes.Code{codes.Unknown, codes.NotFound},
		},
		{
			Output: codes.NotFound,
			Inputs: []codes.Code{codes.Internal, codes.NotFound},
		},
		{
			Output: codes.Internal,
			Inputs: []codes.Code{codes.Unavailable, codes.Internal},
		},
		{
			Output: codes.OutOfRange,
			Inputs: []codes.Code{codes.OutOfRange, codes.Canceled},
		},
		{
			Output: codes.OK,
			Inputs: nil,
		},
		{
			Output: codes.Unknown,
			Inputs: []codes.Code{codes.Unknown},
		},
	} {
		tc := testcase
		t.Run(fmt.Sprintf("%s_%s", tc.Output, tc.Inputs), func(t *testing.T) {
			var merr *multierror.Error
			for _, input := range tc.Inputs {
				merr = multierror.Append(merr, status.Errorf(input, "%s", input))
			}
			assert.Equal(t, tc.Output, errorCode(merr))
		})
	}
}

func TestRandomisedLoadBalancing(t *testing.T) {
	for _, testcase := range []struct {
		Output []string
		Seed   int64
	}{
		{
			Seed:   42,
			Output: []string{"40", "80", "b0", "00"},
		}, {
			Seed:   3,
			Output: []string{"00", "40", "80", "b0"},
		},
		{
			Seed:   51,
			Output: []string{"80", "b0", "00", "40"},
		},
		{
			Seed:   54,
			Output: []string{"b0", "00", "40", "80"},
		},
	} {
		tc := testcase
		t.Run(fmt.Sprintf("Seed: %d", tc.Seed), func(t *testing.T) {
			intN = rand.New(rand.NewSource(tc.Seed)).Intn
			r := NewReplicator(testTrie(t), 4, true)

			var all []string
			assert.NoError(t, r.SequentialAck(t.Context(), "0000", func(s *Server) (bool, error) {
				all = append(all, s.Start)
				return true, nil
			}))
			assert.Equal(t, tc.Output, all)
		})
	}
}
func TestRandomisedSequentialLoadBalancing(t *testing.T) {
	for _, testcase := range []struct {
		Output []string
		Seed   int64
	}{
		{
			Seed:   42,
			Output: []string{"40"},
		}, {
			Seed:   3,
			Output: []string{"00"},
		},
		{
			Seed:   51,
			Output: []string{"80"},
		},
		{
			Seed:   54,
			Output: []string{"b0"},
		},
	} {
		tc := testcase
		t.Run(fmt.Sprintf("Seed: %d", tc.Seed), func(t *testing.T) {
			intN = rand.New(rand.NewSource(tc.Seed)).Intn
			r := NewReplicator(testTrie(t), 4, true)

			var all []string
			assert.NoError(t, r.Sequential(t.Context(), "0000", func(s *Server) error {
				all = append(all, s.Start)
				return nil
			}))
			assert.Equal(t, tc.Output, all)
		})
	}
}
