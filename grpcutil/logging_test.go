package grpcutil

import (
	"fmt"
	"testing"

	"github.com/hashicorp/go-multierror"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHasGRPCCode_DirectStatus(t *testing.T) {
	err := status.Errorf(codes.NotFound, "blob not found")
	assert.True(t, hasGRPCCode(err, codes.NotFound))
	assert.False(t, hasGRPCCode(err, codes.Internal))
}

func TestHasGRPCCode_MultierrorWrapped(t *testing.T) {
	// With multiple errors, multierror wraps them in a way that loses the
	// gRPC status code from status.Code's perspective.
	inner1 := status.Errorf(codes.NotFound, "blob abc123 not found")
	inner2 := status.Errorf(codes.NotFound, "blob def456 not found")
	var multi error
	multi = multierror.Append(multi, inner1, inner2)
	// status.Code sees NotFound because multierror implements Unwrap
	// but the important thing is hasGRPCCode finds it either way
	assert.True(t, hasGRPCCode(multi, codes.NotFound))
	assert.False(t, hasGRPCCode(multi, codes.Internal))
}

func TestHasGRPCCode_MultierrorGroup(t *testing.T) {
	// This mirrors how elan's getTree uses multierror.Group
	var g multierror.Group
	g.Go(func() error {
		return status.Errorf(codes.NotFound, "blob def456 not found")
	})
	err := g.Wait().ErrorOrNil()
	assert.True(t, hasGRPCCode(err, codes.NotFound))
}

func TestHasGRPCCode_NonGRPCError(t *testing.T) {
	err := fmt.Errorf("some random error")
	assert.False(t, hasGRPCCode(err, codes.NotFound))
}

func TestHasGRPCCode_Nil(t *testing.T) {
	assert.False(t, hasGRPCCode(nil, codes.NotFound))
}
