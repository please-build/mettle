package redis

import (
	"context"
	"errors"
	"testing"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestLimiterAllow(t *testing.T) {
	tests := map[string]struct {
		l *Limiter

		expectedError bool
	}{
		"returns true if rate limiter has some tokens left": {
			l:             &Limiter{limiter: rate.NewLimiter(0.1, 1)},
			expectedError: false,
		},
		"returns false if rate limiter has no token left": {
			l:             &Limiter{limiter: &rate.Limiter{}},
			expectedError: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := test.l.Allow()
			if test.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestLimiterReportResult(t *testing.T) {
	tests := map[string]struct {
		result error

		allowMore bool
	}{
		"allows nil errors": {
			allowMore: true,
		},
		"allows redis.Nil errors": {
			result:    redis.Nil,
			allowMore: true,
		},
		"allows context canceled errors": {
			result:    context.Canceled,
			allowMore: true,
		},
		"allows rate limit errors": {
			result:    ErrRateLimitReached,
			allowMore: true,
		},
		"other errors are not allowed and count towards rate limit": {
			result:    errors.New("random infra error"),
			allowMore: false,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// Allows one error every 10 seconds
			l := &Limiter{limiter: rate.NewLimiter(0.1, 1)}
			require.NoError(t, l.Allow())
			l.ReportResult(test.result)
			if test.allowMore {
				assert.NoError(t, l.Allow())
			} else {
				assert.Error(t, l.Allow())
			}
		})
	}
}
