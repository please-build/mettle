package rpc

import (
	"context"
	"net/http"

	"google.golang.org/api/idtoken"
)

const headerName = "x-goog-iap-jwt-assertion"

// newValidator returns a new HTTP handler that validates incoming requests
func newValidator(ctx context.Context, next http.Handler, audience string, allowedUsers []string) http.Handler {
	val, err := idtoken.NewValidator(ctx)
	if err != nil {
		log.Fatalf("Failed to create new validator: %s", err)
	}
	v := &validator{
		validator:    val,
		next:         next,
		audience:     audience,
		allowedUsers: make(map[string]struct{}, len(allowedUsers)),
	}
	for _, user := range allowedUsers {
		v.allowedUsers[user] = struct{}{}
	}
	return v
}

// maybeAddValidation swaddles an HTTP handler in authenticating middleware.
// If the set of allowed users is empty, it has no effect.
func maybeAddValidation(ctx context.Context, next http.Handler, audience string, allowedUsers []string) http.Handler {
	if len(allowedUsers) == 0 {
		return next
	}
	return newValidator(ctx, next, audience, allowedUsers)
}

// A validator validates that incoming requests are signed with valid Cloud IAP tokens.
// See https://cloud.google.com/iap for more general information about what it is.
type validator struct {
	validator    *idtoken.Validator
	next         http.Handler
	audience     string
	allowedUsers map[string]struct{}
}

func (v *validator) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet { // Non-mutating methods are fine for anyone
		v.next.ServeHTTP(w, r)
	} else if hdr := r.Header.Get(headerName); hdr == "" {
		log.Warning("Rejecting request with missing %s header", headerName)
		w.WriteHeader(http.StatusUnauthorized)
	} else if payload, err := v.validator.Validate(r.Context(), hdr, v.audience); err != nil {
		log.Warning("Rejecting unauthorized request: %s", err)
		w.WriteHeader(http.StatusUnauthorized)
	} else if email, present := payload.Claims["email"]; !present {
		log.Warning("Rejecting request, missing email claim")
		w.WriteHeader(http.StatusForbidden)
	} else if _, present := v.allowedUsers[email.(string)]; !present {
		log.Warning("Rejecting request from %s; not in allowed user list", payload.Subject)
		w.WriteHeader(http.StatusForbidden)
	} else {
		v.next.ServeHTTP(w, r)
	}
}
