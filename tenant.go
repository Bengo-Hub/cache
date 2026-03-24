package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// TenantDetails holds the full tenant data from auth-api, cached in Redis.
// This is the single source of truth for tenant branding, contact info, and config.
// Services read from this cache instead of storing tenant data in their own DB.
type TenantDetails struct {
	ID                 string         `json:"id"`
	Name               string         `json:"name"`
	Slug               string         `json:"slug"`
	Status             string         `json:"status"`
	ContactEmail       string         `json:"contact_email,omitempty"`
	ContactPhone       string         `json:"contact_phone,omitempty"`
	LogoURL            string         `json:"logo_url,omitempty"`
	Website            string         `json:"website,omitempty"`
	Country            string         `json:"country,omitempty"`
	Timezone           string         `json:"timezone,omitempty"`
	BrandColors        map[string]any `json:"brand_colors,omitempty"`
	OrgSize            string         `json:"org_size,omitempty"`
	UseCase            string         `json:"use_case,omitempty"`
	UseCases           []string       `json:"use_cases,omitempty"`
	SubscriptionPlan   string         `json:"subscription_plan,omitempty"`
	SubscriptionStatus string         `json:"subscription_status,omitempty"`
	TierLimits         map[string]any `json:"tier_limits,omitempty"`
	Metadata           map[string]any `json:"metadata,omitempty"`
}

// DefaultTenantTTL is the default cache TTL for tenant data, aligned with JWT token lifetime.
// Override with the actual JWT expiry when available.
const DefaultTenantTTL = 6 * time.Hour

// TenantCacheKey returns the Redis key for a tenant's cached details.
func TenantCacheKey(slug string) string {
	return "tenant:" + slug
}

// GetTenantDetails returns cached tenant details for the given slug.
// On cache miss, fetches from auth-api and caches with the given TTL.
// authBaseURL is the auth-api base URL (e.g. "https://sso.codevertexitsolutions.com").
func GetTenantDetails(ctx context.Context, c *Aside, authBaseURL string, slug string, ttl time.Duration) (TenantDetails, error) {
	if ttl == 0 {
		ttl = DefaultTenantTTL
	}
	return GetOrSet(ctx, c, TenantCacheKey(slug), ttl, func(ctx context.Context) (TenantDetails, error) {
		return fetchTenantFromAuthAPI(ctx, authBaseURL, slug)
	})
}

// GetTenantBranding returns just the branding fields from cached tenant details.
func GetTenantBranding(details TenantDetails) TenantBranding {
	b := TenantBranding{
		Name:    details.Name,
		LogoURL: details.LogoURL,
		Email:   details.ContactEmail,
		Phone:   details.ContactPhone,
	}
	if details.BrandColors != nil {
		if v, ok := details.BrandColors["primary"].(string); ok {
			b.PrimaryColor = v
		}
		if v, ok := details.BrandColors["secondary"].(string); ok {
			b.SecondaryColor = v
		}
		if v, ok := details.BrandColors["accent"].(string); ok {
			b.AccentColor = v
		}
	}
	return b
}

// TenantBranding is the subset of tenant details used for UI theming.
type TenantBranding struct {
	Name           string `json:"name"`
	LogoURL        string `json:"logo_url,omitempty"`
	PrimaryColor   string `json:"primary_color,omitempty"`
	SecondaryColor string `json:"secondary_color,omitempty"`
	AccentColor    string `json:"accent_color,omitempty"`
	Email          string `json:"email,omitempty"`
	Phone          string `json:"phone,omitempty"`
}

// fetchTenantFromAuthAPI fetches tenant details from auth-api public endpoint.
func fetchTenantFromAuthAPI(ctx context.Context, authBaseURL, slug string) (TenantDetails, error) {
	endpoint := strings.TrimRight(authBaseURL, "/") + "/api/v1/tenants/by-slug/" + slug

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return TenantDetails{}, fmt.Errorf("cache.fetchTenant: create request: %w", err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return TenantDetails{}, fmt.Errorf("cache.fetchTenant: GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return TenantDetails{}, fmt.Errorf("cache.fetchTenant: tenant %q not found (404)", slug)
	}
	if resp.StatusCode != http.StatusOK {
		return TenantDetails{}, fmt.Errorf("cache.fetchTenant: auth-api HTTP %d for %q", resp.StatusCode, slug)
	}

	var details TenantDetails
	if err := json.NewDecoder(resp.Body).Decode(&details); err != nil {
		return TenantDetails{}, fmt.Errorf("cache.fetchTenant: decode: %w", err)
	}
	return details, nil
}
