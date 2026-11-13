package cache

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestResolveSupportContact(t *testing.T) {
	support := func(m map[string]any) map[string]any { return map[string]any{"support": m} }
	cases := []struct {
		name string
		in   TenantDetails
		want SupportContact
	}{
		{
			name: "support contact set",
			in: TenantDetails{
				ContactEmail: "info@shop.ke", ContactPhone: "+254700000001",
				Metadata: support(map[string]any{"email": "help@shop.ke", "phone": "+254700000002", "whatsapp": "+254700000003"}),
			},
			want: SupportContact{Email: "help@shop.ke", Phone: "+254700000002", WhatsApp: "+254700000003", EmailFromSupport: true, PhoneFromSupport: true},
		},
		{
			name: "no support contact falls back to contact email and phone",
			in:   TenantDetails{ContactEmail: "info@shop.ke", ContactPhone: "+254700000001"},
			want: SupportContact{Email: "info@shop.ke", Phone: "+254700000001", WhatsApp: "+254700000001"},
		},
		{
			name: "support email only keeps the contact phone",
			in: TenantDetails{
				ContactEmail: "info@shop.ke", ContactPhone: "+254700000001",
				Metadata: support(map[string]any{"email": "help@shop.ke"}),
			},
			want: SupportContact{Email: "help@shop.ke", Phone: "+254700000001", WhatsApp: "+254700000001", EmailFromSupport: true},
		},
		{
			name: "whatsapp only serves as the phone too",
			in: TenantDetails{
				ContactPhone: "+254700000001",
				Metadata:     support(map[string]any{"whatsapp": "+254700000003"}),
			},
			want: SupportContact{Phone: "+254700000003", WhatsApp: "+254700000003", PhoneFromSupport: true},
		},
		{
			name: "support phone only serves as whatsapp too",
			in:   TenantDetails{Metadata: support(map[string]any{"phone": "+254700000002"})},
			want: SupportContact{Phone: "+254700000002", WhatsApp: "+254700000002", PhoneFromSupport: true},
		},
		{
			name: "malformed support value is ignored",
			in:   TenantDetails{ContactEmail: "info@shop.ke", Metadata: map[string]any{"support": "help@shop.ke"}},
			want: SupportContact{Email: "info@shop.ke"},
		},
		{
			name: "nothing at all",
			in:   TenantDetails{},
			want: SupportContact{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := ResolveSupportContact(tc.in); got != tc.want {
				t.Errorf("ResolveSupportContact() = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestFetchTenantFillsSupport(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id": "t1", "slug": "codevertex", "contact_email": "info@codevertexafrica.com",
			"metadata": map[string]any{"support": map[string]any{"email": "support@codevertexafrica.com"}},
		})
	}))
	defer srv.Close()

	d, err := fetchTenantFromAuthAPI(context.Background(), srv.URL, "codevertex")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if d.Support.Email != "support@codevertexafrica.com" {
		t.Fatalf("Support.Email = %q, want support@codevertexafrica.com", d.Support.Email)
	}
	if got := ResolveSupportContact(d).Email; got != "support@codevertexafrica.com" {
		t.Fatalf("resolved email = %q", got)
	}
}

func TestWithSupportReadsOldCacheEntries(t *testing.T) {
	// An entry cached before the Support field existed carries only the metadata.
	raw := `{"id":"t1","slug":"shop","metadata":{"support":{"phone":"+254700000002"}}}`
	var d TenantDetails
	if err := json.Unmarshal([]byte(raw), &d); err != nil {
		t.Fatal(err)
	}
	if got := withSupport(d).Support.Phone; got != "+254700000002" {
		t.Fatalf("Support.Phone = %q", got)
	}
}
