package cache

import "strings"

// TenantSupport is the tenant's support contact, kept by auth-api in tenant metadata under
// "support" ({email, phone, whatsapp}). Any field may be empty: a tenant sets only what it has.
// auth-api validates the email and normalises both numbers to E.164 when they are saved.
type TenantSupport struct {
	Email    string `json:"email,omitempty"`
	Phone    string `json:"phone,omitempty"`
	WhatsApp string `json:"whatsapp,omitempty"`
}

// IsZero reports whether no support contact is set.
func (s TenantSupport) IsZero() bool {
	return s.Email == "" && s.Phone == "" && s.WhatsApp == ""
}

// SupportContact is where a tenant's customers are answered from, with the fallback applied:
// the support contact when set, else the tenant's contact email and phone.
type SupportContact struct {
	Email    string `json:"email,omitempty"`
	Phone    string `json:"phone,omitempty"`
	WhatsApp string `json:"whatsapp,omitempty"`
	// EmailFromSupport and PhoneFromSupport say whether the value came from the support contact
	// (true) or from the contact email or phone fallback (false).
	EmailFromSupport bool `json:"email_from_support"`
	PhoneFromSupport bool `json:"phone_from_support"`
}

// MetadataKeySupport is the tenant metadata key holding the support contact.
const MetadataKeySupport = "support"

// TenantSupportFromMetadata reads the support contact from tenant metadata. A missing or
// malformed value gives an empty TenantSupport.
func TenantSupportFromMetadata(meta map[string]any) TenantSupport {
	raw, ok := meta[MetadataKeySupport].(map[string]any)
	if !ok {
		return TenantSupport{}
	}
	text := func(k string) string {
		s, _ := raw[k].(string)
		return strings.TrimSpace(s)
	}
	return TenantSupport{Email: text("email"), Phone: text("phone"), WhatsApp: text("whatsapp")}
}

// withSupport fills d.Support from its metadata when it is not already set. Entries cached
// before the field existed carry the metadata only, so this runs on every read.
func withSupport(d TenantDetails) TenantDetails {
	if d.Support.IsZero() {
		d.Support = TenantSupportFromMetadata(d.Metadata)
	}
	return d
}

// ResolveSupportContact applies the support routing fallback to a tenant:
//   - email: the support email, else the contact email;
//   - phone (SMS and calls): the support phone, else the support WhatsApp number, else the
//     contact phone;
//   - whatsapp: the support WhatsApp number, else the support phone, else the contact phone.
//
// Every service that tells a customer how to reach a tenant (ticket mail, Reply-To, footers,
// payment pages) uses this one helper so the rule stays the same everywhere.
func ResolveSupportContact(d TenantDetails) SupportContact {
	s := withSupport(d).Support
	out := SupportContact{}
	if s.Email != "" {
		out.Email, out.EmailFromSupport = s.Email, true
	} else {
		out.Email = strings.TrimSpace(d.ContactEmail)
	}
	contactPhone := strings.TrimSpace(d.ContactPhone)
	switch {
	case s.Phone != "":
		out.Phone, out.PhoneFromSupport = s.Phone, true
	case s.WhatsApp != "":
		out.Phone, out.PhoneFromSupport = s.WhatsApp, true
	default:
		out.Phone = contactPhone
	}
	switch {
	case s.WhatsApp != "":
		out.WhatsApp = s.WhatsApp
	case s.Phone != "":
		out.WhatsApp = s.Phone
	default:
		out.WhatsApp = contactPhone
	}
	return out
}
