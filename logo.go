package cache

import (
	"bytes"
	"encoding/base64"
	"io"
	"net/http"
	"strings"
	"time"
)

// maxLogoBytes caps how much of a logo is read from a URL — protects a document render against a
// huge or hostile image tying up memory/bandwidth.
const maxLogoBytes = 5 << 20

// logoHTTPClient downloads tenant logos with a bounded timeout (a slow logo host must never stall a
// document download) and pooled keep-alive connections (a menu PDF fetches many images). Safe for
// concurrent use.
var logoHTTPClient = &http.Client{
	Timeout: 6 * time.Second,
	Transport: &http.Transport{
		MaxIdleConns:        64,
		MaxIdleConnsPerHost: 32,
		IdleConnTimeout:     30 * time.Second,
		TLSHandshakeTimeout: 3 * time.Second,
	},
}

// FetchTenantLogo resolves a tenant's logo (from cached TenantDetails) to embeddable image bytes and
// the fpdf image type. Convenience wrapper over FetchLogo for the common call site.
func FetchTenantLogo(td TenantDetails) ([]byte, string) {
	return FetchLogo(td.LogoURL)
}

// FetchLogo resolves a tenant logo URL to its raw bytes and the fpdf image type ("PNG"/"JPG"/"GIF"),
// ready to embed with fpdf.RegisterImageOptionsReader. It is the single, shared resolver used by
// every service's document/report engine so the behaviour can never drift between them.
//
// It handles BOTH shapes a tenant logo now arrives in:
//   - a hosted URL (http/https) — downloaded with a bounded timeout and size cap; and
//   - an inline RFC-2397 data: URI ("data:image/png;base64,…") — base64-decoded in-process.
//     http.Get cannot fetch a data: URI (it errors with an unsupported scheme), which is why logos
//     saved inline as data URIs silently vanished from generated PDFs.
//
// The image type is ALWAYS sniffed from the real bytes, never the HTTP Content-Type or the data-URI
// media type: tenant logos are routinely mislabeled (a JPEG served/tagged as image/png), and fpdf
// rejects a declared-type/actual-bytes mismatch with "not a PNG buffer", poisoning the whole PDF.
//
// Returns (nil, "") on any failure — blank URL, network error, non-200, or a format fpdf cannot
// embed (SVG/WebP) — so callers simply render without a logo (graceful degradation).
func FetchLogo(logoURL string) ([]byte, string) {
	logoURL = strings.TrimSpace(logoURL)
	if logoURL == "" {
		return nil, ""
	}

	var data []byte
	if strings.HasPrefix(logoURL, "data:") {
		data = decodeDataURI(logoURL)
	} else {
		resp, err := logoHTTPClient.Get(logoURL) //nolint:noctx // URL from the trusted auth-api tenant cache
		if err != nil {
			return nil, ""
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, ""
		}
		var buf bytes.Buffer
		if _, err := buf.ReadFrom(&io.LimitedReader{R: resp.Body, N: maxLogoBytes}); err != nil {
			return nil, ""
		}
		data = buf.Bytes()
	}
	if len(data) == 0 {
		return nil, ""
	}
	typ := SniffImageType(data)
	if typ == "" {
		return nil, ""
	}
	return data, typ
}

// decodeDataURI decodes a base64 RFC-2397 data: URI ("data:[<mediatype>];base64,<b64>") into its raw
// bytes, tolerating stray whitespace/newlines in the payload (uploaders sometimes wrap the base64).
// Returns nil for a non-base64 or malformed data URI — image logos are always base64-encoded.
func decodeDataURI(u string) []byte {
	comma := strings.IndexByte(u, ',')
	if comma < 0 || !strings.Contains(u[:comma], ";base64") {
		return nil
	}
	payload := strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' || r == ' ' || r == '\t' {
			return -1
		}
		return r
	}, u[comma+1:])
	raw, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		return nil
	}
	return raw
}

// SniffImageType reports the fpdf image-type string ("PNG"/"JPG"/"GIF") for raw image bytes by
// inspecting the magic number, independent of any declared Content-Type / media type. Returns "" for
// formats fpdf cannot embed (e.g. SVG/WebP) so the caller renders without a logo rather than feeding
// fpdf a mislabeled buffer that poisons the whole document.
func SniffImageType(b []byte) string {
	switch {
	case len(b) >= 3 && b[0] == 0xFF && b[1] == 0xD8 && b[2] == 0xFF:
		return "JPG"
	case len(b) >= 8 && b[0] == 0x89 && b[1] == 'P' && b[2] == 'N' && b[3] == 'G' &&
		b[4] == '\r' && b[5] == '\n' && b[6] == 0x1A && b[7] == '\n':
		return "PNG"
	case len(b) >= 6 && b[0] == 'G' && b[1] == 'I' && b[2] == 'F' && b[3] == '8':
		return "GIF"
	default:
		return ""
	}
}
