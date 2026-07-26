package cache

import (
	"bytes"
	"encoding/base64"
	"image"
	"image/jpeg"
	"image/png"
	"net/http"
	"net/http/httptest"
	"testing"
)

func tinyPNG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 3, 3))
	var b bytes.Buffer
	if err := png.Encode(&b, img); err != nil {
		t.Fatalf("encode png: %v", err)
	}
	return b.Bytes()
}

func tinyJPEG(t *testing.T) []byte {
	t.Helper()
	img := image.NewRGBA(image.Rect(0, 0, 3, 3))
	var b bytes.Buffer
	if err := jpeg.Encode(&b, img, nil); err != nil {
		t.Fatalf("encode jpeg: %v", err)
	}
	return b.Bytes()
}

// TestFetchLogoDataURI is the regression for tenant logos saved inline as base64 data URIs (how the
// auth-api by-slug endpoint now returns some tenants). http.Get cannot fetch a data: URI, so the old
// code dropped the logo entirely; FetchLogo must decode it in-process.
func TestFetchLogoDataURI(t *testing.T) {
	jpg := tinyJPEG(t)
	// Deliberately MISLABEL the JPEG as image/png in the media type — exactly the production shape
	// (accounts.codevertexafrica.com serves a JPEG under an image/png data URI). The type must
	// still come back as JPG, sniffed from the bytes.
	uri := "data:image/png;base64," + base64.StdEncoding.EncodeToString(jpg)
	data, typ := FetchLogo(uri)
	if typ != "JPG" {
		t.Fatalf("FetchLogo(data URI): want type JPG (sniffed), got %q", typ)
	}
	if !bytes.Equal(data, jpg) {
		t.Fatalf("FetchLogo(data URI): decoded bytes do not match the source JPEG")
	}
}

// TestFetchLogoHTTPSniffsBytes proves a hosted logo whose Content-Type lies (JPEG served as
// image/png) is embedded as JPG, sniffed from the real bytes rather than the header.
func TestFetchLogoHTTPSniffsBytes(t *testing.T) {
	jpg := tinyJPEG(t)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png") // the lie that broke PDFs
		_, _ = w.Write(jpg)
	}))
	defer srv.Close()

	_, typ := FetchLogo(srv.URL)
	if typ != "JPG" {
		t.Fatalf("FetchLogo(http): want JPG (sniffed from bytes), got %q", typ)
	}
}

func TestFetchLogoGraceful(t *testing.T) {
	if data, typ := FetchLogo(""); data != nil || typ != "" {
		t.Fatalf("empty URL: want (nil, \"\"), got (%v, %q)", data, typ)
	}
	if data, typ := FetchLogo("data:image/svg+xml;base64,PHN2Zz48L3N2Zz4="); data != nil || typ != "" {
		t.Fatalf("unsupported format (SVG): want (nil, \"\"), got (%v bytes, %q)", len(data), typ)
	}
}

func TestSniffImageType(t *testing.T) {
	if got := SniffImageType(tinyPNG(t)); got != "PNG" {
		t.Fatalf("PNG sniff: got %q", got)
	}
	if got := SniffImageType(tinyJPEG(t)); got != "JPG" {
		t.Fatalf("JPG sniff: got %q", got)
	}
	if got := SniffImageType([]byte("not an image")); got != "" {
		t.Fatalf("garbage sniff: want \"\", got %q", got)
	}
}
