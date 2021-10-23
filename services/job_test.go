package services

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/webtor-io/lazymap"
)

func TestJobMakeURL(t *testing.T) {
	jm := &JobMap{
		cl:      &http.Client{},
		LazyMap: lazymap.New(&lazymap.Config{}),
		cfg:     &JobMapConfig{},
	}
	u, _ := url.Parse("https://example.org/abra?cadabra=123")
	uu := jm.makeURL(u, "/done")
	if uu != "https://example.org/abra/done?cadabra=123" {
		t.Fatalf("Expected %v got %v", "https://example.org/abra/done?cadabra=123", uu)
	}
	u, _ = url.Parse("https://example.org/abra")
	uu = jm.makeURL(u, "/error")
	if uu != "https://example.org/abra/error" {
		t.Fatalf("Expected %v got %v", "https://example.org/abra/error", uu)
	}
}

func TestJobGetDone(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	jm := &JobMap{
		cl:      &http.Client{},
		LazyMap: lazymap.New(&lazymap.Config{}),
		cfg: &JobMapConfig{
			DoneMarkerPath:     "/done",
			ErrorMarkerPath:    "/error",
			ErrorLogPath:       "/error.log",
			InvokePath:         "/touch",
			DoneCheckInterval:  time.Millisecond,
			ErrorCheckInterval: 10 * time.Millisecond,
			Timeout:            20 * time.Millisecond,
		},
	}
	u, _ := url.Parse(ts.URL)
	err := jm.Get(u)
	if err != nil {
		t.Fatalf("Expected nil got %v", err)
	}
}

func TestJobGetError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, "Test Error")
	}))
	defer ts.Close()
	jm := &JobMap{
		cl:      &http.Client{},
		LazyMap: lazymap.New(&lazymap.Config{}),
		cfg: &JobMapConfig{
			DoneMarkerPath:     "/done",
			ErrorMarkerPath:    "/error",
			ErrorLogPath:       "/error.log",
			InvokePath:         "/touch",
			DoneCheckInterval:  10 * time.Millisecond,
			ErrorCheckInterval: 1 * time.Millisecond,
			Timeout:            20 * time.Millisecond,
		},
	}
	u, _ := url.Parse(ts.URL)
	err := jm.Get(u)
	if fmt.Sprintf("%v", err) != "job error: Test Error" {
		t.Fatalf("Expected \"job error: Test Error\" got \"%v\"", err)
	}
}
