package services

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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

func TestJobTouch(t *testing.T) {
	done := false
	go func() {
		<-time.After(1 * time.Second)
		done = true
	}()
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/touch") {
			w.WriteHeader(http.StatusOK)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/error") {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if strings.HasSuffix(r.URL.Path, "/done") {
			if done {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
			return
		}
	}))
	defer ts.Close()
	jm := &JobMap{
		cl: &http.Client{},
		LazyMap: lazymap.New(&lazymap.Config{
			Concurrency: 1,
		}),
		cfg: &JobMapConfig{
			DoneMarkerPath:     "/done",
			ErrorMarkerPath:    "/error",
			ErrorLogPath:       "/error.log",
			InvokePath:         "/touch",
			DoneCheckInterval:  time.Millisecond,
			ErrorCheckInterval: 10 * time.Millisecond,
			Timeout:            10 * time.Second,
		},
	}
	u, _ := url.Parse(ts.URL + "/1")
	go func() {
		jm.Get(u)
	}()
	<-time.After(20 * time.Millisecond)
	st, ok := jm.Status(u)
	if ok != true {
		t.Fatalf("Expected true got %v", ok)
	}
	if st != lazymap.Running {
		t.Fatalf("Expected Running got %v", st)
	}
	to := jm.Touch(u)
	if to != true {
		t.Fatalf("Expected true got %v", to)
	}

	u2, _ := url.Parse(ts.URL + "/2")
	go func() {
		jm.Get(u2)
	}()
	<-time.After(20 * time.Millisecond)

	st, ok = jm.Status(u2)
	if ok != true {
		t.Fatalf("Expected true got %v", ok)
	}
	if st != lazymap.Enqueued {
		t.Fatalf("Expected Running got %v", st)
	}
	to = jm.Touch(u2)
	if to != true {
		t.Fatalf("Expected true got %v", to)
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
