package services

import (
	"fmt"
	"net"
	"net/http"
	"net/url"

	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
)

const (
	webHostFlag = "host"
	webPortFlag = "port"
)

type Web struct {
	host string
	port int
	jm   *JobMap
	ln   net.Listener
}

func NewWeb(c *cli.Context, jm *JobMap) *Web {
	return &Web{
		host: c.String(webHostFlag),
		port: c.Int(webPortFlag),
		jm:   jm,
	}
}

func RegisterWebFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.StringFlag{
			Name:   webHostFlag,
			Usage:  "listening host",
			Value:  "",
			EnvVar: "WEB_HOST",
		},
		cli.IntFlag{
			Name:   webPortFlag,
			Usage:  "http listening port",
			Value:  8080,
			EnvVar: "WEB_PORT",
		},
	)
}

func (s *Web) getSourceURL(r *http.Request) string {
	return r.Header.Get("X-Source-Url")
}

func (s *Web) process(w http.ResponseWriter, r *http.Request) error {
	su := s.getSourceURL(r)
	u, err := url.Parse(su)
	if err != nil {
		return errors.Wrapf(err, "failed to parse source url %v", u)
	}
	if s.jm.Has(u) {
		w.WriteHeader(http.StatusOK)
		return nil
	}
	go func() {
		log.Infof("adding job path=%v", u.Path)
		err := s.jm.Get(u)
		if err != nil {
			log.WithError(err).Errorf("got job error path=%v", u.Path)
		} else {
			log.Infof("job finished path=%v", u.Path)
		}
	}()
	w.WriteHeader(http.StatusCreated)
	return nil
}

func (s *Web) Serve() error {
	addr := fmt.Sprintf("%s:%d", s.host, s.port)
	ln, err := net.Listen("tcp", addr)
	s.ln = ln
	if err != nil {
		return errors.Wrap(err, "failed to web listen to tcp connection")
	}
	m := http.NewServeMux()
	m.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		err := s.process(w, r)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
		}
	})
	log.Infof("serving Web at %v", addr)
	srv := &http.Server{
		Handler:        m,
		MaxHeaderBytes: 50 << 20,
	}
	return srv.Serve(ln)
}

func (s *Web) Close() {
	log.Info("closing Web")
	defer func() {
		log.Info("web closed")
	}()
	if s.ln != nil {
		s.ln.Close()
	}
}
