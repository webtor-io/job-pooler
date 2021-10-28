package services

import (
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/pkg/errors"

	log "github.com/sirupsen/logrus"
	"github.com/urfave/cli"
	"github.com/webtor-io/lazymap"
)

const (
	jobConcurrencyFlag        = "concurrency"
	jobExpireFlag             = "expire"
	jobErrorExpireFlag        = "error-expire"
	jobCapacityFlag           = "capacity"
	jobTimeoutFlag            = "timeout"
	jobDoneCheckIntervalFlag  = "done-check-interval"
	jobErrorCheckIntervalFlag = "error-check-interval"
	jobDoneMarkerPathFlag     = "done-marker-path"
	jobErrorMarkerPathFlag    = "error-marker-path"
	jobErrorLogPathFlag       = "error-log-path"
	jobInvokePathFlag         = "invoke-path"
)

func RegisterJobFlags(f []cli.Flag) []cli.Flag {
	return append(f,
		cli.IntFlag{
			Name:   jobConcurrencyFlag,
			Usage:  "job concurrency",
			Value:  10,
			EnvVar: "JOB_CONCURRENCY",
		},
		cli.IntFlag{
			Name:   jobExpireFlag,
			Usage:  "job expire (sec)",
			Value:  600,
			EnvVar: "JOB_EXPIRE",
		},
		cli.IntFlag{
			Name:   jobErrorExpireFlag,
			Usage:  "job error expire (sec)",
			Value:  30,
			EnvVar: "JOB_ERROR_EXPIRE",
		},
		cli.IntFlag{
			Name:   jobCapacityFlag,
			Usage:  "job capacity",
			Value:  1000,
			EnvVar: "JOB_CAPACITY",
		},
		cli.IntFlag{
			Name:   jobTimeoutFlag,
			Usage:  "job timeout (sec)",
			Value:  60 * 60 * 6,
			EnvVar: "JOB_TIMEOUT",
		},
		cli.IntFlag{
			Name:   jobDoneCheckIntervalFlag,
			Usage:  "job done check interval (sec)",
			Value:  30,
			EnvVar: "JOB_DONE_CHECK_INTERVAL",
		},
		cli.IntFlag{
			Name:   jobErrorCheckIntervalFlag,
			Usage:  "job error check interval (sec)",
			Value:  30,
			EnvVar: "JOB_ERROR_CHECK_INTERVAL",
		},
		cli.StringFlag{
			Name:   jobDoneMarkerPathFlag,
			Usage:  "job done marker path",
			Value:  "/done",
			EnvVar: "JOB_DONE_MARKER_PATH",
		},
		cli.StringFlag{
			Name:   jobErrorMarkerPathFlag,
			Usage:  "job error marker path",
			Value:  "/error",
			EnvVar: "JOB_ERROR_MARKER_PATH",
		},
		cli.StringFlag{
			Name:   jobErrorLogPathFlag,
			Usage:  "job error log path",
			Value:  "/error.log",
			EnvVar: "JOB_ERROR_LOG_PATH",
		},
		cli.StringFlag{
			Name:   jobInvokePathFlag,
			Usage:  "job invoke path",
			Value:  "/touch",
			EnvVar: "JOB_INVOKE_PATH",
		},
	)
}

type JobMapConfig struct {
	Timeout            time.Duration
	DoneCheckInterval  time.Duration
	ErrorCheckInterval time.Duration
	DoneMarkerPath     string
	ErrorMarkerPath    string
	ErrorLogPath       string
	InvokePath         string
}

type JobMap struct {
	lazymap.LazyMap
	cl  *http.Client
	cfg *JobMapConfig
}

func NewJobMap(c *cli.Context, cl *http.Client) *JobMap {
	return &JobMap{
		cl: cl,
		LazyMap: lazymap.New(&lazymap.Config{
			Concurrency: c.Int(jobConcurrencyFlag),
			Expire:      time.Duration(c.Int(jobExpireFlag)) * time.Second,
			ErrorExpire: time.Duration(c.Int(jobErrorExpireFlag)) * time.Second,
			Capacity:    c.Int(jobConcurrencyFlag),
		}),
		cfg: &JobMapConfig{
			Timeout:            time.Duration(c.Int(jobTimeoutFlag)) * time.Second,
			DoneCheckInterval:  time.Duration(c.Int(jobDoneCheckIntervalFlag)) * time.Second,
			ErrorCheckInterval: time.Duration(c.Int(jobErrorCheckIntervalFlag)) * time.Second,
			DoneMarkerPath:     c.String(jobDoneMarkerPathFlag),
			ErrorMarkerPath:    c.String(jobErrorMarkerPathFlag),
			ErrorLogPath:       c.String(jobErrorLogPathFlag),
			InvokePath:         c.String(jobInvokePathFlag),
		},
	}
}

func (s *JobMap) makeURL(u *url.URL, suffix string) string {
	nu := u.Scheme + "://" + u.Host + u.Path + suffix
	if u.RawQuery != "" {
		nu += "?" + u.RawQuery
	}
	return nu
}

func (s *JobMap) get(u *url.URL) error {
	invokeURL := s.makeURL(u, s.cfg.InvokePath)
	doneURL := s.makeURL(u, s.cfg.DoneMarkerPath)
	errorURL := s.makeURL(u, s.cfg.ErrorMarkerPath)
	errorLogURL := s.makeURL(u, s.cfg.ErrorLogPath)
	log.Infof("invoking job path=%v url=%v", u.Path, invokeURL)
	_, err := s.cl.Get(invokeURL)
	if err != nil {
		return errors.Wrapf(err, "failed to request touch url=%v", invokeURL)
	}
	c := make(chan error)
	if s.cfg.DoneCheckInterval > 0 && doneURL != "" {
		go func() {
			t := time.NewTicker(s.cfg.DoneCheckInterval)
			for {
				<-t.C
				r, err := s.cl.Get(doneURL)
				if err != nil {
					c <- errors.Wrapf(err, "failed to request done marker url=%v", doneURL)
					return
				}
				defer r.Body.Close()
				if r.StatusCode == http.StatusOK {
					c <- nil
					return
				}
			}
		}()
	}
	if s.cfg.ErrorCheckInterval > 0 && errorURL != "" {
		go func() {
			t := time.NewTicker(s.cfg.ErrorCheckInterval)
			for {
				<-t.C
				r, err := s.cl.Get(errorURL)
				if err != nil {
					c <- errors.Wrapf(err, "failed to request error marker url=%v", errorURL)
					return
				}
				defer r.Body.Close()
				if r.StatusCode == http.StatusOK && s.cfg.ErrorLogPath != "" {
					r, err := s.cl.Get(errorLogURL)
					if err != nil {
						c <- errors.Wrapf(err, "failed to request error log url=%v", errorLogURL)
						return
					}
					defer r.Body.Close()
					if r.StatusCode == http.StatusOK {
						e, err := ioutil.ReadAll(r.Body)
						if err != nil {
							c <- errors.Wrapf(err, "failed to read error log url %v", errorLogURL)
							return
						}
						c <- errors.Errorf("job error: %v", strings.TrimSpace(string(e)))
						return
					} else {
						c <- errors.Errorf("job error")
						return
					}
				}
			}
		}()
	}
	select {
	case err := <-c:
		return err
	case <-time.After(s.cfg.Timeout):
		return errors.Errorf("failed to get job status")
	}
}

func (s *JobMap) Touch(u *url.URL) bool {
	return s.LazyMap.Touch(u.Path)
}

func (s *JobMap) Get(u *url.URL) error {
	_, err := s.LazyMap.Get(u.Path, func() (interface{}, error) {
		return nil, s.get(u)
	})
	if err != nil {
		return err
	}
	return nil
}
