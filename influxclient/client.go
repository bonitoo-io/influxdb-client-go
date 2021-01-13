package influxclient

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
	"time"

	influxdata "github.com/influxdata/influxdb/v2/models"
)

const version = "3.0.0alpha"

var (
	// ErrEmptyServerURL is returned when params do have set ServerURL field
	ErrEmptyServerURL = errors.New("empty server URL")
	// User-Agent HTTP header value
	userAgent = fmt.Sprintf("influxdb-client-go/%s  (%s; %s)", version, runtime.GOOS, runtime.GOARCH)
)

// Params holds the parameters for creating a new client.
// The only mandatory fields are ServerURL and AuthToken.
type Params struct {
	// ServerURL holds the URL of the InfluxDB server to connect to.
	// This must be non-empty. E.g. http://localhost:8086
	ServerURL string

	// AuthToken holds the authorization token for the API.
	// This can be obtained through the GUI web browser interface.
	AuthToken string

	// DefaultTags specifies a set of tags that will be added to each written
	// point. Tags specified on points override these.
	DefaultTags map[string]string

	// HTTPClient is used to make API requests.
	//
	// This can be used to specify a custom TLS configuration
	// (TLSClientConfig), a custom request timeout (Timeout),
	// or other customization as required.
	//
	// It HTTPClient is nil, http.DefaultClient will be used.
	HTTPClient *http.Client

	// BatchSize holds the default batch size
	// used by PointWriter. If it's zero, DefaultBatchSize will
	// be used. Note that this can be overridden with PointWriter.SetBatchSize.
	BatchSize int

	// FlushInterval holds the default flush interval used by PointWriter.
	// If it's zero, points must be flushed manually.
	// Note that this can be overridden with PointWriter.SetFlushInterval.
	FlushInterval time.Duration
}

// Client implements an InfluxDB client.
type Client struct {
	// Configuration params
	params Params
	// Pre-created Authorization HTTP header value
	authorization string
	// pre-created write endpoint URL
	writeURL *url.URL
	// pre-created query endpoint URL
	queryURL *url.URL
}

// NewClient creates new Client with given Params. Its ServerURL
func NewClient(params Params) (*Client, error) {
	c := &Client{params: params}
	if params.ServerURL == "" {
		return nil, ErrEmptyServerURL
	}
	if !strings.HasSuffix(c.params.ServerURL, "/") {
		// For subsequent path parts concatenation, url has to end with '/'
		c.params.ServerURL = c.params.ServerURL + "/"
	}
	if c.params.AuthToken != "" {
		c.authorization = "Token " + c.params.AuthToken
	}
	if c.params.HTTPClient == nil {
		c.params.HTTPClient = http.DefaultClient
	}
	// Prepared basic URLs
	serverURL, err := url.Parse(c.params.ServerURL)
	if err != nil {
		return nil, fmt.Errorf("error parsing server URL: %w", err)
	}
	c.writeURL, err = serverURL.Parse("api/v2/write")
	if err != nil {
		return nil, fmt.Errorf("error parsing server URL: %w", err)
	}
	c.queryURL, err = serverURL.Parse("api/v2/query")
	if err != nil {
		return nil, fmt.Errorf("error parsing server URL: %w", err)
	}
	return c, nil
}

// WritePoints writes all the given points to the server with the
// given organization id into the given bucket.
// The points are written synchronously. For a higher throughput
// API that buffers individual points and writes them asynchronously,
// use the PointWriter method.
func (c *Client) WritePoints(org, bucket string, points []influxdata.Point) error {
	var err error
	var buff strings.Builder
	size := 0
	for _, p := range points {
		size += p.StringSize()
	}
	buff.Grow(size + len(points))
	for _, p := range points {
		_, err = buff.WriteString(p.String())
		if err != nil {
			return fmt.Errorf("error marshaling points: %w", err)
		}
		_, err = buff.WriteRune('\n')
		if err != nil {
			return fmt.Errorf("error marshaling points: %w", err)
		}
	}

	resp, err := c.makeAPIRequest(c.writeURL, org, bucket, strings.NewReader(buff.String()))
	if err != nil {
		return err
	}
	// discard body so connection can be reused
	_, _ = io.Copy(ioutil.Discard, resp.Body)
	_ = resp.Body.Close()
	return nil
}

// makeAPIRequest issues a POST request and handle HTTP errors
func (c *Client) makeAPIRequest(endpoint *url.URL, org, bucket string, body io.Reader) (*http.Response, error) {
	params := make(url.Values)

	params.Set("org", org)
	if bucket != "" {
		params.Set("bucket", bucket)
		params.Set("precision", "ns")
	}
	// copy URL
	urlObj := *endpoint
	urlObj.RawQuery = params.Encode()

	fullURL := urlObj.String()

	req, err := http.NewRequest("POST", fullURL, body)
	if err != nil {
		return nil, fmt.Errorf("error calling %s: %w", fullURL, err)
	}
	req.Header.Set("User-Agent", userAgent)
	if c.authorization != "" {
		req.Header.Add("Authorization", c.authorization)
	}

	resp, err := c.params.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error calling %s: %w", fullURL, err)
	}
	if resp.StatusCode < 200 && resp.StatusCode > 299 {
		return nil, c.handleHTTPError(resp)
	}

	return resp, nil
}

func (c *Client) handleHTTPError(r *http.Response) error {
	// successful status code range
	if r.StatusCode >= 200 && r.StatusCode < 300 {
		return nil
	}
	defer func() {
		// discard body so connection can be reused
		_, _ = io.Copy(ioutil.Discard, r.Body)
		_ = r.Body.Close()
	}()

	httpError := struct {
		Code       string
		Message    string
		RetryAfter uint
	}{}

	if v := r.Header.Get("Retry-After"); v != "" {
		r, err := strconv.ParseUint(v, 10, 32)
		if err == nil {
			httpError.RetryAfter = uint(r)
		}
	}
	// Default code
	httpError.Code = r.Status
	// json encoded error
	ctype, _, _ := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if ctype == "application/json" {
		err := json.NewDecoder(r.Body).Decode(&httpError)
		if err != nil {
			httpError.Message = err.Error()
		}
	} else {
		body, err := ioutil.ReadAll(r.Body)
		if err != nil {
			httpError.Message = err.Error()
		} else {
			httpError.Message = string(body)
		}
	}

	if httpError.Message == "" {
		switch r.StatusCode {
		case http.StatusTooManyRequests:
			httpError.Code = "too many requests"
			httpError.Message = "exceeded rate limit"
		case http.StatusServiceUnavailable:
			httpError.Code = "unavailable"
			httpError.Message = "service temporarily unavailable"
		default:
			//
			httpError.Message = r.Header.Get("X-Influxdb-Error")
		}
	}
	return fmt.Errorf("%s: %s", httpError.Code, httpError.Message)
}
