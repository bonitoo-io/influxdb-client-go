package influxclient

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"strconv"
)

// Dialect defines model for Dialect.
type Dialect struct {
	Annotations []string `json:"annotations"`
	Delimiter   string   `json:"delimiter"`
	Header      bool     `json:"header"`
}

// Query defines model for Query.
type Query struct {
	Dialect Dialect `json:"dialect"`
	Query   string  `json:"query"`
	Type    string  `json:"type"`
}

var defaultDialect = Dialect{
	Annotations: []string{"datatype", "default", "group"},
	Delimiter:   ",",
	Header:      true,
}

func (c *Client) Query(ctx context.Context, org, query string) (*QueryResults, error) {
	queryURL, _ := c.apiURL.Parse("query")
	params := queryURL.Query()
	params.Set("org", org)
	queryURL.RawQuery = params.Encode()
	q := Query{Dialect: defaultDialect, Query: query, Type: "flux"}
	qrJSON, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}
	resp, err := c.makeAPICallWithParams(ctx, http.MethodPost, queryURL, map[string]string{"org": org}, bytes.NewReader(qrJSON))
	if err != nil {
		return nil, err
	}

	if resp.Header.Get("Content-Encoding") == "gzip" {
		resp.Body, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
	}
	return NewQueryResults(resp.Body), nil
}

// makeAPICallWithParams issues an HTTP request to InfluxDB server API url and return response.
// It constructs full url from endpoint and queryParams
func (c *Client) makeAPICallWithParams(ctx context.Context, httpMethod string, endpointURL *url.URL, queryParams map[string]string, body io.Reader) (*http.Response, error) {
	urlParams := make(url.Values)

	for k, v := range queryParams {
		urlParams.Set(k, v)
	}
	// copy URL
	urlObj := *endpointURL
	urlObj.RawQuery = urlParams.Encode()

	fullURL := urlObj.String()

	return c.MakeAPICall(ctx, httpMethod, fullURL, body)
}

// MakeAPICall issues an HTTP request to InfluxDB server API url and return response.
// HTTP errors are handled and returned as an error. HttpMethod is an HTTP verb, e.g. POST, GET.
// Body can be nil.
func (c *Client) MakeAPICall(ctx context.Context, httpMethod string, url string, body io.Reader) (*http.Response, error) {

	req, err := http.NewRequestWithContext(ctx, httpMethod, url, body)
	if err != nil {
		return nil, fmt.Errorf("error calling %s: %w", url, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "gzip")
	//req.Header.Set("User-Agent", userAgent)
	if c.authorization != "" {
		req.Header.Add("Authorization", c.authorization)
	}

	resp, err := c.params.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("error calling %s: %w", url, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, c.resolveHTTPError(resp)
	}

	return resp, nil
}

// resolveHTTPError parses server error response and returns error with human readable message
func (c *Client) resolveHTTPError(r *http.Response) error {
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
