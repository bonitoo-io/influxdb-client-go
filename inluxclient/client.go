// Copyright 2021 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

// Package influxclient provides client for InfluxDB server.
package influxclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Params holds the parameters for creating a new client.
// The only mandatory field is ServerURL. AuthToken is also important
// if authentication was not done outside this client.
type Params struct {
	// ServerURL holds the URL of the InfluxDB server to connect to.
	// This must be non-empty. E.g. http://localhost:8086
	ServerURL string

	// AuthToken holds the authorization token for the API.
	// This can be obtained through the GUI web browser interface.
	AuthToken string

	// HTTPClient is used to make API requests.
	//
	// This can be used to specify a custom TLS configuration
	// (TLSClientConfig), a custom request timeout (Timeout),
	// or other customization as required.
	//
	// It HTTPClient is nil, http.DefaultClient will be used.
	HTTPClient *http.Client
}

// Client implements an InfluxDB client.
type Client struct {
	// Configuration params.
	params Params
	// Pre-created Authorization HTTP header value.
	authorization string
	// Cached base server API URL.
	apiURL *url.URL
}

// httpParams holds parameters for creating an HTTP request
type httpParams struct {
	// URL of server endpoint
	endpointURL *url.URL
	// Params to be added to URL
	queryParams map[string]string
	// HTTP request method, eg. POST
	httpMethod string
	// HTTP request headers
	headers map[string]string
	// HTTP POST/PUT body
	body io.Reader
}

// New creates new Client with given Params, where ServerURL and AuthToken are mandatory.
func New(params Params) (*Client, error) {
	c := &Client{params: params}
	if params.ServerURL == "" {
		return nil, errors.New("empty server URL")
	}
	if c.params.AuthToken != "" {
		c.authorization = "Token " + c.params.AuthToken
	}
	if c.params.HTTPClient == nil {
		c.params.HTTPClient = http.DefaultClient
	}

	serverAddress := params.ServerURL
	if !strings.HasSuffix(serverAddress, "/") {
		// For subsequent path parts concatenation, url has to end with '/'
		serverAddress = params.ServerURL + "/"
	}
	var err error
	// Prepare server API URL
	c.apiURL, err = url.Parse(serverAddress + "api/v2/")
	if err != nil {
		return nil, fmt.Errorf("error parsing server URL: %w", err)
	}
	return c, nil
}

// HealthCheck defines model for HealthCheck.
type HealthCheck struct {
	Checks  *[]HealthCheck `json:"checks,omitempty"`
	Commit  *string        `json:"commit,omitempty"`
	Message *string        `json:"message,omitempty"`
	Name    string         `json:"name"`
	Status  string         `json:"status"`
	Version *string        `json:"version,omitempty"`
}

// Health returns an InfluxDB server health check result. Read the HealthCheck.Status field to get server status.
// Health doesn't validate authentication params.
func (c *Client) Health(ctx context.Context) (*HealthCheck, error) {
	queryURL, err := url.Parse(c.params.ServerURL + "/health")
	if err != nil {
		return nil, fmt.Errorf("error calling health:  %w", err)
	}
	resp, herr := c.makeAPICall(ctx, httpParams{
		endpointURL: queryURL,
		httpMethod:  http.MethodGet,
		headers:     nil,
		queryParams: nil,
		body:        nil,
	})
	if herr != nil {
		return nil, fmt.Errorf("error calling health:  %w", herr)
	}
	bodyBytes, err := ioutil.ReadAll(resp.Body)
	defer resp.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("error calling health:  %w", err)
	}

	if resp.Header.Get("Content-Type") == "application/json" {
		var dest HealthCheck
		if err := json.Unmarshal(bodyBytes, &dest); err != nil {
			return nil, fmt.Errorf("error calling health:  %w", err)
		}
		return &dest, nil
	}
	return nil, fmt.Errorf("error calling health: unexpected response: %s", string(bodyBytes))
}

// makeAPICall issues an HTTP request to InfluxDB server API url according to parameters.
// Additionally, sets Authorization header and User-Agent.
// It return http.response or Error
func (c *Client) makeAPICall(ctx context.Context, params httpParams) (*http.Response, *Error) {
	urlParams := make(url.Values)

	for k, v := range params.queryParams {
		urlParams.Set(k, v)
	}
	// copy URL
	urlObj := *params.endpointURL
	urlObj.RawQuery = urlParams.Encode()

	fullURL := urlObj.String()

	req, err := http.NewRequestWithContext(ctx, params.httpMethod, fullURL, params.body)
	if err != nil {
		return nil, NewError(fmt.Sprintf("error calling %s: %s", fullURL, err.Error()))
	}
	for k, v := range params.headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("User-Agent", UserAgent)
	if c.authorization != "" {
		req.Header.Add("Authorization", c.authorization)
	}

	resp, err := c.params.HTTPClient.Do(req)
	if err != nil {
		return nil, NewError(fmt.Sprintf("error calling %s: %s", fullURL, err.Error()))
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, c.resolveHTTPError(resp)
	}

	return resp, nil
}

// resolveHTTPError parses server error response and returns error with human readable message
func (c *Client) resolveHTTPError(r *http.Response) *Error {
	// successful status code range
	if r.StatusCode >= 200 && r.StatusCode < 300 {
		return nil
	}
	defer func() {
		// discard body so connection can be reused
		_, _ = io.Copy(ioutil.Discard, r.Body)
		_ = r.Body.Close()
	}()

	httpError := &Error{}

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
			// InfluxDB 1.x error from v2 compatibility API
			httpError.Message = r.Header.Get("X-Influxdb-Error")
		}
	}
	return httpError
}
