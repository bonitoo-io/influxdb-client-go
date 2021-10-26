// Copyright 2021 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package influxclient

import (
	"context"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNew(t *testing.T) {
	_, err := New(Params{})
	require.Error(t, err)
	assert.Equal(t, "empty server URL", err.Error())

	c, err := New(Params{ServerURL: "http://localhost:8086"})
	require.NoError(t, err)
	assert.Equal(t, "http://localhost:8086", c.params.ServerURL)
	assert.Equal(t, "http://localhost:8086/api/v2/", c.apiURL.String())
	assert.Equal(t, "", c.authorization)

	_, err = New(Params{ServerURL: "localhost\n"})
	if assert.Error(t, err) {
		assert.True(t, strings.HasPrefix(err.Error(), "error parsing server URL:"))
	}

	c, err = New(Params{ServerURL: "http://localhost:8086", AuthToken: "my-token"})
	require.NoError(t, err)
	assert.Equal(t, "Token my-token", c.authorization)
}

func TestURLs(t *testing.T) {
	urls := []struct {
		serverURL    string
		serverAPIURL string
	}{
		{"http://host:8086", "http://host:8086/api/v2/"},
		{"http://host:8086/", "http://host:8086/api/v2/"},
		{"http://host:8086/path", "http://host:8086/path/api/v2/"},
		{"http://host:8086/path/", "http://host:8086/path/api/v2/"},
		{"http://host:8086/path1/path2/path3", "http://host:8086/path1/path2/path3/api/v2/"},
		{"http://host:8086/path1/path2/path3/", "http://host:8086/path1/path2/path3/api/v2/"},
	}
	for _, url := range urls {
		t.Run(url.serverURL, func(t *testing.T) {
			c, err := New(Params{ServerURL: url.serverURL})
			require.NoError(t, err)
			assert.Equal(t, url.serverURL, c.params.ServerURL)
			assert.Equal(t, url.serverAPIURL, c.apiURL.String())
		})
	}
}

func TestHealthOk(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write([]byte(`{"name":"influxdb", "message":"ready for queries and writes", "status":"pass", "checks":[], "version": "2.0.4", "commit": "4e7a59bb9a"}`))
	}))
	defer ts.Close()
	client, err := New(Params{ServerURL: ts.URL, AuthToken: ""})
	require.NoError(t, err)
	check, err := client.Health(context.Background())
	require.NoError(t, err)
	require.NotNil(t, check)
	assert.Equal(t, "influxdb", check.Name)
	assert.Equal(t, "pass", check.Status)
	if assert.NotNil(t, check.Message) {
		assert.Equal(t, "ready for queries and writes", *check.Message)
	}
	if assert.NotNil(t, check.Commit) {
		assert.Equal(t, "4e7a59bb9a", *check.Commit)
	}
	if assert.NotNil(t, check.Version) {
		assert.Equal(t, "2.0.4", *check.Version)
	}
	if assert.NotNil(t, check.Checks) {
		assert.Len(t, *check.Checks, 0)
	}

}

func TestReadyHtml(t *testing.T) {
	html := `<!doctype html><html lang="en"><body><div id="react-root" data-basepath=""></div><script src="/static/39f7ddd770.js"></script></body></html>`
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "text/html")
		w.WriteHeader(200)
		w.Write([]byte(html))
	}))
	defer ts.Close()
	client, err := New(Params{ServerURL: ts.URL, AuthToken: ""})
	require.NoError(t, err)
	check, err := client.Health(context.Background())
	require.Error(t, err)
	assert.Nil(t, check)
	assert.Equal(t, "error calling health: unexpected response: "+html, err.Error())
}
