// Copyright 2021 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package influxclient

import (
	"github.com/stretchr/testify/assert"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
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

func TestReadyOk(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(200)
		w.Write([]byte(`{
    "status": "ready",
    "started": "2021-02-24T12:13:37.681813026Z",
    "up": "5713h41m50.256128486s"
}`))
	}))
	defer ts.Close()
	client, err := New(Params{ServerURL: ts.URL, AuthToken: ""})
	require.NoError(t, err)
	dur, err := client.Ready()
	require.NoError(t, err)
	exp := 5713*time.Hour + 41*time.Minute + 50*time.Second + 256128486*time.Nanosecond
	assert.Equal(t, exp, dur)
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
	dur, err := client.Ready()
	require.Error(t, err)
	assert.Equal(t, time.Duration(0), dur)
	assert.Equal(t, "error calling Ready: unexpected response: "+html, err.Error())
}
