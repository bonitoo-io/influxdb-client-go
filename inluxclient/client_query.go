// Copyright 2021 InfluxData, Inc. All rights reserved.
// Use of this source code is governed by MIT
// license that can be found in the LICENSE file.

package influxclient

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
)

// Dialect defines model for Dialect.
type Dialect struct {
	Annotations []string `json:"annotations"`
	Delimiter   string   `json:"delimiter"`
	Header      bool     `json:"header"`
}

// QueryParam is key-value pair for parameters inside the Flux query
type QueryParam struct {
	Key   string
	Value string
}

// Query defines model for Query.
type Query struct {
	Dialect Dialect           `json:"dialect"`
	Query   string            `json:"query"`
	Type    string            `json:"type"`
	Params  map[string]string `json:"params,omitempty"`
}

// defaultDialect is Query dialect value with all annotations, name header and comma as delimiter
var defaultDialect = Dialect{
	Annotations: []string{"datatype", "default", "group"},
	Delimiter:   ",",
	Header:      true,
}

// Query sends the given flux query on the given organization ID.
// The result must be closed after use.
// Flux query can contains reference to params that must be passed in the params argument
func (c *Client) Query(ctx context.Context, org, query string, params ...QueryParam) (*QueryResultReader, error) {
	queryURL, _ := c.apiURL.Parse("query")

	q := Query{Dialect: defaultDialect, Query: query, Type: "flux", Params: make(map[string]string)}
	for _, r := range params {
		q.Params[r.Key] = r.Value
	}
	qrJSON, err := json.Marshal(q)
	if err != nil {
		return nil, err
	}
	resp, herr := c.makeAPICall(ctx, httpParams{
		endpointURL: queryURL,
		httpMethod:  "POST",
		headers:     map[string]string{"Accept-Encoding": "gzip", "Content-Type": "application/json"},
		queryParams: map[string]string{"org": org},
		body:        bytes.NewReader(qrJSON),
	})
	if herr != nil {
		return nil, herr
	}

	if resp.Header.Get("Content-Encoding") == "gzip" {
		resp.Body, err = gzip.NewReader(resp.Body)
		if err != nil {
			return nil, err
		}
	}
	return NewQueryResultReader(resp.Body), nil
}
