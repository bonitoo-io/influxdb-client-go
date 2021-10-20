package influxclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQuery(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "text/csv")
		w.WriteHeader(200)
		w.Write([]byte(`#group,false,false,true,true,false,true,true,true,false,false,false
#default,_result,,,,,,,,,,
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,string,string,string,double,double,double
,result,table,_start,_stop,_time,deviceId,location,sensor,air_hum,air_press,air_temp
,,0,2021-10-19T14:39:57.464357168Z,2021-10-19T14:54:57.464357168Z,2021-10-19T14:40:21.833564544Z,2663346492,saman-home-room-0-1,BME280,48.8,1022.28,22.73
,,0,2021-10-19T14:39:57.464357168Z,2021-10-19T14:54:57.464357168Z,2021-10-19T14:41:29.840881203Z,2663346492,saman-home-room-0-1,BME280,49.2,1022.34,22.7`))
	}))
	defer ts.Close()
	client, err := New(Params{ServerURL: ts.URL, AuthToken: ""})
	require.NoError(t, err)
	res, err := client.Query(context.Background(), "1", "1")
	require.NoError(t, err)
	defer res.Close()
	i := 0
	for res.NextSection() && res.Err() == nil {
		for res.NextRow() {
			i++
		}
	}
	require.NoError(t, res.Err())
	assert.Equal(t, 2, i)

}

func TestQueryError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Add("Content-Type", "application/json")
		w.WriteHeader(400)
		w.Write([]byte(`{"code":"invalid","message":"compilation failed: error at @1:170-1:171: invalid expression @1:167-1:168: |"}`))
	}))
	defer ts.Close()
	client, err := New(Params{ServerURL: ts.URL, AuthToken: ""})
	require.NoError(t, err)
	res, err := client.Query(context.Background(), "1", "1")
	assert.Nil(t, res)
	require.Error(t, err)
	assert.Equal(t, `invalid: compilation failed: error at @1:170-1:171: invalid expression @1:167-1:168: |`, err.Error())
}
