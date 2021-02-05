package influxclient_test

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"strings"
	"testing"
	"time"

	influxclient "github.com/influxdata/influxdb-client-go/inluxclient"
)

func TestDecode(t *testing.T) {
	csvTable := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,duration,string,string,base64Binary,boolean
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,took,_field,_measurement,note,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,32m,f,test,ZGF0YWluYmFzZTY0,true
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,1h23m4s,f,test,eHh4eHhjY2NjY2NkZGRkZA==,false

`
	reader := strings.NewReader(csvTable)
	res := influxclient.NewQueryResults(ioutil.NopCloser(reader))
	require.True(t, res.NextRow())
	require.Nil(t, res.Err())

	m := map[string]string{}
	err := res.Decode(m)
	require.Nil(t, err)
	assert.Equal(t, map[string]string{
		"result":       "_result",
		"table":        "0",
		"_start":       "2020-02-17T22:19:49.747562847Z",
		"_stop":        "2020-02-18T22:19:49.747562847Z",
		"_time":        "2020-02-18T10:34:08.135814545Z",
		"took":         "32m",
		"_field":       "f",
		"_measurement": "test",
		"note":         "ZGF0YWluYmFzZTY0",
		"b":            "true",
	}, m)

	m2 := map[string]interface{}{}
	err = res.Decode(m2)
	require.Nil(t, err)
	assert.Equal(t, map[string]interface{}{
		"result":       "_result",
		"table":        int64(0),
		"_start":       mustParseTime("2020-02-17T22:19:49.747562847Z"),
		"_stop":        mustParseTime("2020-02-18T22:19:49.747562847Z"),
		"_time":        mustParseTime("2020-02-18T10:34:08.135814545Z"),
		"took":         time.Minute * 32,
		"_field":       "f",
		"_measurement": "test",
		"note":         []byte("datainbase64"),
		"b":            true,
	}, m2)
	s := &struct {
		Table int           `flux:"table"`
		Start time.Time     `flux:"_start"`
		Stop  time.Time     `flux:"_stop"`
		Time  time.Time     `flux:"_time"`
		Took  time.Duration `flux:"took"`
		Field string        `flux:"_field"`
		Meas  string        `flux:"_measurement"`
		Note  []byte        `flux:"note"`
		Tag   bool          `flux:"b"`
	}{}
	err = res.Decode(s)
	require.Nil(t, err)
	assert.Equal(t, &struct {
		Table int           `flux:"table"`
		Start time.Time     `flux:"_start"`
		Stop  time.Time     `flux:"_stop"`
		Time  time.Time     `flux:"_time"`
		Took  time.Duration `flux:"took"`
		Field string        `flux:"_field"`
		Meas  string        `flux:"_measurement"`
		Note  []byte        `flux:"note"`
		Tag   bool          `flux:"b"`
	}{
		Table: 0,
		Start: mustParseTime("2020-02-17T22:19:49.747562847Z"),
		Stop:  mustParseTime("2020-02-18T22:19:49.747562847Z"),
		Time:  mustParseTime("2020-02-18T10:34:08.135814545Z"),
		Took:  time.Minute * 32,
		Field: "f",
		Meas:  "test",
		Note:  []byte("datainbase64"),
		Tag:   true,
	}, s)

}
