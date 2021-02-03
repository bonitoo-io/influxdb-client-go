package influxclient_test

import (
	"errors"
	"fmt"
	"github.com/stretchr/testify/assert"
	"io"
	"io/ioutil"
	"log"
	"strings"
	"testing"
	"time"

	influxclient "github.com/influxdata/influxdb-client-go/inluxclient"
	"github.com/stretchr/testify/require"
)

func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}

type expectedTable struct {
	columns []influxclient.TableColumn
	rows    [][]interface{}
}

func verifyTables(t *testing.T, csv string, tables []expectedTable) {
	reader := strings.NewReader(csv)
	res := influxclient.NewQueryResult(ioutil.NopCloser(reader))

	for _, table := range tables {
		require.True(t, res.NextTable(), res.Err())
		require.Nil(t, res.Err())
		require.Equal(t, table.columns, res.Columns())
		for _, row := range table.rows {
			require.True(t, res.NextRow(), res.Err())
			require.Nil(t, res.Err())
			require.Equal(t, row, res.Values())
			for i, c := range table.columns {
				require.Equal(t, row[i], res.ValueByName(c.Name))
			}
		}
		require.False(t, res.NextRow(), res.Err())
		require.Nil(t, res.Err())
	}

	require.False(t, res.NextTable(), res.Err())
	require.Nil(t, res.Err())
}

func verifyParsingError(t *testing.T, csvTable, error string) {
	reader := strings.NewReader(csvTable)
	res := influxclient.NewQueryResult(ioutil.NopCloser(reader))

	require.False(t, res.NextTable())
	require.NotNil(t, res.Err())
	assert.Equal(t, error, res.Err().Error())

}

func TestQueryResult(t *testing.T) {
	csvTable := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf

`
	reader := strings.NewReader(csvTable)
	res := influxclient.NewQueryResult(ioutil.NopCloser(reader))
	for res.NextTable() && res.Err() == nil {
		for res.NextRow() {
			// read values
			temp := res.Values()[5].(float64)
			fmt.Println(temp)
		}
	}
	if res.Err() != nil {
		log.Fatal(res.Err())
	}

}

func TestQueryResultSingleTable(t *testing.T) {
	csvTable := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf

`
	tables := []expectedTable{
		{[]influxclient.TableColumn{
			{DataType: "string", DefaultValue: "_result", Name: "result", IsGroup: false},
			{DataType: "long", DefaultValue: "", Name: "table", IsGroup: false},
			{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_start", IsGroup: true},
			{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_stop", IsGroup: true},
			{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_time", IsGroup: false},
			{DataType: "double", DefaultValue: "", Name: "_value", IsGroup: false},
			{DataType: "string", DefaultValue: "", Name: "_field", IsGroup: true},
			{DataType: "string", DefaultValue: "", Name: "_measurement", IsGroup: true},
			{DataType: "string", DefaultValue: "", Name: "a", IsGroup: true},
			{DataType: "string", DefaultValue: "", Name: "b", IsGroup: true},
		},
			[][]interface{}{
				{
					"_result",
					int64(0),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T10:34:08.135814545Z"),
					1.4,
					"f",
					"test",
					"1",
					"adsfasdf",
				},
				{
					"_result",
					int64(0),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.850214724Z"),
					6.6,
					"f",
					"test",
					"1",
					"adsfasdf",
				},
			},
		},
	}

	verifyTables(t, csvTable, tables)
}

func TestQueryResultMultiTables(t *testing.T) {
	csvTableMultiStructure := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,1,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,4,i,test,1,adsfasdf
,,1,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,-1,i,test,1,adsfasdf

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,boolean,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,2,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.62797864Z,false,f,test,0,adsfasdf
,,2,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.969100374Z,true,f,test,0,adsfasdf

d
#datatype,string,long,dateTime:RFC3339Nano,dateTime:RFC3339Nano,dateTime:RFC3339Nano,unsignedLong,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,3,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.62797864Z,0,i,test,0,adsfasdf
,,3,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.969100374Z,2,i,test,0,adsfasdf

`
	tablesMultiStructure := []expectedTable{
		{ // Table 1
			[]influxclient.TableColumn{
				{DataType: "string", DefaultValue: "_result", Name: "result", IsGroup: false},
				{DataType: "long", DefaultValue: "", Name: "table", IsGroup: false},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_start", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_stop", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_time", IsGroup: false},
				{DataType: "double", DefaultValue: "", Name: "_value", IsGroup: false},
				{DataType: "string", DefaultValue: "", Name: "_field", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "_measurement", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "a", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "b", IsGroup: true},
			},
			[][]interface{}{
				{"_result",
					int64(0),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T10:34:08.135814545Z"),
					1.4,
					"f",
					"test",
					"1",
					"adsfasdf",
				},
				{
					"_result",
					int64(0),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.850214724Z"),
					6.6,
					"f",
					"test",
					"1",
					"adsfasdf",
				},
			},
		},
		{ //Table 2
			[]influxclient.TableColumn{
				{DataType: "string", DefaultValue: "_result", Name: "result", IsGroup: false},
				{DataType: "long", DefaultValue: "", Name: "table", IsGroup: false},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_start", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_stop", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_time", IsGroup: false},
				{DataType: "long", DefaultValue: "", Name: "_value", IsGroup: false},
				{DataType: "string", DefaultValue: "", Name: "_field", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "_measurement", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "a", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "b", IsGroup: true},
			},
			[][]interface{}{
				{"_result",
					int64(1),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T10:34:08.135814545Z"),
					int64(4),
					"i",
					"test",
					"1",
					"adsfasdf",
				},
				{
					"_result",
					int64(1),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.850214724Z"),
					int64(-1),
					"i",
					"test",
					"1",
					"adsfasdf",
				},
			},
		},
		{ // Table 3
			[]influxclient.TableColumn{
				{DataType: "string", DefaultValue: "_result", Name: "result", IsGroup: false},
				{DataType: "long", DefaultValue: "", Name: "table", IsGroup: false},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_start", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_stop", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_time", IsGroup: false},
				{DataType: "boolean", DefaultValue: "", Name: "_value", IsGroup: false},
				{DataType: "string", DefaultValue: "", Name: "_field", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "_measurement", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "a", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "b", IsGroup: true},
			},
			[][]interface{}{
				{"_result",
					int64(2),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.62797864Z"),
					false,
					"f",
					"test",
					"0",
					"adsfasdf",
				},
				{
					"_result",
					int64(2),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.969100374Z"),
					true,
					"f",
					"test",
					"0",
					"adsfasdf",
				},
			},
		},
		{ //Table 4
			[]influxclient.TableColumn{
				{DataType: "string", DefaultValue: "_result", Name: "result", IsGroup: false},
				{DataType: "long", DefaultValue: "", Name: "table", IsGroup: false},
				{DataType: "dateTime:RFC3339Nano", DefaultValue: "", Name: "_start", IsGroup: true},
				{DataType: "dateTime:RFC3339Nano", DefaultValue: "", Name: "_stop", IsGroup: true},
				{DataType: "dateTime:RFC3339Nano", DefaultValue: "", Name: "_time", IsGroup: false},
				{DataType: "unsignedLong", DefaultValue: "", Name: "_value", IsGroup: false},
				{DataType: "string", DefaultValue: "", Name: "_field", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "_measurement", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "a", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "b", IsGroup: true},
			},
			[][]interface{}{
				{"_result",
					int64(3),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.62797864Z"),
					uint64(0),
					"i",
					"test",
					"0",
					"adsfasdf",
				},
				{
					"_result",
					int64(3),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.969100374Z"),
					uint64(2),
					"i",
					"test",
					"0",
					"adsfasdf",
				},
			},
		},
	}
	verifyTables(t, csvTableMultiStructure, tablesMultiStructure)

	csvTableMultiTables := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf
,,1,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,4.3,i,test,1,xyxyxyxy
,,1,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,-1.2,i,test,1,xyxyxyxy
,,2,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.62797864Z,0.1,f,test,0,adsfasdf
,,2,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.969100374Z,0.3,f,test,0,adsfasdf
,,3,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.62797864Z,10,i,test,0,xyxyxyxy
,,3,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.969100374Z,2,i,test,0,xyxyxyxy

`
	tablesMultiTables := []expectedTable{
		{
			[]influxclient.TableColumn{
				{DataType: "string", DefaultValue: "_result", Name: "result", IsGroup: false},
				{DataType: "long", DefaultValue: "", Name: "table", IsGroup: false},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_start", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_stop", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_time", IsGroup: false},
				{DataType: "double", DefaultValue: "", Name: "_value", IsGroup: false},
				{DataType: "string", DefaultValue: "", Name: "_field", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "_measurement", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "a", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "b", IsGroup: true},
			},
			[][]interface{}{
				{"_result",
					int64(0),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T10:34:08.135814545Z"),
					1.4,
					"f",
					"test",
					"1",
					"adsfasdf",
				},
				{
					"_result",
					int64(0),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.850214724Z"),
					6.6,
					"f",
					"test",
					"1",
					"adsfasdf",
				},
				{"_result",
					int64(1),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T10:34:08.135814545Z"),
					4.3,
					"i",
					"test",
					"1",
					"xyxyxyxy",
				},
				{
					"_result",
					int64(1),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.850214724Z"),
					-1.2,
					"i",
					"test",
					"1",
					"xyxyxyxy",
				},
				{"_result",
					int64(2),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.62797864Z"),
					0.1,
					"f",
					"test",
					"0",
					"adsfasdf",
				},
				{
					"_result",
					int64(2),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.969100374Z"),
					0.3,
					"f",
					"test",
					"0",
					"adsfasdf",
				},
				{"_result",
					int64(3),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.62797864Z"),
					float64(10),
					"i",
					"test",
					"0",
					"xyxyxyxy",
				},
				{
					"_result",
					int64(3),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.969100374Z"),
					float64(2),
					"i",
					"test",
					"0",
					"xyxyxyxy",
				},
			},
		},
	}
	verifyTables(t, csvTableMultiTables, tablesMultiTables)

}

func TestAdvanceInTable(t *testing.T) {
	csvTableMultiStructure := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,1,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,4,i,test,1,adsfasdf
,,1,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,-1,i,test,1,adsfasdf

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,boolean,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,2,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.62797864Z,false,f,test,0,adsfasdf
,,2,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.969100374Z,true,f,test,0,adsfasdf

d
#datatype,string,long,dateTime:RFC3339Nano,dateTime:RFC3339Nano,dateTime:RFC3339Nano,unsignedLong,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,3,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.62797864Z,0,i,test,0,adsfasdf
,,3,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.969100374Z,2,i,test,0,adsfasdf

`
	tables := []expectedTable{
		{ // Table 1
			[]influxclient.TableColumn{
				{DataType: "string", DefaultValue: "_result", Name: "result", IsGroup: false},
				{DataType: "long", DefaultValue: "", Name: "table", IsGroup: false},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_start", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_stop", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_time", IsGroup: false},
				{DataType: "double", DefaultValue: "", Name: "_value", IsGroup: false},
				{DataType: "string", DefaultValue: "", Name: "_field", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "_measurement", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "a", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "b", IsGroup: true},
			},
			[][]interface{}{
				{"_result",
					int64(0),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T10:34:08.135814545Z"),
					1.4,
					"f",
					"test",
					"1",
					"adsfasdf",
				},
				{
					"_result",
					int64(0),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.850214724Z"),
					6.6,
					"f",
					"test",
					"1",
					"adsfasdf",
				},
			},
		},
		{ //Table 2
			[]influxclient.TableColumn{
				{DataType: "string", DefaultValue: "_result", Name: "result", IsGroup: false},
				{DataType: "long", DefaultValue: "", Name: "table", IsGroup: false},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_start", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_stop", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_time", IsGroup: false},
				{DataType: "long", DefaultValue: "", Name: "_value", IsGroup: false},
				{DataType: "string", DefaultValue: "", Name: "_field", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "_measurement", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "a", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "b", IsGroup: true},
			},
			[][]interface{}{
				{"_result",
					int64(1),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T10:34:08.135814545Z"),
					int64(4),
					"i",
					"test",
					"1",
					"adsfasdf",
				},
				{
					"_result",
					int64(1),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.850214724Z"),
					int64(-1),
					"i",
					"test",
					"1",
					"adsfasdf",
				},
			},
		},
		{ // Table 3
			[]influxclient.TableColumn{
				{DataType: "string", DefaultValue: "_result", Name: "result", IsGroup: false},
				{DataType: "long", DefaultValue: "", Name: "table", IsGroup: false},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_start", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_stop", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_time", IsGroup: false},
				{DataType: "boolean", DefaultValue: "", Name: "_value", IsGroup: false},
				{DataType: "string", DefaultValue: "", Name: "_field", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "_measurement", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "a", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "b", IsGroup: true},
			},
			[][]interface{}{
				{"_result",
					int64(2),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.62797864Z"),
					false,
					"f",
					"test",
					"0",
					"adsfasdf",
				},
				{
					"_result",
					int64(2),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.969100374Z"),
					true,
					"f",
					"test",
					"0",
					"adsfasdf",
				},
			},
		},
		{ //Table 4
			[]influxclient.TableColumn{
				{DataType: "string", DefaultValue: "_result", Name: "result", IsGroup: false},
				{DataType: "long", DefaultValue: "", Name: "table", IsGroup: false},
				{DataType: "dateTime:RFC3339Nano", DefaultValue: "", Name: "_start", IsGroup: true},
				{DataType: "dateTime:RFC3339Nano", DefaultValue: "", Name: "_stop", IsGroup: true},
				{DataType: "dateTime:RFC3339Nano", DefaultValue: "", Name: "_time", IsGroup: false},
				{DataType: "unsignedLong", DefaultValue: "", Name: "_value", IsGroup: false},
				{DataType: "string", DefaultValue: "", Name: "_field", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "_measurement", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "a", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "b", IsGroup: true},
			},
			[][]interface{}{
				{"_result",
					int64(3),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.62797864Z"),
					uint64(0),
					"i",
					"test",
					"0",
					"adsfasdf",
				},
				{
					"_result",
					int64(3),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.969100374Z"),
					uint64(2),
					"i",
					"test",
					"0",
					"adsfasdf",
				},
			},
		},
	}

	reader := strings.NewReader(csvTableMultiStructure)
	res := influxclient.NewQueryResult(ioutil.NopCloser(reader))

	//test skip first table header
	require.True(t, res.NextRow())
	require.Nil(t, res.Err())
	require.Equal(t, tables[0].rows[0], res.Values())
	_ = res.Close()

	reader = strings.NewReader(csvTableMultiStructure)
	res = influxclient.NewQueryResult(ioutil.NopCloser(reader))

	//test skip tables
	require.True(t, res.NextTable())
	require.Nil(t, res.Err())
	require.True(t, res.NextTable())
	require.Nil(t, res.Err())
	require.True(t, res.NextRow())
	require.Nil(t, res.Err())
	require.True(t, res.NextTable())
	require.Nil(t, res.Err())
	require.True(t, res.NextRow())
	require.Nil(t, res.Err())
	require.Equal(t, tables[2].rows[0], res.Values())

	_ = res.Close()

	csvTableMultiTables := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf
,,1,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,4.3,i,test,1,xyxyxyxy
,,1,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,-1.2,i,test,1,xyxyxyxy
,,2,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.62797864Z,0.1,f,test,0,adsfasdf
,,2,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.969100374Z,0.3,f,test,0,adsfasdf
,,3,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.62797864Z,10,i,test,0,xyxyxyxy
,,3,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.969100374Z,2,i,test,0,xyxyxyxy

`
	reader = strings.NewReader(csvTableMultiTables)
	res = influxclient.NewQueryResult(ioutil.NopCloser(reader))

	//test skip first table header
	require.True(t, res.NextRow() && res.NextRow())
	require.Nil(t, res.Err())
	require.Equal(t, tables[0].rows[1], res.Values())
	_ = res.Close()

	reader = strings.NewReader(csvTableMultiTables)
	res = influxclient.NewQueryResult(ioutil.NopCloser(reader))
	require.True(t, res.NextTable())
	require.Nil(t, res.Err())
	require.False(t, res.NextTable())
	require.Nil(t, res.Err())
}

func TestTableColumn_String(t *testing.T) {
	col1 := influxclient.TableColumn{DataType: "string", DefaultValue: "_result", Name: "result", IsGroup: false}
	col2 := influxclient.TableColumn{DataType: "string", DefaultValue: "", Name: "b", IsGroup: true}

	assert.Equal(t, "{name: result, datatype: string, defaultValue: _result, group: false}", col1.String())
	assert.Equal(t, "{name: b, datatype: string, defaultValue: , group: true}", col2.String())

}

func TestQueryResult_ValueByName(t *testing.T) {
	csvTable := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,long,string,duration,base64Binary,dateTime:RFC3339
#group,false,false,true,true,false,true,true,false,false,false
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,deviceId,sensor,elapsed,note,start
,,0,2020-04-28T12:36:50.990018157Z,2020-04-28T12:51:50.990018157Z,2020-04-28T12:38:11.480545389Z,1467463,BME280,1m1s,ZGF0YWluYmFzZTY0,2020-04-27T00:00:00Z
,,1,2020-04-28T12:36:50.990018157Z,2020-04-28T12:51:50.990018157Z,2020-04-28T12:39:36.330153686Z,1467463,BME280,1h20m30.13245s,eHh4eHhjY2NjY2NkZGRkZA==,2020-04-28T00:00:00Z

#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf

`
	tables := []expectedTable{
		{ // Table 1
			[]influxclient.TableColumn{
				{DataType: "string", DefaultValue: "_result", Name: "result", IsGroup: false},
				{DataType: "long", DefaultValue: "", Name: "table", IsGroup: false},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_start", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_stop", IsGroup: true},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_time", IsGroup: false},
				{DataType: "long", DefaultValue: "", Name: "deviceId", IsGroup: true},
				{DataType: "string", DefaultValue: "", Name: "sensor", IsGroup: true},
				{DataType: "duration", DefaultValue: "", Name: "elapsed", IsGroup: false},
				{DataType: "base64Binary", DefaultValue: "", Name: "note", IsGroup: false},
				{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "start", IsGroup: false},
			},
			[][]interface{}{
				{
					"_result",
					int64(0),
					mustParseTime("2020-04-28T12:36:50.990018157Z"),
					mustParseTime("2020-04-28T12:51:50.990018157Z"),
					mustParseTime("2020-04-28T12:38:11.480545389Z"),
					int64(1467463),
					"BME280",
					time.Minute + time.Second,
					[]byte("datainbase64"),
					time.Date(2020, 4, 27, 0, 0, 0, 0, time.UTC),
				},
				{
					"_result",
					int64(1),
					mustParseTime("2020-04-28T12:36:50.990018157Z"),
					mustParseTime("2020-04-28T12:51:50.990018157Z"),
					mustParseTime("2020-04-28T12:39:36.330153686Z"),
					int64(1467463),
					"BME280",
					time.Hour + 20*time.Minute + 30*time.Second + 132450000*time.Nanosecond,
					[]byte("xxxxxccccccddddd"),
					time.Date(2020, 4, 28, 0, 0, 0, 0, time.UTC),
				},
			},
		},
		{[]influxclient.TableColumn{
			{DataType: "string", DefaultValue: "_result", Name: "result", IsGroup: false},
			{DataType: "long", DefaultValue: "", Name: "table", IsGroup: false},
			{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_start", IsGroup: true},
			{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_stop", IsGroup: true},
			{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_time", IsGroup: false},
			{DataType: "double", DefaultValue: "", Name: "_value", IsGroup: false},
			{DataType: "string", DefaultValue: "", Name: "_field", IsGroup: true},
			{DataType: "string", DefaultValue: "", Name: "_measurement", IsGroup: true},
			{DataType: "string", DefaultValue: "", Name: "a", IsGroup: true},
			{DataType: "string", DefaultValue: "", Name: "b", IsGroup: true},
		},
			[][]interface{}{
				{
					"_result",
					int64(0),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T10:34:08.135814545Z"),
					1.4,
					"f",
					"test",
					"1",
					"adsfasdf",
				},
				{
					"_result",
					int64(0),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.850214724Z"),
					6.6,
					"f",
					"test",
					"1",
					"adsfasdf",
				},
			},
		},
	}

	verifyTables(t, csvTable, tables)

	reader := strings.NewReader(csvTable)
	res := influxclient.NewQueryResult(ioutil.NopCloser(reader))

	require.True(t, res.NextTable() && res.NextRow(), res.Err())
	require.Nil(t, res.Err())
	assert.Equal(t, []byte("datainbase64"), res.ValueByName("note"))
	assert.Nil(t, res.ValueByName(""))
	assert.Nil(t, res.ValueByName("invalid"))
	assert.Nil(t, res.ValueByName("a"))

	require.True(t, res.NextTable() && res.NextRow(), res.Err())
	assert.Equal(t, "1", res.ValueByName("a"))
	assert.Nil(t, res.ValueByName("note"))

}

func TestErrorInRow(t *testing.T) {
	csvTableError := `#datatype,string,string
#group,true,true
#default,,
,error,reference
,failed to create physical plan: invalid time bounds from procedure from: bounds contain zero time,897`

	verifyParsingError(t, csvTableError, "failed to create physical plan: invalid time bounds from procedure from: bounds contain zero time,897")

	csvTableErrorNoReference := `#datatype,string,string
#group,true,true
#default,,
,error,reference
,failed to create physical plan: invalid time bounds from procedure from: bounds contain zero time,`
	verifyParsingError(t, csvTableErrorNoReference, "failed to create physical plan: invalid time bounds from procedure from: bounds contain zero time")

	csvTableErrorNoMessage := `#datatype,string,string
#group,true,true
#default,,
,error,reference
,,`
	verifyParsingError(t, csvTableErrorNoMessage, "unknown query error")
}

func TestInvalidDataType(t *testing.T) {
	csvTable := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,int,string,duration,base64Binary,dateTime:RFC3339
#group,false,false,true,true,false,true,true,false,false,false
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,deviceId,sensor,elapsed,note,start
,,0,2020-04-28T12:36:50.990018157Z,2020-04-28T12:51:50.990018157Z,2020-04-28T12:38:11.480545389Z,1467463,BME280,1m1s,ZGF0YWluYmFzZTY0,2020-04-27T00:00:00Z
,,0,2020-04-28T12:36:50.990018157Z,2020-04-28T12:51:50.990018157Z,2020-04-28T12:39:36.330153686Z,1467463,BME280,1h20m30.13245s,eHh4eHhjY2NjY2NkZGRkZA==,2020-04-28T00:00:00Z
`

	reader := strings.NewReader(csvTable)
	res := influxclient.NewQueryResult(ioutil.NopCloser(reader))

	require.True(t, res.NextTable())
	require.Nil(t, res.Err())
	require.False(t, res.NextRow())
	require.NotNil(t, res.Err())
	assert.Equal(t, "deviceId has unknown data type int", res.Err().Error())
}

func TestReorderedAnnotations(t *testing.T) {
	csvTable1 := `#group,false,false,true,true,false,false,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf

`
	tables := []expectedTable{
		{[]influxclient.TableColumn{
			{DataType: "string", DefaultValue: "_result", Name: "result", IsGroup: false},
			{DataType: "long", DefaultValue: "", Name: "table", IsGroup: false},
			{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_start", IsGroup: true},
			{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_stop", IsGroup: true},
			{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_time", IsGroup: false},
			{DataType: "double", DefaultValue: "", Name: "_value", IsGroup: false},
			{DataType: "string", DefaultValue: "", Name: "_field", IsGroup: true},
			{DataType: "string", DefaultValue: "", Name: "_measurement", IsGroup: true},
			{DataType: "string", DefaultValue: "", Name: "a", IsGroup: true},
			{DataType: "string", DefaultValue: "", Name: "b", IsGroup: true},
		},
			[][]interface{}{
				{
					"_result",
					int64(0),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T10:34:08.135814545Z"),
					1.4,
					"f",
					"test",
					"1",
					"adsfasdf",
				},
				{
					"_result",
					int64(0),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.850214724Z"),
					6.6,
					"f",
					"test",
					"1",
					"adsfasdf",
				},
			},
		},
	}

	verifyTables(t, csvTable1, tables)

	csvTable2 := `#default,_result,,,,,,,,,
#group,false,false,true,true,false,false,true,true,true,true
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf

`
	verifyTables(t, csvTable2, tables)
}

func TestDatatypeOnlyAnnotation(t *testing.T) {
	csvTable1 := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf

`
	tables := []expectedTable{
		{[]influxclient.TableColumn{
			{DataType: "string", DefaultValue: "", Name: "result", IsGroup: false},
			{DataType: "long", DefaultValue: "", Name: "table", IsGroup: false},
			{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_start", IsGroup: false},
			{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_stop", IsGroup: false},
			{DataType: "dateTime:RFC3339", DefaultValue: "", Name: "_time", IsGroup: false},
			{DataType: "double", DefaultValue: "", Name: "_value", IsGroup: false},
			{DataType: "string", DefaultValue: "", Name: "_field", IsGroup: false},
			{DataType: "string", DefaultValue: "", Name: "_measurement", IsGroup: false},
			{DataType: "string", DefaultValue: "", Name: "a", IsGroup: false},
			{DataType: "string", DefaultValue: "", Name: "b", IsGroup: false},
		},
			[][]interface{}{
				{
					nil,
					int64(0),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T10:34:08.135814545Z"),
					1.4,
					"f",
					"test",
					"1",
					"adsfasdf",
				},
				{
					nil,
					int64(0),
					mustParseTime("2020-02-17T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:19:49.747562847Z"),
					mustParseTime("2020-02-18T22:08:44.850214724Z"),
					6.6,
					"f",
					"test",
					"1",
					"adsfasdf",
				},
			},
		},
	}

	verifyTables(t, csvTable1, tables)
}

func TestMissingDatatypeAnnotation(t *testing.T) {
	csvTable1 := `
#group,false,false,true,true,false,true,true,false,false,false
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,deviceId,sensor,elapsed,note,start
,,0,2020-04-28T12:36:50.990018157Z,2020-04-28T12:51:50.990018157Z,2020-04-28T12:38:11.480545389Z,1467463,BME280,1m1s,ZGF0YWluYmFzZTY0,2020-04-27T00:00:00Z
,,0,2020-04-28T12:36:50.990018157Z,2020-04-28T12:51:50.990018157Z,2020-04-28T12:39:36.330153686Z,1467463,BME280,1h20m30.13245s,eHh4eHhjY2NjY2NkZGRkZA==,2020-04-28T00:00:00Z
`

	verifyParsingError(t, csvTable1, "parsing error, datatype annotation not found")

	csvTable2 := `
#default,_result,,,,,,,,,
#group,false,false,true,true,false,true,true,false,false,false
,result,table,_start,_stop,_time,deviceId,sensor,elapsed,note,start
,,0,2020-04-28T12:36:50.990018157Z,2020-04-28T12:51:50.990018157Z,2020-04-28T12:38:11.480545389Z,1467463,BME280,1m1s,ZGF0YWluYmFzZTY0,2020-04-27T00:00:00Z
,,0,2020-04-28T12:36:50.990018157Z,2020-04-28T12:51:50.990018157Z,2020-04-28T12:39:36.330153686Z,1467463,BME280,1h20m30.13245s,eHh4eHhjY2NjY2NkZGRkZA==,2020-04-28T00:00:00Z
`
	verifyParsingError(t, csvTable2, "parsing error, datatype annotation not found")
}

func TestMissingAnnotations(t *testing.T) {
	csvTable := `
,result,table,_start,_stop,_time,deviceId,sensor,elapsed,note,start
,,0,2020-04-28T12:36:50.990018157Z,2020-04-28T12:51:50.990018157Z,2020-04-28T12:38:11.480545389Z,1467463,BME280,1m1s,ZGF0YWluYmFzZTY0,2020-04-27T00:00:00Z
,,0,2020-04-28T12:36:50.990018157Z,2020-04-28T12:51:50.990018157Z,2020-04-28T12:39:36.330153686Z,1467463,BME280,1h20m30.13245s,eHh4eHhjY2NjY2NkZGRkZA==,2020-04-28T00:00:00Z

`
	verifyParsingError(t, csvTable, "parsing error, annotations not found")
}

func TestDifferentNumberOfColumns(t *testing.T) {
	csvTable := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,int,string,duration,base64Binary,dateTime:RFC3339
#group,false,false,true,true,false,true,true,false,false,
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,deviceId,sensor,elapsed,note,start
,,0,2020-04-28T12:36:50.990018157Z,2020-04-28T12:51:50.990018157Z,2020-04-28T12:38:11.480545389Z,1467463,BME280,1m1s,ZGF0YWluYmFzZTY0,2020-04-27T00:00:00Z,2345234
`

	verifyParsingError(t, csvTable, "parsing error, row has different number of columns than the table: 11 vs 10")

	csvTable2 := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,int,string,duration,base64Binary,dateTime:RFC3339
#group,false,false,true,true,false,true,true,false,false,
#default,_result,,,,,,,
,result,table,_start,_stop,_time,deviceId,sensor,elapsed,note,start
,,0,2020-04-28T12:36:50.990018157Z,2020-04-28T12:51:50.990018157Z,2020-04-28T12:38:11.480545389Z,1467463,BME280,1m1s,ZGF0YWluYmFzZTY0,2020-04-27T00:00:00Z,2345234
`

	verifyParsingError(t, csvTable2, "parsing error, row has different number of columns than the table: 8 vs 10")

	csvTable3 := `#default,_result,,,,,,,
#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,int,string,duration,base64Binary,dateTime:RFC3339
#group,false,false,true,true,false,true,true,false,false,
,result,table,_start,_stop,_time,deviceId,sensor,elapsed,note,start
,,0,2020-04-28T12:36:50.990018157Z,2020-04-28T12:51:50.990018157Z,2020-04-28T12:38:11.480545389Z,1467463,BME280,1m1s,ZGF0YWluYmFzZTY0,2020-04-27T00:00:00Z,2345234
`

	verifyParsingError(t, csvTable3, "parsing error, row has different number of columns than the table: 10 vs 8")
}

func TestCSVError(t *testing.T) {
	csvErrTable := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,",0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf
`
	reader := strings.NewReader(csvErrTable)
	res := influxclient.NewQueryResult(ioutil.NopCloser(reader))

	require.False(t, res.NextTable())
	require.NotNil(t, res.Err())
}

type errCloser struct {
	io.Reader
}

func (errCloser) Close() error {
	return errors.New("close error")
}

func newErrCloser(r io.Reader) io.ReadCloser {
	return errCloser{r}
}

func TestCloseError(t *testing.T) {
	csvTable := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf
`
	csvErrTable := `#datatype,string,long,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339,double,string,string,string,string
,result,table,_start,_stop,_time,_value,_field,_measurement,a,b
,",0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,1.4,f,test,1,adsfasdf
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,6.6,f,test,1,adsfasdf
`
	reader := strings.NewReader(csvTable)
	res := influxclient.NewQueryResult(newErrCloser(reader))

	require.True(t, res.NextTable() && res.NextRow() && res.NextRow())
	require.False(t, res.NextRow())
	require.NotNil(t, res.Err())
	assert.Equal(t, "close error", res.Err().Error())

	reader = strings.NewReader(csvErrTable)
	res = influxclient.NewQueryResult(newErrCloser(reader))

	require.False(t, res.NextTable())
	require.NotNil(t, res.Err())
	assert.True(t, strings.HasPrefix(res.Err().Error(), "close error,"))
}
