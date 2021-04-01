package annotatedcsv

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDecodeStructBasicAllTypes(t *testing.T) {
	csvTable := `#datatype,string,unsignedLong,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339Nano,duration,string,long,base64Binary,boolean
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,took,_field,index,note,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,32m,f,-1,ZGF0YWluYmFzZTY0,true
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,1h23m4s,f,1,eHh4eHhjY2NjY2NkZGRkZA==,false

#datatype,long,double,dateTime,string
#default,,,,
,index,score,time,name
,0,3.3,2021-02-18T10:34:08.135814545Z,Thomas
,1,5.1,2021-02-18T22:08:44.850214724Z,John

`
	reader := strings.NewReader(csvTable)
	res := NewReader(reader)
	require.True(t, res.NextSection())
	require.NoError(t, res.Err())
	require.True(t, res.NextRow())
	require.NoError(t, res.Err())

	s := &struct {
		Table uint          `flux:"table"`
		Start time.Time     `flux:"_start"`
		Stop  time.Time     `flux:"_stop"`
		Time  time.Time     `flux:"_time"`
		Took  time.Duration `flux:"took"`
		Field string        `flux:"_field"`
		Index int           `flux:"index"`
		Note  []byte        `flux:"note"`
		Tag   bool          `flux:"b"`
	}{}
	err := res.Decode(s)
	require.NoError(t, err)
	assert.Equal(t, &struct {
		Table uint          `flux:"table"`
		Start time.Time     `flux:"_start"`
		Stop  time.Time     `flux:"_stop"`
		Time  time.Time     `flux:"_time"`
		Took  time.Duration `flux:"took"`
		Field string        `flux:"_field"`
		Index int           `flux:"index"`
		Note  []byte        `flux:"note"`
		Tag   bool          `flux:"b"`
	}{
		Table: 0,
		Start: mustParseTime("2020-02-17T22:19:49.747562847Z"),
		Stop:  mustParseTime("2020-02-18T22:19:49.747562847Z"),
		Time:  mustParseTime("2020-02-18T10:34:08.135814545Z"),
		Took:  time.Minute * 32,
		Field: "f",
		Index: -1,
		Note:  []byte("datainbase64"),
		Tag:   true,
	}, s)

	s2 := &struct {
		Table interface{} `flux:"table"`
		Start interface{} `flux:"_start"`
		Stop  interface{} `flux:"_stop"`
		Time  interface{} `flux:"_time"`
		Took  interface{} `flux:"took"`
		Field interface{} `flux:"_field"`
		Index interface{} `flux:"index"`
		Note  interface{} `flux:"note"`
		Tag   interface{} `flux:"b"`
	}{}
	err = res.Decode(s2)
	require.NoError(t, err)
	assert.Equal(t, &struct {
		Table interface{} `flux:"table"`
		Start interface{} `flux:"_start"`
		Stop  interface{} `flux:"_stop"`
		Time  interface{} `flux:"_time"`
		Took  interface{} `flux:"took"`
		Field interface{} `flux:"_field"`
		Index interface{} `flux:"index"`
		Note  interface{} `flux:"note"`
		Tag   interface{} `flux:"b"`
	}{
		Table: uint64(0),
		Start: mustParseTime("2020-02-17T22:19:49.747562847Z"),
		Stop:  mustParseTime("2020-02-18T22:19:49.747562847Z"),
		Time:  mustParseTime("2020-02-18T10:34:08.135814545Z"),
		Took:  time.Minute * 32,
		Field: "f",
		Index: int64(-1),
		Note:  []byte("datainbase64"),
		Tag:   true,
	}, s2)

	s3 := &struct {
		Table string `flux:"table"`
		Start string `flux:"_start"`
		Stop  string `flux:"_stop"`
		Time  string `flux:"_time"`
		Took  string `flux:"took"`
		Field string `flux:"_field"`
		Index string `flux:"index"`
		Note  string `flux:"note"`
		Tag   string `flux:"b"`
	}{}
	err = res.Decode(s3)
	require.NoError(t, err)
	assert.Equal(t, &struct {
		Table string `flux:"table"`
		Start string `flux:"_start"`
		Stop  string `flux:"_stop"`
		Time  string `flux:"_time"`
		Took  string `flux:"took"`
		Field string `flux:"_field"`
		Index string `flux:"index"`
		Note  string `flux:"note"`
		Tag   string `flux:"b"`
	}{
		Table: "0",
		Start: "2020-02-17T22:19:49.747562847Z",
		Stop:  "2020-02-18T22:19:49.747562847Z",
		Time:  "2020-02-18T10:34:08.135814545Z",
		Took:  "32m",
		Field: "f",
		Index: "-1",
		Note:  "ZGF0YWluYmFzZTY0",
		Tag:   "true",
	}, s3)

	require.True(t, res.NextRow(), res.Err())
	require.NoError(t, res.Err())
	err = res.Decode(s)
	require.NoError(t, err)

	assert.Equal(t, &struct {
		Table uint          `flux:"table"`
		Start time.Time     `flux:"_start"`
		Stop  time.Time     `flux:"_stop"`
		Time  time.Time     `flux:"_time"`
		Took  time.Duration `flux:"took"`
		Field string        `flux:"_field"`
		Index int           `flux:"index"`
		Note  []byte        `flux:"note"`
		Tag   bool          `flux:"b"`
	}{
		Table: 0,
		Start: mustParseTime("2020-02-17T22:19:49.747562847Z"),
		Stop:  mustParseTime("2020-02-18T22:19:49.747562847Z"),
		Time:  mustParseTime("2020-02-18T22:08:44.850214724Z"),
		Took:  time.Hour + 23*time.Minute + 4*time.Second,
		Field: "f",
		Index: 1,
		Note:  []byte("xxxxxccccccddddd"),
		Tag:   false,
	}, s)

	err = res.Decode(s2)
	require.NoError(t, err)

	assert.Equal(t, &struct {
		Table interface{} `flux:"table"`
		Start interface{} `flux:"_start"`
		Stop  interface{} `flux:"_stop"`
		Time  interface{} `flux:"_time"`
		Took  interface{} `flux:"took"`
		Field interface{} `flux:"_field"`
		Index interface{} `flux:"index"`
		Note  interface{} `flux:"note"`
		Tag   interface{} `flux:"b"`
	}{
		Table: uint64(0),
		Start: mustParseTime("2020-02-17T22:19:49.747562847Z"),
		Stop:  mustParseTime("2020-02-18T22:19:49.747562847Z"),
		Time:  mustParseTime("2020-02-18T22:08:44.850214724Z"),
		Took:  time.Hour + 23*time.Minute + 4*time.Second,
		Field: "f",
		Index: int64(1),
		Note:  []byte("xxxxxccccccddddd"),
		Tag:   false,
	}, s2)

	err = res.Decode(s3)
	require.NoError(t, err)

	assert.Equal(t, &struct {
		Table string `flux:"table"`
		Start string `flux:"_start"`
		Stop  string `flux:"_stop"`
		Time  string `flux:"_time"`
		Took  string `flux:"took"`
		Field string `flux:"_field"`
		Index string `flux:"index"`
		Note  string `flux:"note"`
		Tag   string `flux:"b"`
	}{
		Table: "0",
		Start: "2020-02-17T22:19:49.747562847Z",
		Stop:  "2020-02-18T22:19:49.747562847Z",
		Time:  "2020-02-18T22:08:44.850214724Z",
		Took:  "1h23m4s",
		Field: "f",
		Index: "1",
		Note:  "eHh4eHhjY2NjY2NkZGRkZA==",
		Tag:   "false",
	}, s3)

	require.True(t, res.NextSection())
	require.NoError(t, res.Err())
	require.True(t, res.NextRow())
	require.NoError(t, res.Err())

	sn := &struct {
		Index int64     `flux:"index"`
		Time  time.Time `flux:"time"`
		Name  string    `flux:"name"`
		Score float64   `flux:"score"`
		Sum   float64
	}{}

	err = res.Decode(sn)
	require.NoError(t, err)

	assert.Equal(t, &struct {
		Index int64     `flux:"index"`
		Time  time.Time `flux:"time"`
		Name  string    `flux:"name"`
		Score float64   `flux:"score"`
		Sum   float64
	}{
		Index: 0,
		Time:  mustParseTime("2021-02-18T10:34:08.135814545Z"),
		Score: 3.3,
		Name:  "Thomas",
		Sum:   0,
	}, sn)

	sn2 := &struct {
		Index interface{} `flux:"index"`
		Time  interface{} `flux:"time"`
		Name  interface{} `flux:"name"`
		Score interface{} `flux:"score"`
		Sum   interface{}
	}{}

	err = res.Decode(sn2)
	require.NoError(t, err)

	assert.Equal(t, &struct {
		Index interface{} `flux:"index"`
		Time  interface{} `flux:"time"`
		Name  interface{} `flux:"name"`
		Score interface{} `flux:"score"`
		Sum   interface{}
	}{
		Index: int64(0),
		Time:  mustParseTime("2021-02-18T10:34:08.135814545Z"),
		Score: 3.3,
		Name:  "Thomas",
		Sum:   nil,
	}, sn2)

	sn3 := &struct {
		Index string `flux:"index"`
		Time  string `flux:"time"`
		Name  string `flux:"name"`
		Score string `flux:"score"`
		Sum   string
	}{}

	err = res.Decode(sn3)
	require.NoError(t, err)

	assert.Equal(t, &struct {
		Index string `flux:"index"`
		Time  string `flux:"time"`
		Name  string `flux:"name"`
		Score string `flux:"score"`
		Sum   string
	}{
		Index: "0",
		Time:  "2021-02-18T10:34:08.135814545Z",
		Score: "3.3",
		Name:  "Thomas",
		Sum:   "",
	}, sn3)

	require.True(t, res.NextRow())
	require.NoError(t, res.Err())
	err = res.Decode(sn)
	require.NoError(t, err)

	assert.Equal(t, &struct {
		Index int64     `flux:"index"`
		Time  time.Time `flux:"time"`
		Name  string    `flux:"name"`
		Score float64   `flux:"score"`
		Sum   float64
	}{
		Index: 1,
		Time:  mustParseTime("2021-02-18T22:08:44.850214724Z"),
		Score: 5.1,
		Name:  "John",
		Sum:   0,
	}, sn)

	err = res.Decode(sn2)
	require.NoError(t, err)
	assert.Equal(t, &struct {
		Index interface{} `flux:"index"`
		Time  interface{} `flux:"time"`
		Name  interface{} `flux:"name"`
		Score interface{} `flux:"score"`
		Sum   interface{}
	}{
		Index: int64(1),
		Time:  mustParseTime("2021-02-18T22:08:44.850214724Z"),
		Score: 5.1,
		Name:  "John",
		Sum:   nil,
	}, sn2)

	err = res.Decode(sn3)
	require.NoError(t, err)

	assert.Equal(t, &struct {
		Index string `flux:"index"`
		Time  string `flux:"time"`
		Name  string `flux:"name"`
		Score string `flux:"score"`
		Sum   string
	}{
		Index: "1",
		Time:  "2021-02-18T22:08:44.850214724Z",
		Score: "5.1",
		Name:  "John",
		Sum:   "",
	}, sn3)

}

func TestDecodeStructSkipField(t *testing.T) {
	csvTable := `#datatype,long,double,dateTime:RFC3339Nano,string
#default,,,,
,Index,Score,Time,Name
,0,3.3,2021-02-18T10:34:08.135814545Z,Thomas
,1,5.1,2021-02-18T22:08:44.850214724Z,John

`
	reader := strings.NewReader(csvTable)
	res := NewReader(reader)
	require.True(t, res.NextSection())
	require.NoError(t, res.Err())
	require.True(t, res.NextRow())
	require.NoError(t, res.Err())

	// Test decode in struct no tag
	s := &struct {
		Index int64
		Time  time.Time
		Name  string
		Score int `flux:"-"`
	}{}

	err := res.Decode(s)
	require.NoError(t, err)

	assert.Equal(t, &struct {
		Index int64
		Time  time.Time
		Name  string
		Score int `flux:"-"`
	}{
		Index: 0,
		Time:  mustParseTime("2021-02-18T10:34:08.135814545Z"),
		Score: 0,
		Name:  "Thomas",
	}, s)
}

func TestDecodeStructNoTag(t *testing.T) {
	csvTable := `#datatype,long,double,dateTime:RFC3339Nano,string
#default,,,,
,Index,Score,Time,Name
,0,3.3,2021-02-18T10:34:08.135814545Z,Thomas
,1,5.1,2021-02-18T22:08:44.850214724Z,John

`
	reader := strings.NewReader(csvTable)
	res := NewReader(reader)
	require.True(t, res.NextSection())
	require.NoError(t, res.Err())
	require.True(t, res.NextRow())
	require.NoError(t, res.Err())

	// Test decode in struct no tag
	s := &struct {
		Index int64
		Time  time.Time
		Name  string
		Score float64
	}{}

	err := res.Decode(s)
	require.NoError(t, err)

	assert.Equal(t, &struct {
		Index int64
		Time  time.Time
		Name  string
		Score float64
	}{
		Index: 0,
		Time:  mustParseTime("2021-02-18T10:34:08.135814545Z"),
		Score: 3.3,
		Name:  "Thomas",
	}, s)
}

func TestDecodeStructNoMatchedFields(t *testing.T) {
	csvTable := `#datatype,long,double,dateTime:RFC3339Nano,string
#default,,,,
,index,score,time,name
,0,3.3,2021-02-18T10:34:08.135814545Z,Thomas
,1,5.1,2021-02-18T22:08:44.850214724Z,John

`
	reader := strings.NewReader(csvTable)
	res := NewReader(reader)
	require.True(t, res.NextSection())
	require.NoError(t, res.Err())
	require.True(t, res.NextRow())
	require.NoError(t, res.Err())

	// Test decode in struct no tag
	s := &struct {
		Index int64
		Time  time.Time
		Name  string
		Score float64
	}{}

	err := res.Decode(s)
	require.NoError(t, err)

	assert.Equal(t, &struct {
		Index int64
		Time  time.Time
		Name  string
		Score float64
	}{
		Index: 0,
		Time:  time.Time{},
		Score: 0.0,
		Name:  "",
	}, s)

	// Test decode in struct no tag
	sn := &struct {
		Index int64     `flux:"Index"`
		Time  time.Time `flux:"Time"`
		Name  string    `flux:"Name"`
		Score float64   `flux:"Score"`
		Sum   float64
	}{}

	err = res.Decode(sn)
	require.NoError(t, err)

	assert.Equal(t, &struct {
		Index int64     `flux:"Index"`
		Time  time.Time `flux:"Time"`
		Name  string    `flux:"Name"`
		Score float64   `flux:"Score"`
		Sum   float64
	}{
		Index: 0,
		Time:  time.Time{},
		Score: 0.0,
		Name:  "",
	}, sn)
}

func TestDecodeFail(t *testing.T) {
	csvTable := `#datatype,long,double,dateTime:RFC3339Nano,stringer
#default,,,,
,index,score,time,name
,0,3.3,2021-02-18T10:34:08.135814545Z,Thomas
,1,5.1,2021-02-18T22:08:44.850214724Z,John
	
`
	reader := strings.NewReader(csvTable)
	res := NewReader(reader)
	require.True(t, res.NextSection())
	require.NoError(t, res.Err())
	require.True(t, res.NextRow())
	require.NoError(t, res.Err())

	err := res.Decode(map[string]int{})
	require.Error(t, err)

	s := struct {
		Index int64     `flux:"Index"`
		Time  time.Time `flux:"Time"`
		Name  string    `flux:"Name"`
		Score float64   `flux:"Score"`
		Sum   float64
	}{}

	err = res.Decode(s)
	require.Error(t, err)

	var r []float64
	err = res.Decode(r)
	require.Error(t, err)

	err = res.Decode(&r)
	require.Error(t, err)

	var f float64
	err = res.Decode(&f)
	require.Error(t, err)

}

func TestDecodeSliceBasicAllTypes(t *testing.T) {
	csvTable := `#datatype,string,unsignedLong,dateTime:RFC3339,dateTime:RFC3339,dateTime:RFC3339Nano,duration,string,long,base64Binary,boolean
#group,false,false,true,true,false,false,true,true,true,true
#default,_result,,,,,,,,,
,result,table,_start,_stop,_time,took,_field,index,note,b
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T10:34:08.135814545Z,32m,f,-1,ZGF0YWluYmFzZTY0,true
,,0,2020-02-17T22:19:49.747562847Z,2020-02-18T22:19:49.747562847Z,2020-02-18T22:08:44.850214724Z,1h23m4s,f,1,eHh4eHhjY2NjY2NkZGRkZA==,false

#datatype,long,double,dateTime,string
#default,,,,
,index,score,time,name
,0,3.3,2021-02-18T10:34:08.135814545Z,Thomas
,1,5.1,2021-02-18T22:08:44.850214724Z,John

`
	reader := strings.NewReader(csvTable)
	res := NewReader(reader)
	require.True(t, res.NextSection())
	require.NoError(t, res.Err())
	require.True(t, res.NextRow())
	require.NoError(t, res.Err())

	var r []interface{}
	require.NoError(t, res.Decode(&r))
	er := []interface{}{
		"_result",
		uint64(0),
		mustParseTime("2020-02-17T22:19:49.747562847Z"),
		mustParseTime("2020-02-18T22:19:49.747562847Z"),
		mustParseTime("2020-02-18T10:34:08.135814545Z"),
		time.Minute * 32,
		"f",
		int64(-1),
		[]byte("datainbase64"),
		true,
	}
	require.Equal(t, er, r)

	var rs []string
	require.NoError(t, res.Decode(&rs))
	er2 := []string{
		"_result",
		"0",
		"2020-02-17T22:19:49.747562847Z",
		"2020-02-18T22:19:49.747562847Z",
		"2020-02-18T10:34:08.135814545Z",
		"32m",
		"f",
		"-1",
		"ZGF0YWluYmFzZTY0",
		"true",
	}
	require.Equal(t, er2, rs)
}

func TestConversionErrors(t *testing.T) {
	s1 := &struct {
		S []byte
		D time.Duration
		T time.Time
		I int8
		U uint8
		B bool
		F float32
	}{}
	require.Error(t, toBytes(reflect.ValueOf(s1).Elem().FieldByName("S"), "#"))
	require.Error(t, toDuration(reflect.ValueOf(s1).Elem().FieldByName("D"), "#"))
	require.Error(t, toTime(reflect.ValueOf(s1).Elem().FieldByName("D"), "#"))
	require.Error(t, toInt(reflect.ValueOf(s1).Elem().FieldByName("I"), "#"))
	require.Error(t, toInt(reflect.ValueOf(s1).Elem().FieldByName("I"), "1600"))
	require.Error(t, toUint(reflect.ValueOf(s1).Elem().FieldByName("U"), "#"))
	require.Error(t, toUint(reflect.ValueOf(s1).Elem().FieldByName("U"), "1600"))
	require.Error(t, toBool(reflect.ValueOf(s1).Elem().FieldByName("B"), "#"))
	require.Error(t, toFloat(reflect.ValueOf(s1).Elem().FieldByName("F"), "#"))
	require.Error(t, toFloat(reflect.ValueOf(s1).Elem().FieldByName("F"), "1e64"))
}

func TestConversions(t *testing.T) {
	assert.NotNil(t, conversions[conv{stringCol, fieldKind(reflect.String)}])
	assert.Nil(t, conversions[conv{stringCol, fieldKind(reflect.Bool)}])
	assert.Nil(t, conversions[conv{stringCol, fieldKind(reflect.Int)}])
	assert.Nil(t, conversions[conv{stringCol, fieldKind(reflect.Int8)}])
	assert.Nil(t, conversions[conv{stringCol, fieldKind(reflect.Int16)}])
	assert.Nil(t, conversions[conv{stringCol, fieldKind(reflect.Int32)}])
	assert.Nil(t, conversions[conv{stringCol, fieldKind(reflect.Int64)}])
	assert.Nil(t, conversions[conv{stringCol, fieldKind(reflect.Uint)}])
	assert.Nil(t, conversions[conv{stringCol, fieldKind(reflect.Uint8)}])
	assert.Nil(t, conversions[conv{stringCol, fieldKind(reflect.Uint16)}])
	assert.Nil(t, conversions[conv{stringCol, fieldKind(reflect.Uint32)}])
	assert.Nil(t, conversions[conv{stringCol, fieldKind(reflect.Uint64)}])
	assert.Nil(t, conversions[conv{stringCol, fieldKind(reflect.Float32)}])
	assert.Nil(t, conversions[conv{stringCol, fieldKind(reflect.Float64)}])
	assert.Nil(t, conversions[conv{stringCol, durationKind}])
	assert.Nil(t, conversions[conv{stringCol, timeKind}])
	assert.Nil(t, conversions[conv{stringCol, bytesKind}])

	assert.NotNil(t, conversions[conv{boolCol, fieldKind(reflect.String)}])
	assert.NotNil(t, conversions[conv{boolCol, fieldKind(reflect.Bool)}])
	assert.Nil(t, conversions[conv{boolCol, fieldKind(reflect.Int)}])
	assert.Nil(t, conversions[conv{boolCol, fieldKind(reflect.Int8)}])
	assert.Nil(t, conversions[conv{boolCol, fieldKind(reflect.Int16)}])
	assert.Nil(t, conversions[conv{boolCol, fieldKind(reflect.Int32)}])
	assert.Nil(t, conversions[conv{boolCol, fieldKind(reflect.Int64)}])
	assert.Nil(t, conversions[conv{boolCol, fieldKind(reflect.Uint)}])
	assert.Nil(t, conversions[conv{boolCol, fieldKind(reflect.Uint8)}])
	assert.Nil(t, conversions[conv{boolCol, fieldKind(reflect.Uint16)}])
	assert.Nil(t, conversions[conv{boolCol, fieldKind(reflect.Uint32)}])
	assert.Nil(t, conversions[conv{boolCol, fieldKind(reflect.Uint64)}])
	assert.Nil(t, conversions[conv{boolCol, fieldKind(reflect.Float32)}])
	assert.Nil(t, conversions[conv{boolCol, fieldKind(reflect.Float64)}])
	assert.Nil(t, conversions[conv{boolCol, durationKind}])
	assert.Nil(t, conversions[conv{boolCol, timeKind}])
	assert.Nil(t, conversions[conv{boolCol, bytesKind}])

	assert.NotNil(t, conversions[conv{durationCol, fieldKind(reflect.String)}])
	assert.Nil(t, conversions[conv{durationCol, fieldKind(reflect.Bool)}])
	assert.Nil(t, conversions[conv{durationCol, fieldKind(reflect.Int)}])
	assert.Nil(t, conversions[conv{durationCol, fieldKind(reflect.Int8)}])
	assert.Nil(t, conversions[conv{durationCol, fieldKind(reflect.Int16)}])
	assert.Nil(t, conversions[conv{durationCol, fieldKind(reflect.Int32)}])
	assert.Nil(t, conversions[conv{durationCol, fieldKind(reflect.Int64)}])
	assert.Nil(t, conversions[conv{durationCol, fieldKind(reflect.Uint)}])
	assert.Nil(t, conversions[conv{durationCol, fieldKind(reflect.Uint8)}])
	assert.Nil(t, conversions[conv{durationCol, fieldKind(reflect.Uint16)}])
	assert.Nil(t, conversions[conv{durationCol, fieldKind(reflect.Uint32)}])
	assert.Nil(t, conversions[conv{durationCol, fieldKind(reflect.Uint64)}])
	assert.Nil(t, conversions[conv{durationCol, fieldKind(reflect.Float32)}])
	assert.Nil(t, conversions[conv{durationCol, fieldKind(reflect.Float64)}])
	assert.NotNil(t, conversions[conv{durationCol, durationKind}])
	assert.Nil(t, conversions[conv{durationCol, timeKind}])
	assert.Nil(t, conversions[conv{durationCol, bytesKind}])

	assert.NotNil(t, conversions[conv{longCol, fieldKind(reflect.String)}])
	assert.Nil(t, conversions[conv{longCol, fieldKind(reflect.Bool)}])
	assert.NotNil(t, conversions[conv{longCol, fieldKind(reflect.Int)}])
	assert.NotNil(t, conversions[conv{longCol, fieldKind(reflect.Int8)}])
	assert.NotNil(t, conversions[conv{longCol, fieldKind(reflect.Int16)}])
	assert.NotNil(t, conversions[conv{longCol, fieldKind(reflect.Int32)}])
	assert.NotNil(t, conversions[conv{longCol, fieldKind(reflect.Int64)}])
	assert.Nil(t, conversions[conv{longCol, fieldKind(reflect.Uint)}])
	assert.Nil(t, conversions[conv{longCol, fieldKind(reflect.Uint8)}])
	assert.Nil(t, conversions[conv{longCol, fieldKind(reflect.Uint16)}])
	assert.Nil(t, conversions[conv{longCol, fieldKind(reflect.Uint32)}])
	assert.Nil(t, conversions[conv{longCol, fieldKind(reflect.Uint64)}])
	assert.NotNil(t, conversions[conv{longCol, fieldKind(reflect.Float32)}])
	assert.NotNil(t, conversions[conv{longCol, fieldKind(reflect.Float64)}])
	assert.Nil(t, conversions[conv{longCol, durationKind}])
	assert.Nil(t, conversions[conv{longCol, timeKind}])
	assert.Nil(t, conversions[conv{longCol, bytesKind}])

	assert.NotNil(t, conversions[conv{uLongCol, fieldKind(reflect.String)}])
	assert.Nil(t, conversions[conv{uLongCol, fieldKind(reflect.Bool)}])
	assert.NotNil(t, conversions[conv{uLongCol, fieldKind(reflect.Int)}])
	assert.NotNil(t, conversions[conv{uLongCol, fieldKind(reflect.Int8)}])
	assert.NotNil(t, conversions[conv{uLongCol, fieldKind(reflect.Int16)}])
	assert.NotNil(t, conversions[conv{uLongCol, fieldKind(reflect.Int32)}])
	assert.NotNil(t, conversions[conv{uLongCol, fieldKind(reflect.Int64)}])
	assert.NotNil(t, conversions[conv{uLongCol, fieldKind(reflect.Uint)}])
	assert.NotNil(t, conversions[conv{uLongCol, fieldKind(reflect.Uint8)}])
	assert.NotNil(t, conversions[conv{uLongCol, fieldKind(reflect.Uint16)}])
	assert.NotNil(t, conversions[conv{uLongCol, fieldKind(reflect.Uint32)}])
	assert.NotNil(t, conversions[conv{uLongCol, fieldKind(reflect.Uint64)}])
	assert.NotNil(t, conversions[conv{uLongCol, fieldKind(reflect.Float32)}])
	assert.NotNil(t, conversions[conv{uLongCol, fieldKind(reflect.Float64)}])
	assert.Nil(t, conversions[conv{uLongCol, durationKind}])
	assert.Nil(t, conversions[conv{uLongCol, timeKind}])
	assert.Nil(t, conversions[conv{uLongCol, bytesKind}])

	assert.NotNil(t, conversions[conv{doubleCol, fieldKind(reflect.String)}])
	assert.Nil(t, conversions[conv{doubleCol, fieldKind(reflect.Bool)}])
	assert.Nil(t, conversions[conv{doubleCol, fieldKind(reflect.Int)}])
	assert.Nil(t, conversions[conv{doubleCol, fieldKind(reflect.Int8)}])
	assert.Nil(t, conversions[conv{doubleCol, fieldKind(reflect.Int16)}])
	assert.Nil(t, conversions[conv{doubleCol, fieldKind(reflect.Int32)}])
	assert.Nil(t, conversions[conv{doubleCol, fieldKind(reflect.Int64)}])
	assert.Nil(t, conversions[conv{doubleCol, fieldKind(reflect.Uint)}])
	assert.Nil(t, conversions[conv{doubleCol, fieldKind(reflect.Uint8)}])
	assert.Nil(t, conversions[conv{doubleCol, fieldKind(reflect.Uint16)}])
	assert.Nil(t, conversions[conv{doubleCol, fieldKind(reflect.Uint32)}])
	assert.Nil(t, conversions[conv{doubleCol, fieldKind(reflect.Uint64)}])
	assert.NotNil(t, conversions[conv{doubleCol, fieldKind(reflect.Float32)}])
	assert.NotNil(t, conversions[conv{doubleCol, fieldKind(reflect.Float64)}])
	assert.Nil(t, conversions[conv{doubleCol, durationKind}])
	assert.Nil(t, conversions[conv{doubleCol, timeKind}])
	assert.Nil(t, conversions[conv{doubleCol, bytesKind}])

	assert.NotNil(t, conversions[conv{base64BinaryCol, fieldKind(reflect.String)}])
	assert.Nil(t, conversions[conv{base64BinaryCol, fieldKind(reflect.Bool)}])
	assert.Nil(t, conversions[conv{base64BinaryCol, fieldKind(reflect.Int)}])
	assert.Nil(t, conversions[conv{base64BinaryCol, fieldKind(reflect.Int8)}])
	assert.Nil(t, conversions[conv{base64BinaryCol, fieldKind(reflect.Int16)}])
	assert.Nil(t, conversions[conv{base64BinaryCol, fieldKind(reflect.Int32)}])
	assert.Nil(t, conversions[conv{base64BinaryCol, fieldKind(reflect.Int64)}])
	assert.Nil(t, conversions[conv{base64BinaryCol, fieldKind(reflect.Uint)}])
	assert.Nil(t, conversions[conv{base64BinaryCol, fieldKind(reflect.Uint8)}])
	assert.Nil(t, conversions[conv{base64BinaryCol, fieldKind(reflect.Uint16)}])
	assert.Nil(t, conversions[conv{base64BinaryCol, fieldKind(reflect.Uint32)}])
	assert.Nil(t, conversions[conv{base64BinaryCol, fieldKind(reflect.Uint64)}])
	assert.Nil(t, conversions[conv{base64BinaryCol, fieldKind(reflect.Float32)}])
	assert.Nil(t, conversions[conv{base64BinaryCol, fieldKind(reflect.Float64)}])
	assert.Nil(t, conversions[conv{base64BinaryCol, durationKind}])
	assert.Nil(t, conversions[conv{base64BinaryCol, timeKind}])
	assert.NotNil(t, conversions[conv{base64BinaryCol, bytesKind}])

	assert.NotNil(t, conversions[conv{timeColRFC, fieldKind(reflect.String)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Bool)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Int)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Int8)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Int16)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Int32)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Int64)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Uint)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Uint8)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Uint16)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Uint32)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Uint64)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Float32)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Float64)}])
	assert.Nil(t, conversions[conv{timeColRFC, durationKind}])
	assert.NotNil(t, conversions[conv{timeColRFC, timeKind}])
	assert.Nil(t, conversions[conv{timeColRFC, bytesKind}])

	assert.NotNil(t, conversions[conv{timeColRFC, fieldKind(reflect.String)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Bool)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Int)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Int8)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Int16)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Int32)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Int64)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Uint)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Uint8)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Uint16)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Uint32)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Uint64)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Float32)}])
	assert.Nil(t, conversions[conv{timeColRFC, fieldKind(reflect.Float64)}])
	assert.Nil(t, conversions[conv{timeColRFC, durationKind}])
	assert.NotNil(t, conversions[conv{timeColRFC, timeKind}])
	assert.Nil(t, conversions[conv{timeColRFC, bytesKind}])

	assert.NotNil(t, conversions[conv{timeColRFCNano, fieldKind(reflect.String)}])
	assert.Nil(t, conversions[conv{timeColRFCNano, fieldKind(reflect.Bool)}])
	assert.Nil(t, conversions[conv{timeColRFCNano, fieldKind(reflect.Int)}])
	assert.Nil(t, conversions[conv{timeColRFCNano, fieldKind(reflect.Int8)}])
	assert.Nil(t, conversions[conv{timeColRFCNano, fieldKind(reflect.Int16)}])
	assert.Nil(t, conversions[conv{timeColRFCNano, fieldKind(reflect.Int32)}])
	assert.Nil(t, conversions[conv{timeColRFCNano, fieldKind(reflect.Int64)}])
	assert.Nil(t, conversions[conv{timeColRFCNano, fieldKind(reflect.Uint)}])
	assert.Nil(t, conversions[conv{timeColRFCNano, fieldKind(reflect.Uint8)}])
	assert.Nil(t, conversions[conv{timeColRFCNano, fieldKind(reflect.Uint16)}])
	assert.Nil(t, conversions[conv{timeColRFCNano, fieldKind(reflect.Uint32)}])
	assert.Nil(t, conversions[conv{timeColRFCNano, fieldKind(reflect.Uint64)}])
	assert.Nil(t, conversions[conv{timeColRFCNano, fieldKind(reflect.Float32)}])
	assert.Nil(t, conversions[conv{timeColRFCNano, fieldKind(reflect.Float64)}])
	assert.Nil(t, conversions[conv{timeColRFCNano, durationKind}])
	assert.NotNil(t, conversions[conv{timeColRFCNano, timeKind}])
	assert.Nil(t, conversions[conv{timeColRFCNano, bytesKind}])
}

func TestConversionErrorReporting(t *testing.T) {
	csvTable := `#datatype,long,double,dateTime,string
#default,,,,
,index,score,time,name
,0,3.3,2021-02-18T10:34:08.135814545Z,Thomas
,1,5.1,2021-02-18T22:08:44.850214724Z,John

`
	reader := strings.NewReader(csvTable)
	res := NewReader(reader)
	require.True(t, res.NextSection())
	require.True(t, res.NextRow())
	require.NoError(t, res.Err())

	s := &struct {
		Value int `flux:"score"`
	}{}
	err := res.Decode(s)
	require.Error(t, err)
	assert.Equal(t, "cannot convert from column type double to int", err.Error())

	csvTable = `#datatype,long,double,dateTime,string
#default,,,,
,index,score,time,name
,1.0,3.3,2021-02-18T10:34:08.135814545Z,Thomas
,1,5.1,2021-02-18T22:08:44.850214724Z,John

`
	reader = strings.NewReader(csvTable)
	res = NewReader(reader)
	require.True(t, res.NextSection())
	require.True(t, res.NextRow())
	require.NoError(t, res.Err())

	s2 := &struct {
		Index int `flux:"index"`
	}{}
	err = res.Decode(s2)
	require.Error(t, err)
	assert.Equal(t, `cannot convert value "1.0" to type "long" at line 4: strconv.ParseInt: parsing "1.0": invalid syntax`, err.Error())

}

// MustParseTime returns  parsed dateTime in RFC3339 and panics if it fails
func mustParseTime(s string) time.Time {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
