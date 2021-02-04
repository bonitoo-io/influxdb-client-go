package influxclient

import (
	"encoding/base64"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"
)

// TableColumn holds flux query table column properties
type TableColumn struct {
	Name         string
	DataType     string
	IsGroup      bool
	DefaultValue string
}

// String returns TableColumn string dump
func (f *TableColumn) String() string {
	return fmt.Sprintf("{name: %s, datatype: %s, defaultValue: %s, group: %v}", f.Name, f.DataType, f.DefaultValue, f.IsGroup)
}

const (
	stringDatatype       = "string"
	doubleDatatype       = "double"
	boolDatatype         = "boolean"
	longDatatype         = "long"
	uLongDatatype        = "unsignedLong"
	durationDatatype     = "duration"
	base64BinaryDataType = "base64Binary"
	timeDatatypeRFC      = "dateTime:RFC3339"
	timeDatatypeRFCNano  = "dateTime:RFC3339Nano"
)

type parsingState int
type parsingMode int

const (
	parsingStateNoTable parsingState = iota
	parsingStateDataRow
	parsingStateAnnotation
	parsingStateNameRow
	parsingStateError
)

const (
	parsingModeResult parsingMode = iota
	parsingModeRow
)

// QueryResults parses Flux query response stream.
// Walking though the result set is done by repeatedly calling NextTable() and NextRow() until return false.
// NextRow() can be also called initially, to advance straight to the first row of the first table.
// Actual flux table schema (columns with names, data types, etc) is returned by Columns() method.
// Data are acquired by Values() or by ValueByName() functions.
// Preliminary end can be caused by an error, so when NextTable() or NextRow() return false, check Err() for an error.
// Reader is automatically closed at the end reading or in case of an error.
type QueryResults struct {
	io.Closer
	csvReader *csv.Reader
	columns   []TableColumn
	values    []interface{}
	err       error
	lastRow   []string
	names     map[string]int
}

// NewQueryResults returns new QueryResults.
// Reader must source csv lines with Flux query response.
func NewQueryResults(reader io.ReadCloser) *QueryResults {
	csvReader := csv.NewReader(reader)
	csvReader.FieldsPerRecord = -1
	return &QueryResults{
		Closer:    reader,
		csvReader: csvReader,
		names:     map[string]int{},
	}
}

// NextTable advances to the next table in the query response.
// Any remaining data in the current table is discarded.
// When there are no more tables, it returns false.
func (r *QueryResults) NextTable() bool {
	return r.next(parsingModeResult)
}

// NextRow advances to the next row in the current table.
// If called in the beginning, it also advances to the first table.
// When there are no more rows in the current table, it returns false.
func (r *QueryResults) NextRow() bool {
	return r.next(parsingModeRow)
}

// Columns returns information on the columns in the current
// table. It returns nil if there is no current table (for example
// before NextTable has been called, or after NextTable returns false).
func (r *QueryResults) Columns() []TableColumn {
	return r.columns
}

// Err returns any error encountered. This should be called after NextTable or NextRow
// returns false to check that all the results were correctly received.
func (r *QueryResults) Err() error {
	return r.err
}

// Values returns the values in the current row.
// It returns nil if there is no current row.
// All rows in a table have the same number of values.
// The caller should not use the slice after NextRow
// has been called again, because it's re-used.
func (r *QueryResults) Values() []interface{} {
	return r.values
}

// ValueByName returns value for given column name.
// It returns nil if table has no value for such column.
func (r *QueryResults) ValueByName(name string) interface{} {
	if i, ok := r.names[name]; ok {
		return r.values[i]
	}
	return nil
}

// next advances to next row/table via parsing csv rows.
// It returns false at the end of each table  or at the end of the stream.
func (r *QueryResults) next(mode parsingMode) bool {
	// set closing query in case of preliminary return
	var row []string
	closer := func() {
		r.columns = nil
		r.values = nil
		r.lastRow = nil
		r.names = map[string]int{}
		if err := r.Close(); err != nil {
			message := err.Error()
			if r.err != nil {
				message = fmt.Sprintf("%s,%s", message, r.err.Error())
			}
			r.err = errors.New(message)
		}
	}
	defer func() { closer() }()
	parsingState := parsingStateDataRow
	dataTypeAnnotationFound := false
readRow:
	if r.lastRow != nil {
		row = r.lastRow
		r.lastRow = nil
	} else {
		row, r.err = r.csvReader.Read()
	}
	if r.err == io.EOF {
		r.err = nil
		return false
	}
	if r.err != nil {
		return false
	}

	if len(row) <= 1 {
		goto readRow
	}
	if r.columns == nil {
		parsingState = parsingStateNoTable
	} else {
		// test columns consistency in case of data row or already discovered annotations
		if (row[0] == "" || parsingState == parsingStateAnnotation) && len(row)-1 != len(r.columns) {
			r.err = fmt.Errorf("parsing error, row has different number of columns than the table: %d vs %d", len(row)-1, len(r.columns))
			return false
		}
	}
	switch {
	case row[0] == "":
		switch parsingState {
		case parsingStateNoTable:
			r.err = errors.New("parsing error, annotations not found")
			return false
		case parsingStateAnnotation:
			if !dataTypeAnnotationFound {
				r.err = errors.New("parsing error, datatype annotation not found")
				return false
			}
			parsingState = parsingStateNameRow
			fallthrough
		case parsingStateNameRow:
			if row[1] == "error" {
				parsingState = parsingStateError
			} else {
				r.names = map[string]int{}
				for i, n := range row[1:] {
					r.columns[i].Name = n
					r.names[n] = i
				}
				parsingState = parsingStateDataRow
			}
			goto readRow
		case parsingStateError:
			var message string
			if len(row) > 1 && len(row[1]) > 0 {
				message = row[1]
			} else {
				message = "unknown query error"
			}
			reference := ""
			if len(row) > 2 && len(row[2]) > 0 {
				reference = fmt.Sprintf(",%s", row[2])
			}
			r.err = fmt.Errorf("%s%s", message, reference)
			return false
		case parsingStateDataRow:
			if mode == parsingModeResult {
				// if it is first data row after parsing header, stop
				if dataTypeAnnotationFound {
					r.lastRow = row
				} else {
					//skip to next table
					goto readRow
				}
			} else {

				for i, v := range row[1:] {
					r.values[i], r.err = toValue(v, r.columns[i])
					if r.err != nil {
						return false
					}
				}
			}
		}
	case row[0][0] == '#':
		switch parsingState {
		case parsingStateNoTable:
			parsingState = parsingStateDataRow
			fallthrough
		case parsingStateDataRow:
			// table definition was found. if next row is requested, and not the initial table, return
			if mode == parsingModeRow && r.columns != nil {
				r.lastRow = row
				closer = func() {}
				return false
			}
			if r.columns == nil || len(r.columns) != len(row)-1 {
				r.columns = make([]TableColumn, len(row)-1)
				r.values = make([]interface{}, len(row)-1)
			} else {
				for i := range row[1:] {
					r.columns[i] = TableColumn{}
				}
			}
			parsingState = parsingStateAnnotation
			fallthrough
		case parsingStateAnnotation:
			switch row[0] {
			case "#datatype":
				dataTypeAnnotationFound = true
				for i, d := range row[1:] {
					r.columns[i].DataType = d
				}
				goto readRow
			case "#group":
				for i, g := range row[1:] {
					r.columns[i].IsGroup = g == "true"
				}
				goto readRow
			case "#default":
				for i, c := range row[1:] {
					r.columns[i].DefaultValue = c
				}
				goto readRow
			}
		}
	}
	// don't close query
	closer = func() {}
	return true
}

// toValues converts s into type by t
func toValue(val string, col TableColumn) (interface{}, error) {
	if val == "" {
		val = col.DefaultValue
	}
	if val == "" {
		return nil, nil
	}
	switch col.DataType {
	case stringDatatype:
		return val, nil
	case timeDatatypeRFC:
		return time.Parse(time.RFC3339, val)
	case timeDatatypeRFCNano:
		return time.Parse(time.RFC3339Nano, val)
	case durationDatatype:
		return time.ParseDuration(val)
	case doubleDatatype:
		return strconv.ParseFloat(val, 64)
	case boolDatatype:
		if strings.ToLower(val) == "false" {
			return false, nil
		}
		return true, nil
	case longDatatype:
		return strconv.ParseInt(val, 10, 64)
	case uLongDatatype:
		return strconv.ParseUint(val, 10, 64)
	case base64BinaryDataType:
		return base64.StdEncoding.DecodeString(val)
	default:
		return nil, fmt.Errorf("%v has unknown data type %v", col.Name, col.DataType)
	}
}
