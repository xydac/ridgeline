package parquet

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"time"
	"unicode"

	pq "github.com/parquet-go/parquet-go"

	"github.com/xydac/ridgeline/connectors"
)

// typedWriter writes rows using a parquet schema derived from a
// connector-declared Schema. Stable fields become typed columns;
// everything else still goes through data_json so queries written
// against the untyped sink keep working.
type typedWriter struct {
	w       *pq.Writer
	rowType reflect.Type
	columns []connectors.Column
	fields  map[string]int // declared column name to field index in rowType
}

// newTypedWriter builds a typed writer for the given schema. The
// returned writer's row struct has fields in this order:
//
//	stream (string, required)
//	timestamp (int64, required)
//	one field per schema column, typed per its ColumnType (optional)
//	data_json (string, required)
//
// The declared fields are pointer-typed so a record that omits a
// field writes a null cell rather than a zero value.
func newTypedWriter(out io.Writer, schema connectors.Schema) (*typedWriter, error) {
	fields := []reflect.StructField{
		{Name: "Stream", Type: reflect.TypeOf(""), Tag: `parquet:"stream,snappy"`},
		{Name: "Timestamp", Type: reflect.TypeOf(int64(0)), Tag: `parquet:"timestamp,snappy"`},
	}
	idx := map[string]int{}
	for _, col := range schema.Columns {
		goType, err := goTypeFor(col.Type)
		if err != nil {
			return nil, fmt.Errorf("column %q: %w", col.Name, err)
		}
		goName := exportedFieldName(col.Name)
		if goName == "" {
			return nil, fmt.Errorf("column %q: empty name", col.Name)
		}
		tag := fmt.Sprintf(`parquet:"%s,optional,snappy"`, col.Name)
		fields = append(fields, reflect.StructField{
			Name: goName,
			Type: reflect.PointerTo(goType),
			Tag:  reflect.StructTag(tag),
		})
		idx[col.Name] = len(fields) - 1
	}
	fields = append(fields, reflect.StructField{
		Name: "DataJSON",
		Type: reflect.TypeOf(""),
		Tag:  `parquet:"data_json,snappy"`,
	})
	rowType := reflect.StructOf(fields)
	w := pq.NewWriter(out)
	return &typedWriter{w: w, rowType: rowType, columns: schema.Columns, fields: idx}, nil
}

// writeRow marshals one Record into an instance of the row struct and
// appends it. dataJSON is the already-encoded overflow payload.
func (t *typedWriter) writeRow(stream string, ts time.Time, data map[string]any, dataJSON string) error {
	v := reflect.New(t.rowType).Elem()
	v.FieldByName("Stream").SetString(stream)
	v.FieldByName("Timestamp").SetInt(ts.UTC().UnixMicro())
	for _, col := range t.columns {
		fv := v.Field(t.fields[col.Name])
		raw, ok := data[col.Name]
		if !ok || raw == nil {
			continue // leave nil pointer for optional
		}
		typed, err := coerce(raw, col.Type)
		if err != nil {
			return fmt.Errorf("column %q: %w", col.Name, err)
		}
		ptr := reflect.New(fv.Type().Elem())
		ptr.Elem().Set(reflect.ValueOf(typed))
		fv.Set(ptr)
	}
	v.FieldByName("DataJSON").SetString(dataJSON)
	return t.w.Write(v.Interface())
}

func (t *typedWriter) Flush() error { return t.w.Flush() }
func (t *typedWriter) Close() error { return t.w.Close() }

// goTypeFor maps a ColumnType to the Go type used in the runtime row
// struct. Timestamp is stored as int64 microseconds to match the
// existing Timestamp column's encoding.
func goTypeFor(ct connectors.ColumnType) (reflect.Type, error) {
	switch ct {
	case connectors.String, connectors.JSON:
		return reflect.TypeOf(""), nil
	case connectors.Int:
		return reflect.TypeOf(int64(0)), nil
	case connectors.Float:
		return reflect.TypeOf(float64(0)), nil
	case connectors.Bool:
		return reflect.TypeOf(false), nil
	case connectors.Timestamp:
		return reflect.TypeOf(int64(0)), nil
	}
	return nil, fmt.Errorf("unsupported column type %v", ct)
}

// coerce converts a value from a connector's Data map into the Go
// type the typed row expects. It accepts the common JSON-decoded
// shapes (float64 for all numerics, bool, string) and returns a
// descriptive error on a type mismatch so a misdeclared schema fails
// fast instead of silently writing garbage.
func coerce(raw any, ct connectors.ColumnType) (any, error) {
	switch ct {
	case connectors.String, connectors.JSON:
		if s, ok := raw.(string); ok {
			return s, nil
		}
		return nil, fmt.Errorf("want string, got %T", raw)
	case connectors.Int:
		switch v := raw.(type) {
		case int:
			return int64(v), nil
		case int32:
			return int64(v), nil
		case int64:
			return v, nil
		case float32:
			return int64(v), nil
		case float64:
			return int64(v), nil
		}
		return nil, fmt.Errorf("want int, got %T", raw)
	case connectors.Float:
		switch v := raw.(type) {
		case float32:
			return float64(v), nil
		case float64:
			return v, nil
		case int:
			return float64(v), nil
		case int64:
			return float64(v), nil
		}
		return nil, fmt.Errorf("want float, got %T", raw)
	case connectors.Bool:
		if b, ok := raw.(bool); ok {
			return b, nil
		}
		return nil, fmt.Errorf("want bool, got %T", raw)
	case connectors.Timestamp:
		switch v := raw.(type) {
		case time.Time:
			return v.UTC().UnixMicro(), nil
		case int64:
			return v, nil
		case float64:
			return int64(v), nil
		case string:
			tt, err := time.Parse(time.RFC3339Nano, v)
			if err != nil {
				return nil, fmt.Errorf("parse timestamp %q: %w", v, err)
			}
			return tt.UTC().UnixMicro(), nil
		}
		return nil, fmt.Errorf("want timestamp, got %T", raw)
	}
	return nil, fmt.Errorf("unsupported column type %v", ct)
}

// exportedFieldName returns an exported Go identifier derived from
// name. Non-letter/digit runes become underscores, a leading digit
// gets a leading "F_", and the first letter is uppercased so
// reflect.StructOf accepts the resulting field.
func exportedFieldName(name string) string {
	if name == "" {
		return ""
	}
	var b strings.Builder
	for i, r := range name {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			if i == 0 {
				if unicode.IsDigit(r) {
					b.WriteString("F_")
				}
				b.WriteRune(unicode.ToUpper(r))
				continue
			}
			b.WriteRune(r)
		default:
			b.WriteRune('_')
		}
	}
	return b.String()
}
