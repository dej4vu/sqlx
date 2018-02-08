package sqlx

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strings"
	"sync"
)

// Alternatively for a custom mapping, any func(string) string can be used instead.
var NameMapper = ToSnakeCase

var ZeroIsNull = true

var finfos map[reflect.Type]fieldInfo

type fieldInfo map[string][]int

var finfoLock sync.RWMutex

var TagName = "db"

func init() {
	finfos = make(map[reflect.Type]fieldInfo)
}

// getFieldInfo creates a fieldInfo for the provided type. Fields that are not tagged
// with the "sql" tag and unexported fields are not included.
func getFieldInfo(typ reflect.Type) fieldInfo {
	finfoLock.RLock()
	finfo, ok := finfos[typ]
	finfoLock.RUnlock()
	if ok {
		return finfo
	}

	finfo = make(fieldInfo)

	n := typ.NumField()
	for i := 0; i < n; i++ {
		f := typ.Field(i)
		tag := f.Tag.Get(TagName)

		// Skip unexported fields or fields marked with "-"
		if f.PkgPath != "" || tag == "-" {
			continue
		}

		// Handle embedded structs
		if f.Anonymous && f.Type.Kind() == reflect.Struct {
			for k, v := range getFieldInfo(f.Type) {
				finfo[k] = append([]int{i}, v...)
			}
			continue
		}

		// Use field name for untagged fields
		if tag == "" {
			tag = f.Name
		}
		// Trim tag info
		if idx := strings.Index(tag, ","); idx > 0 {
			tag = tag[:idx]
		}
		tag = NameMapper(tag)

		finfo[tag] = []int{i}
	}

	finfoLock.Lock()
	finfos[typ] = finfo
	finfoLock.Unlock()

	return finfo
}

// ScanStruct a single Row into dst
// The dst must be type of point to struct
func ScanStruct(rows *sql.Rows, dst interface{}) error {
	return scanRow(dst, rows)
}

// ScanMap a single row into dst
func ScanMap(rows *sql.Rows, dst map[string]interface{}) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	values := make([]interface{}, len(columns))
	for i, _ := range values {
		values[i] = new(interface{})
	}
	err = rows.Scan(values...)
	if err != nil {
		return err
	}
	for i, column := range columns {
		dst[column] = *(values[i].(*interface{}))
	}
	return rows.Err()
}

func scanRow(dst interface{}, rows *sql.Rows) error {
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	targets, err := getTargets(dst, columns)
	if err != nil {
		return err
	}
	if err = rows.Scan(targets...); err != nil {
		return err
	}
	if err = setTargets(dst, columns, targets); err != nil {
		return err
	}
	return rows.Err()
}

func zeroFill(addr interface{}) interface{} {
	return reflect.New(reflect.TypeOf(addr)).Interface()
}

func getTargets(dst interface{}, columns []string) ([]interface{}, error) {
	dstVal := reflect.ValueOf(dst)
	dstTyp := dstVal.Type()

	if dstTyp.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("sqlx: called with non-pointer destination %v", dstTyp)
	}
	if dstTyp.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("sqlx: called with pointer to non-struct %v", dstTyp)
	}
	fieldInfo := getFieldInfo(dstTyp.Elem())
	elem := dstVal.Elem()
	var values []interface{}
	for _, name := range columns {
		var v interface{}
		idx, ok := fieldInfo[strings.ToLower(name)]
		if !ok {
			log.Printf("sql: column [%s] not found in struct", name)
			v = &sql.RawBytes{}
		} else {
			v = elem.FieldByIndex(idx).Addr().Interface()
			if ZeroIsNull {
				v = zeroFill(v)
			}
		}
		values = append(values, v)
	}
	return values, nil
}

func setTargets(dst interface{}, columns []string, targets []interface{}) error {
	if len(columns) != len(targets) {
		return fmt.Errorf("sqlx: mismatch in number of columns (%d) and targets (%d)",
			len(columns), len(targets))

	}

	dstVal := reflect.ValueOf(dst)
	dstTyp := dstVal.Type()
	fieldInfo := getFieldInfo(dstTyp.Elem())
	elem := dstVal.Elem()
	for i, name := range columns {
		idx, ok := fieldInfo[strings.ToLower(name)]
		if !ok {
			log.Printf("sqlx: column [%s] not found in struct", name)
		} else {
			sv := reflect.ValueOf(targets[i])
			if sv.Elem().IsNil() {
				// null column, so set target to be zero value
				elem.FieldByIndex(idx).Set(reflect.Zero(elem.FieldByIndex(idx).Type()))
			} else {
				elem.FieldByIndex(idx).Set(sv.Elem().Elem())
			}
		}
	}
	return nil
}

func ToSnakeCase(src string) string {
	thisUpper := false
	prevUpper := false
	buf := bytes.NewBufferString("")
	for i, v := range src {
		if v >= 'A' && v <= 'Z' {
			thisUpper = true
		} else {
			thisUpper = false
		}
		if i > 0 && thisUpper && !prevUpper {
			buf.WriteRune('_')
		}
		prevUpper = thisUpper
		buf.WriteRune(v)
	}
	return strings.ToLower(buf.String())
}
