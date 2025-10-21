package etl

import (
	"reflect"

	"github.com/pkg/errors"
)

// TargetData represents data for a single table
type TargetData struct {
	Table   string
	Records any // slice or array
}

// TargetDatas is a slice of Data
type TargetDatas []*TargetData

// Validate validates the TargetDatas
func (datas TargetDatas) Validate() error {
	for _, data := range datas {
		if data.Records != nil {
			v := reflect.ValueOf(data.Records)
			switch v.Kind() {
			case reflect.Slice, reflect.Array:
			default:
				return errors.Errorf("table %s records must be slice or array, got %T", data.Table, data.Records)
			}
		}
	}
	return nil
}

// FilterNonEmpty returns a new TargetDatas containing only tables with non-empty records
// Non-slice/array types are skipped (validation is handled by Validate method)
func (datas TargetDatas) FilterNonEmpty() TargetDatas {
	filtered := make(TargetDatas, 0, len(datas))
	for _, data := range datas {
		if data.Records == nil {
			continue
		}

		rv := reflect.ValueOf(data.Records)
		// Only process slice/array types, skip others
		if rv.Kind() != reflect.Slice && rv.Kind() != reflect.Array {
			continue
		}

		if rv.Len() == 0 {
			continue
		}

		filtered = append(filtered, data)
	}
	return filtered
}
