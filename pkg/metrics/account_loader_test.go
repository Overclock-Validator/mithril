package metrics

import (
	"reflect"
	"testing"
)

// Exercise every field so adding a metric without merging it is caught.
func TestAccountLoaderAccumulateAllFields(t *testing.T) {
	var src AccountLoader
	var fill func(reflect.Value)
	fill = func(v reflect.Value) {
		for i := 0; i < v.NumField(); i++ {
			f := v.Field(i)
			if f.Kind() == reflect.Struct {
				fill(f)
			} else {
				f.SetUint(uint64(i + 1))
			}
		}
	}
	fill(reflect.ValueOf(&src).Elem())
	var dst AccountLoader
	dst.Accumulate(src)
	if dst != src {
		t.Fatal("first merge lost fields")
	}
	dst.Accumulate(src)
	var check func(reflect.Value, reflect.Value)
	check = func(a, b reflect.Value) {
		for i := 0; i < a.NumField(); i++ {
			if a.Field(i).Kind() == reflect.Struct {
				check(a.Field(i), b.Field(i))
			} else if a.Field(i).Uint() != 2*b.Field(i).Uint() {
				t.Errorf("field %s not accumulated", a.Type().Field(i).Name)
			}
		}
	}
	check(reflect.ValueOf(dst), reflect.ValueOf(src))
}
