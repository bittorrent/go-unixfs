package util

import (
	"github.com/warpfork/go-wish/cmp"
	"testing"
)

func testMapIntersects(t *testing.T) {
	intersectTestTable := map[string]struct {
		input1 		map[string]interface{}
		input2 		map[string]interface{}
		want  		bool
	}{
		"partial":
		{input1: map[string]interface{}{"key1": 123, "key2": "val2"},
			input2: map[string]interface{}{"key1": 3},
			want: true, },
		"equal":
		{input1: map[string]interface{}{"key1": 123, "key2": "val2"},
			input2: map[string]interface{}{"key1": 123, "key2": "val2"},
			want: true},
		"empty":
		{input1: map[string]interface{}{"key1": 123, "key2": "val2"},
			input2: map[string]interface{}{"key3": 123, "key4": "val2"},
			want: false},
	}

	for name, td := range intersectTestTable {
		t.Run(name, func(t *testing.T) {
			got := Intersects(td.input1, td.input2)
			diff := cmp.Diff(td.want, got)
			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func testKeyIntersects(t *testing.T) {
	keyIntersectTestTable := map[string]struct {
		input1 		map[string]interface{}
		input2 		map[string]interface{}
		want  		bool
	}{
		"partial":
		{input1: map[string]interface{}{"key1": 123, "key2": "val2"},
			input2: map[string]interface{}{"key1": 3},
			want: true, },
		"equal":
		{input1: map[string]interface{}{"key1": "", "key2": "val2"},
			input2: map[string]interface{}{"key1": 123, "key2": "val2"},
			want: true},
		"empty":
		{input1: map[string]interface{}{"key1": 123, "key2": "val2"},
			input2: map[string]interface{}{"key3": 123, "key4": "val2"},
			want: false},
	}

	for name, td := range keyIntersectTestTable {
		t.Run(name, func(t *testing.T) {
			got := Intersects(td.input1, td.input2)
			diff := cmp.Diff(td.want, got)
			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func testEqualKeySets(t *testing.T) {
	equalKeySetTestTable := map[string]struct {
		input1 		map[string]interface{}
		input2 		[]string
		want  		bool
	}{
		"partial":
		{input1: map[string]interface{}{"key1": 123, "key2": "val2"},
			input2: []string{"key1"},
			want: false, },
		"equal":
		{input1: map[string]interface{}{"key1": "", "key2": "val2"},
			input2: []string{"key1", "key2"},
			want: true},
		"empty":
		{input1: map[string]interface{}{"key1": 123, "key2": "val2"},
			input2: []string{"key3", "key4"},
			want: false},
	}

	for name, td := range equalKeySetTestTable {
		t.Run(name, func(t *testing.T) {
			got := EqualKeySets(td.input1, td.input2)
			diff := cmp.Diff(td.want, got)
			if diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}


func TestIntersects(t *testing.T) {
	testMapIntersects(t)
	testKeyIntersects(t)
	testEqualKeySets(t)
}
