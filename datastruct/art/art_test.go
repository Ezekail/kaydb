package art

import (
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
)

func TestAdaptiveRadixTree_PrefixScan(t *testing.T) {
	// 先初始化一些数据
	art := NewART()
	art.Put([]byte("acse"), 123)
	art.Put([]byte("cced"), 123)
	art.Put([]byte("acde"), 123)
	art.Put([]byte("bbfe"), 123)
	art.Put([]byte("bbfc"), 123)
	art.Put([]byte("eefs"), 123)

	keys1 := art.PrefixScan([]byte("bbf"), -1)
	assert.Equal(t, 0, len(keys1))

	keys2 := art.PrefixScan(nil, 0)
	assert.Equal(t, 0, len(keys2))

	keys3 := art.PrefixScan([]byte("b"), 1)
	assert.Equal(t, 1, len(keys3))

	keys4 := art.PrefixScan(nil, 6)
	assert.Equal(t, 6, len(keys4))
}

func TestAdaptiveRadixTree_Put(t *testing.T) {
	art := NewART()
	type args struct {
		key   []byte
		value interface{}
	}
	tests := []struct {
		name        string
		art         *AdaptiveRadixTree
		args        args
		wantOldVal  interface{}
		wantUpdated bool
	}{
		{"nil", art, args{key: nil, value: nil}, nil, false},
		{"normal-1", art, args{key: []byte("1"), value: 11}, nil, false},
		{"normal-2", art, args{key: []byte("1"), value: 22}, 11, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotOldValue, gotUpdated := art.Put(tt.args.key, tt.args.value)
			if !reflect.DeepEqual(gotOldValue, tt.wantOldVal) {
				t.Errorf("Put() gotOldValue = %v want = %v", gotOldValue, tt.wantOldVal)
			}
			if gotUpdated != tt.wantUpdated {
				t.Errorf("Put() gotUpdated = %v, want %v", gotUpdated, tt.wantUpdated)
			}
		})
	}
}

func TestAdaptiveRadixTree_Get(t *testing.T) {
	// 初始化一些数据
	art := NewART()
	art.Put(nil, nil)
	art.Put([]byte("0"), 0)
	art.Put([]byte("11"), 11)
	art.Put([]byte("11"), "rewrite-data")

	type args struct {
		key []byte
	}
	tests := []struct {
		name string
		art  *AdaptiveRadixTree
		args args
		want interface{}
	}{
		{"nil", art, args{key: nil}, nil},
		{"zero", art, args{key: []byte("0")}, 0},
		{"rewrite-data", art, args{key: []byte("11")}, "rewrite-data"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := art.Get(tt.args.key); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAdaptiveRadixTree_Delete(t *testing.T) {
	art := NewART()
	art.Put(nil, nil)
	art.Put([]byte("0"), 0)
	art.Put([]byte("11"), 11)
	art.Put([]byte("11"), "rewrite-data")

	type args struct {
		key []byte
	}
	tests := []struct {
		name        string
		art         *AdaptiveRadixTree
		args        args
		wantVal     interface{}
		wantUpdated bool
	}{
		{"nil", art, args{key: nil}, nil, false},
		{"zero", art, args{key: []byte("0")}, 0, true},
		{"rewrite-data", art, args{key: []byte("11")}, "rewrite-data", true},
		{"not-exist", art, args{key: []byte("not-exist")}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotVal, gotUpdated := art.Delete(tt.args.key)
			if !reflect.DeepEqual(gotVal, tt.wantVal) {
				t.Errorf("Delete() gotVal = %v, want = %v", gotVal, tt.wantVal)
			}
			if gotUpdated != tt.wantUpdated {
				t.Errorf("Delete() gotUpdated = %v, want = %v", gotUpdated, tt.wantUpdated)
			}
		})
	}
}
