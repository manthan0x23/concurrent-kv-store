package kvcache

import (
	"bytes"
	"encoding/json"
	"errors"
	"strconv"
	"time"
)

type ValueType int

var (
	ErrNotFound        = errors.New("key not found")
	ErrNotInt          = errors.New("value is not an int")
	ErrDuplicateKey    = errors.New("key already present")
	ErrTypeMismatch    = errors.New("content does not match value type")
	ErrVersionMismatch = errors.New("version does not match expected version value")
	ErrUnknownType     = errors.New("unknown value type")
)

const (
	TypeString ValueType = iota // 0
	TypeInt
	TypeJson
	TypeFloat
)

type Point struct {
	Type      ValueType
	Content   []byte
	Version   int64
	ExpiresAt time.Time
}

type Store struct {
	data map[string]Point
	vs   int64
}

func (kv *Store) Get(key string) (Point, error) {
	e, ok := kv.data[key]
	if !ok || kv.expired(e) {
		delete(kv.data, key)
		return Point{}, ErrNotFound
	}

	c := make([]byte, len(e.Content))
	copy(c, e.Content)
	e.Content = c
	return e, nil
}

func encode(content any, vtype ValueType) ([]byte, error) {
	var data []byte

	switch vtype {
	case TypeString:
		s, ok := content.(string)
		if !ok {
			return nil, ErrTypeMismatch
		}
		data = []byte(s)

	case TypeInt:
		n, ok := content.(int64)
		if !ok {
			return nil, ErrTypeMismatch
		}
		data = []byte(strconv.FormatInt(n, 10))

	case TypeJson:
		b, ok := content.([]byte)
		if !ok || !json.Valid(b) {
			return nil, ErrTypeMismatch
		}
		data = bytes.Clone(b)

	case TypeFloat:
		f, ok := content.(float64)
		if !ok {
			return nil, ErrTypeMismatch
		}
		data = []byte(strconv.FormatFloat(f, 'f', -1, 64))

	default:
		return nil, ErrUnknownType
	}

	return data, nil
}

func (kv *Store) Put(key string, content any, vtype ValueType, ttl time.Duration) (int64, error) {

	data, err := encode(content, vtype)

	if err != nil {
		return 0, err
	}

	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}

	kv.vs++
	kv.data[key] = Point{
		Type:      vtype,
		Content:   data,
		Version:   kv.vs,
		ExpiresAt: exp,
	}
	return kv.vs, nil
}

func (kv *Store) expired(e Point) bool {
	return !e.ExpiresAt.IsZero() && !time.Now().Before(e.ExpiresAt)
}

// CAS style
func (kv *Store) CompareAndPut(key string, expectedVersion int64, content any, vtype ValueType, ttl time.Duration) (int64, error) {
	data, err := encode(content, vtype)
	if err != nil {
		return 0, err
	}

	curr, ok := kv.data[key]
	if ok && kv.expired(curr) {
		delete(kv.data, key)
		ok = false
	}
	if !ok {
		return 0, ErrNotFound
	}
	if curr.Version != expectedVersion {
		return 0, ErrVersionMismatch
	}

	var exp time.Time
	if ttl > 0 {
		exp = time.Now().Add(ttl)
	}

	kv.vs++
	kv.data[key] = Point{Type: vtype, Content: data, Version: kv.vs, ExpiresAt: exp}
	return kv.vs, nil
}

func New() *Store {
	return &Store{
		data: make(map[string]Point),
	}
}

// Incr adds 1 to an int key and returns the new value.
// A missing or expired key starts at 0 (so the result is 1, no TTL).
// The existing TTL is kept, like Redis INCR.
func (kv *Store) Incr(key string) (int64, error) {
	e, ok := kv.data[key]
	if ok && kv.expired(e) {
		delete(kv.data, key)
		e, ok = Point{}, false // drop the old deadline too
	}

	var n int64
	if ok {
		if e.Type != TypeInt {
			return 0, ErrNotInt
		}
		var err error
		n, err = strconv.ParseInt(string(e.Content), 10, 64)
		if err != nil {
			return 0, ErrNotInt
		}
	}
	n++

	kv.vs++
	kv.data[key] = Point{
		Type:      TypeInt,
		Content:   []byte(strconv.FormatInt(n, 10)),
		Version:   kv.vs,
		ExpiresAt: e.ExpiresAt, // zero value if the key was missing
	}
	return n, nil
}
