package kvcache

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"strconv"
	"sync"
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
	ErrIntOverflow     = errors.New("int64 overflow")
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

// how often the background cleaner removes expired keys
const cleanupInterval = 5 * time.Minute

type Store struct {
	mu   sync.RWMutex
	data map[string]Point
	vs   int64

	stop      chan struct{}  // closed by Close() to tell the cleaner to exit
	wg        sync.WaitGroup // lets Close() wait until the cleaner has really exited
	closeOnce sync.Once      // makes Close() safe to call more than once
}

func (kv *Store) Get(key string) (Point, error) {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	e, ok := kv.data[key]
	if !ok || kv.expired(e) {
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

	kv.mu.Lock()
	defer kv.mu.Unlock()
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

	kv.mu.Lock()
	defer kv.mu.Unlock()
	curr, ok := kv.data[key]
	if ok && kv.expired(curr) {
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
	KV := &Store{
		data: make(map[string]Point),
		stop: make(chan struct{}),
	}

	KV.wg.Go(func() { KV.cleaner(cleanupInterval) })

	return KV
}

// Incr adds 1 to an int key and returns the new value.
// A missing or expired key starts at 0 (so the result is 1, no TTL).
// The existing TTL is kept, like Redis INCR.
func (kv *Store) Incr(key string) (int64, error) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	e, ok := kv.data[key]
	if ok && kv.expired(e) {

		e, ok = Point{}, false
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
	if n == math.MaxInt64 {
		return 0, ErrIntOverflow
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

func (kv *Store) Delete(key string) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	e, ok := kv.data[key]
	delete(kv.data, key)
	return ok && !kv.expired(e)
}

// cleaner runs in its own goroutine: every interval it removes expired keys,
// until Close() closes kv.stop.
func (kv *Store) cleaner(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			kv.removeExpired()
		case <-kv.stop:
			return
		}
	}
}

// removeExpired deletes every expired key. The whole scan holds the write lock:
// ranging over the map is a read and must not overlap with Put/Incr/etc., and
// deciding "expired" and deleting must happen without a gap, or a fresh Put
// in between could get deleted.
func (kv *Store) removeExpired() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for k, val := range kv.data {
		if kv.expired(val) {
			delete(kv.data, k) // deleting while ranging over a map is allowed in Go
		}
	}
}

// Close stops the cleaner and waits until it has exited. Safe to call twice.
func (kv *Store) Close() {
	kv.closeOnce.Do(func() { close(kv.stop) })
	kv.wg.Wait()
}
