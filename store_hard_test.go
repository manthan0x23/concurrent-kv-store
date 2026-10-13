package kvcache

import (
	"errors"
	"fmt"
	"math"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Run with:  go test -race -count=1 -v ./...
//
// TTL tests use real time (the store calls time.Now directly), so they sleep a few ms.
// If you add a fake clock later, replace the sleeps with clock jumps.

const shortTTL = 20 * time.Millisecond

func waitExpire() { time.Sleep(2 * shortTTL) }

func newStore(t *testing.T) *Store {
	t.Helper()
	kv := New()
	t.Cleanup(kv.Close)
	return kv
}

func mustPut(t *testing.T, kv *Store, key string, content any, vt ValueType, ttl time.Duration) int64 {
	t.Helper()
	v, err := kv.Put(key, content, vt, ttl)
	if err != nil {
		t.Fatalf("Put(%q): unexpected error %v", key, err)
	}
	return v
}

func mustGet(t *testing.T, kv *Store, key string) Point {
	t.Helper()
	p, err := kv.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): unexpected error %v", key, err)
	}
	return p
}

// ───────────────────────── Get / Put ─────────────────────────

func TestGetMissing(t *testing.T) {
	kv := newStore(t)
	if _, err := kv.Get("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("err = %v; want ErrNotFound", err)
	}
}

func TestPutAllTypes(t *testing.T) {
	kv := newStore(t)
	tests := []struct {
		name    string
		content any
		vt      ValueType
		want    string
	}{
		{"string", "hello", TypeString, "hello"},
		{"empty string", "", TypeString, ""},
		{"int", int64(-42), TypeInt, "-42"},
		{"int max", int64(math.MaxInt64), TypeInt, "9223372036854775807"},
		{"json", []byte(`{"a":[1,2]}`), TypeJson, `{"a":[1,2]}`},
		{"float", 3.25, TypeFloat, "3.25"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mustPut(t, kv, tc.name, tc.content, tc.vt, 0)
			p := mustGet(t, kv, tc.name)
			if string(p.Content) != tc.want || p.Type != tc.vt {
				t.Errorf("got %q (type %d); want %q (type %d)", p.Content, p.Type, tc.want, tc.vt)
			}
		})
	}
}

func TestPutTypeMismatch(t *testing.T) {
	kv := newStore(t)
	tests := []struct {
		name    string
		content any
		vt      ValueType
		wantErr error
	}{
		{"string given int64", int64(1), TypeString, ErrTypeMismatch},
		{"int given plain int", 5, TypeInt, ErrTypeMismatch}, // 5 is int, not int64
		{"int given string", "5", TypeInt, ErrTypeMismatch},
		{"json invalid", []byte(`{bad`), TypeJson, ErrTypeMismatch},
		{"json given string", `{"a":1}`, TypeJson, ErrTypeMismatch},
		{"json nil", []byte(nil), TypeJson, ErrTypeMismatch},
		{"float given int64", int64(1), TypeFloat, ErrTypeMismatch},
		{"unknown type", "x", ValueType(99), ErrUnknownType},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := kv.Put("k", tc.content, tc.vt, 0); !errors.Is(err, tc.wantErr) {
				t.Errorf("err = %v; want %v", err, tc.wantErr)
			}
		})
	}
	// a failed Put must not create the key
	if _, err := kv.Get("k"); !errors.Is(err, ErrNotFound) {
		t.Errorf("failed Puts created the key: err = %v", err)
	}
}

// The caller must not be able to change stored data through a returned slice.
func TestGetReturnsCopy(t *testing.T) {
	kv := newStore(t)
	mustPut(t, kv, "a", "hello", TypeString, 0)

	p := mustGet(t, kv, "a")
	p.Content[0] = 'X'

	if got := string(mustGet(t, kv, "a").Content); got != "hello" {
		t.Errorf("stored value changed through returned slice: %q", got)
	}
}

// ...and not through the slice it passed in to Put.
func fCopiesJSONInput(t *testing.T) {
	kv := newStore(t)
	in := []byte(`{"a":1}`)
	mustPut(t, kv, "j", in, TypeJson, 0)

	in[6] = '9'

	if got := string(mustGet(t, kv, "j").Content); got != `{"a":1}` {
		t.Errorf("stored value changed through caller's input slice: %q", got)
	}
}

// ───────────────────────── Versions ─────────────────────────

func TestVersionsStrictlyIncrease(t *testing.T) {
	kv := newStore(t)
	v1 := mustPut(t, kv, "a", "1", TypeString, 0)
	v2 := mustPut(t, kv, "b", "2", TypeString, 0) // different key, same global counter
	v3 := mustPut(t, kv, "a", "3", TypeString, 0) // overwrite
	if !(v1 < v2 && v2 < v3) {
		t.Errorf("versions not increasing: %d, %d, %d", v1, v2, v3)
	}
	if p := mustGet(t, kv, "a"); p.Version != v3 {
		t.Errorf("Get version = %d; want %d", p.Version, v3)
	}
}

// Failed operations must not use up a version number.
func TestFailedOpsDoNotBumpVersion(t *testing.T) {
	kv := newStore(t)
	v := mustPut(t, kv, "s", "str", TypeString, 0)

	kv.Put("x", 5, TypeInt, 0)                         // type mismatch
	kv.CompareAndPut("s", v+100, "y", TypeString, 0)   // stale version
	kv.CompareAndPut("missing", 1, "y", TypeString, 0) // not found
	kv.Incr("s")                                       // not an int

	if next := mustPut(t, kv, "z", "z", TypeString, 0); next != v+1 {
		t.Errorf("next version = %d; want %d (a failed op consumed a version)", next, v+1)
	}
}

// ───────────────────────── TTL ─────────────────────────

func TestTTLExpires(t *testing.T) {
	kv := newStore(t)
	mustPut(t, kv, "a", "x", TypeString, shortTTL)
	mustGet(t, kv, "a") // visible before expiry

	waitExpire()

	if _, err := kv.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("after TTL: err = %v; want ErrNotFound", err)
	}
}

func TestZeroAndNegativeTTLNeverExpire(t *testing.T) {
	kv := newStore(t)
	mustPut(t, kv, "zero", "x", TypeString, 0)
	mustPut(t, kv, "neg", "x", TypeString, -time.Second)

	waitExpire()
	kv.removeExpired()

	mustGet(t, kv, "zero")
	mustGet(t, kv, "neg")
}

// Overwriting a key with ttl=0 must remove the old TTL.
func TestOverwriteClearsTTL(t *testing.T) {
	kv := newStore(t)
	mustPut(t, kv, "a", "old", TypeString, shortTTL)
	mustPut(t, kv, "a", "new", TypeString, 0)

	waitExpire()

	if got := string(mustGet(t, kv, "a").Content); got != "new" {
		t.Errorf("got %q; want \"new\"", got)
	}
}

func TestRemoveExpired(t *testing.T) {
	kv := newStore(t)
	mustPut(t, kv, "dies1", "x", TypeString, shortTTL)
	mustPut(t, kv, "dies2", "x", TypeString, shortTTL)
	mustPut(t, kv, "lives", "x", TypeString, 0)
	mustPut(t, kv, "lives-long", "x", TypeString, time.Hour)

	waitExpire()
	kv.removeExpired()

	kv.mu.RLock()
	n := len(kv.data)
	_, ok := kv.data["dies1"]
	kv.mu.RUnlock()

	if n != 2 || ok {
		t.Errorf("after removeExpired: %d keys (dies1 present=%v); want 2 keys", n, ok)
	}
}

// ───────────────────────── Delete ─────────────────────────

func TestDelete(t *testing.T) {
	kv := newStore(t)
	mustPut(t, kv, "a", "x", TypeString, 0)

	if !kv.Delete("a") {
		t.Error("first Delete = false; want true")
	}
	if kv.Delete("a") {
		t.Error("second Delete = true; want false")
	}
	if kv.Delete("never-existed") {
		t.Error("Delete(missing) = true; want false")
	}
	if _, err := kv.Get("a"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after Delete: err = %v", err)
	}
}

func TestDeleteExpiredReturnsFalse(t *testing.T) {
	kv := newStore(t)
	mustPut(t, kv, "a", "x", TypeString, shortTTL)
	waitExpire()
	if kv.Delete("a") {
		t.Error("Delete(expired) = true; want false (it was already logically gone)")
	}
}

// ───────────────────────── CompareAndPut ─────────────────────────

func TestCompareAndPut(t *testing.T) {
	kv := newStore(t)
	v1 := mustPut(t, kv, "a", "one", TypeString, 0)

	// right version → succeeds, returns a newer version
	v2, err := kv.CompareAndPut("a", v1, "two", TypeString, 0)
	if err != nil || v2 <= v1 {
		t.Fatalf("CAS(right version) = %d, %v; want > %d, nil", v2, err, v1)
	}

	// old version again → refused, value unchanged
	if _, err := kv.CompareAndPut("a", v1, "three", TypeString, 0); !errors.Is(err, ErrVersionMismatch) {
		t.Errorf("CAS(stale) err = %v; want ErrVersionMismatch", err)
	}
	if p := mustGet(t, kv, "a"); string(p.Content) != "two" || p.Version != v2 {
		t.Errorf("after stale CAS: %q v%d; want \"two\" v%d", p.Content, p.Version, v2)
	}

	// missing key
	if _, err := kv.CompareAndPut("nope", 1, "x", TypeString, 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("CAS(missing) err = %v; want ErrNotFound", err)
	}

	// bad content → type error, value unchanged
	if _, err := kv.CompareAndPut("a", v2, int64(1), TypeString, 0); !errors.Is(err, ErrTypeMismatch) {
		t.Errorf("CAS(bad type) err = %v; want ErrTypeMismatch", err)
	}
	if p := mustGet(t, kv, "a"); p.Version != v2 {
		t.Errorf("failed CAS changed the value")
	}
}

func TestCompareAndPutExpired(t *testing.T) {
	kv := newStore(t)
	v := mustPut(t, kv, "a", "x", TypeString, shortTTL)
	waitExpire()
	if _, err := kv.CompareAndPut("a", v, "y", TypeString, 0); !errors.Is(err, ErrNotFound) {
		t.Errorf("CAS on expired key: err = %v; want ErrNotFound", err)
	}
}

// ───────────────────────── Incr ─────────────────────────

func TestIncr(t *testing.T) {
	kv := newStore(t)

	if n, err := kv.Incr("new"); n != 1 || err != nil {
		t.Errorf("Incr(new) = %d, %v; want 1, nil", n, err)
	}

	mustPut(t, kv, "five", int64(5), TypeInt, 0)
	if n, err := kv.Incr("five"); n != 6 || err != nil {
		t.Errorf("Incr(5) = %d, %v; want 6, nil", n, err)
	}
	if got := string(mustGet(t, kv, "five").Content); got != "6" {
		t.Errorf("stored = %q; want \"6\"", got)
	}

	mustPut(t, kv, "neg", int64(-1), TypeInt, 0)
	if n, _ := kv.Incr("neg"); n != 0 {
		t.Errorf("Incr(-1) = %d; want 0", n)
	}
}

func TestIncrWrongType(t *testing.T) {
	kv := newStore(t)
	tests := []struct {
		name    string
		content any
		vt      ValueType
	}{
		{"string", "5", TypeString}, // looks like a number, but the type is string
		{"float", 1.5, TypeFloat},
		{"json", []byte(`1`), TypeJson},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mustPut(t, kv, tc.name, tc.content, tc.vt, 0)
			before := mustGet(t, kv, tc.name)
			if _, err := kv.Incr(tc.name); !errors.Is(err, ErrNotInt) {
				t.Errorf("err = %v; want ErrNotInt", err)
			}
			if after := mustGet(t, kv, tc.name); after.Version != before.Version {
				t.Error("failed Incr changed the stored value")
			}
		})
	}
}

// Redis returns an error instead of wrapping around. Silently going from
// MaxInt64 to MinInt64 is a bug a counter user would never notice.
func TestIncrOverflow(t *testing.T) {
	kv := newStore(t)
	mustPut(t, kv, "max", int64(math.MaxInt64), TypeInt, 0)
	n, err := kv.Incr("max")
	if err == nil {
		t.Errorf("Incr(MaxInt64) = %d, nil; want an error (overflow)", n)
	}
}

func TestIncrKeepsTTL(t *testing.T) {
	kv := newStore(t)
	mustPut(t, kv, "c", int64(1), TypeInt, time.Hour)
	exp := mustGet(t, kv, "c").ExpiresAt

	kv.Incr("c")

	if got := mustGet(t, kv, "c").ExpiresAt; !got.Equal(exp) {
		t.Errorf("ExpiresAt changed: %v → %v", exp, got)
	}
}

func TestIncrExpiredRestartsWithoutTTL(t *testing.T) {
	kv := newStore(t)
	mustPut(t, kv, "c", int64(100), TypeInt, shortTTL)
	waitExpire()

	if n, err := kv.Incr("c"); n != 1 || err != nil {
		t.Errorf("Incr(expired) = %d, %v; want 1, nil", n, err)
	}
	if exp := mustGet(t, kv, "c").ExpiresAt; !exp.IsZero() {
		t.Errorf("restarted counter kept the old (past) expiry %v", exp)
	}
}

// ───────────────────────── Concurrency (the hard part) ─────────────────────────

// 100 goroutines × 100 Incr. Not just the final total: every Incr must return a
// DIFFERENT number. Two calls returning the same n means both read the same old value.
func TestConcurrentIncrUniqueResults(t *testing.T) {
	kv := newStore(t)
	const G, N = 100, 100

	results := make([][]int64, G) // own slot per goroutine, no lock needed
	var wg sync.WaitGroup
	for g := range G {
		wg.Go(func() {
			for range N {
				n, err := kv.Incr("hits")
				if err != nil {
					t.Errorf("Incr: %v", err)
					return
				}
				results[g] = append(results[g], n)
			}
		})
	}
	wg.Wait()

	seen := make(map[int64]bool, G*N)
	for _, rs := range results {
		for _, n := range rs {
			if seen[n] {
				t.Fatalf("Incr returned %d twice (lost update)", n)
			}
			seen[n] = true
		}
	}
	for i := int64(1); i <= G*N; i++ {
		if !seen[i] {
			t.Fatalf("no Incr returned %d; results should be exactly 1..%d", i, G*N)
		}
	}
	if got := string(mustGet(t, kv, "hits").Content); got != fmt.Sprint(G*N) {
		t.Errorf("final = %s; want %d", got, G*N)
	}
}

// 100 goroutines all hold the same version and race to CAS. Exactly one may win.
func TestConcurrentCASExactlyOneWinner(t *testing.T) {
	kv := newStore(t)
	v := mustPut(t, kv, "lock", "free", TypeString, 0)

	var wins, mismatches atomic.Int32
	start := make(chan struct{}) // closed to release all goroutines at once (module 12 §5)
	var wg sync.WaitGroup
	for i := range 100 {
		wg.Go(func() {
			<-start
			_, err := kv.CompareAndPut("lock", v, fmt.Sprint("owner-", i), TypeString, 0)
			switch {
			case err == nil:
				wins.Add(1)
			case errors.Is(err, ErrVersionMismatch):
				mismatches.Add(1)
			default:
				t.Errorf("unexpected err %v", err)
			}
		})
	}
	close(start)
	wg.Wait()

	if wins.Load() != 1 || mismatches.Load() != 99 {
		t.Errorf("wins=%d mismatches=%d; want 1 and 99", wins.Load(), mismatches.Load())
	}
}

// Optimistic locking the way a real client uses it: read → compute → CAS → retry on
// mismatch. 50 goroutines × 100 increments through CAS must give exactly 5000.
func TestConcurrentCASRetryLoopCounter(t *testing.T) {
	kv := newStore(t)
	mustPut(t, kv, "n", int64(0), TypeInt, 0)
	const G, N = 50, 100

	var retries atomic.Int64
	var wg sync.WaitGroup
	for range G {
		wg.Go(func() {
			for range N {
				for {
					p := mustGet(t, kv, "n")
					var cur int64
					fmt.Sscan(string(p.Content), &cur)
					_, err := kv.CompareAndPut("n", p.Version, cur+1, TypeInt, 0)
					if err == nil {
						break
					}
					if !errors.Is(err, ErrVersionMismatch) {
						t.Errorf("CAS: %v", err)
						return
					}
					retries.Add(1)
				}
			}
		})
	}
	wg.Wait()

	if got := string(mustGet(t, kv, "n").Content); got != fmt.Sprint(G*N) {
		t.Errorf("final = %s; want %d", got, G*N)
	}
	t.Logf("CAS retries under contention: %d", retries.Load())
}

// Versions handed out concurrently (Put on different keys + Incr) must be unique
// and have no gaps: the global counter is only touched under the lock.
func TestConcurrentVersionsUniqueNoGaps(t *testing.T) {
	kv := newStore(t)
	const G, N = 50, 200

	versions := make([][]int64, G)
	var wg sync.WaitGroup
	for g := range G {
		wg.Go(func() {
			for i := range N {
				v, err := kv.Put(fmt.Sprint(g, "-", i), "x", TypeString, 0)
				if err != nil {
					t.Errorf("Put: %v", err)
					return
				}
				versions[g] = append(versions[g], v)
			}
		})
	}
	wg.Wait()

	seen := make(map[int64]bool, G*N)
	for _, vs := range versions {
		for _, v := range vs {
			if seen[v] {
				t.Fatalf("version %d handed out twice", v)
			}
			seen[v] = true
		}
	}
	for v := int64(1); v <= G*N; v++ {
		if !seen[v] {
			t.Fatalf("version %d missing (gap)", v)
		}
	}
}

// Everything at once on a small set of keys, while the cleaner sweeps nonstop.
// Passes only if `go test -race` reports nothing and nothing panics.
func TestConcurrentChaos(t *testing.T) {
	kv := newStore(t)
	keys := []string{"a", "b", "c", "d"}
	deadline := time.Now().Add(300 * time.Millisecond)

	var wg sync.WaitGroup
	wg.Go(func() { // a cleaner running far more often than every 5 min
		for time.Now().Before(deadline) {
			kv.removeExpired()
		}
	})
	for g := range 16 {
		wg.Go(func() {
			for i := 0; time.Now().Before(deadline); i++ {
				k := keys[(g+i)%len(keys)]
				switch i % 6 {
				case 0:
					kv.Put(k, int64(i), TypeInt, time.Millisecond) // expires almost at once
				case 1:
					kv.Put(k, "s", TypeString, 0)
				case 2:
					if p, err := kv.Get(k); err == nil {
						p.Content = append(p.Content[:0], 'Z') // scribble on our copy
					}
				case 3:
					kv.Incr(k)
				case 4:
					if p, err := kv.Get(k); err == nil {
						kv.CompareAndPut(k, p.Version, "cas", TypeString, 0)
					}
				case 5:
					kv.Delete(k)
				}
			}
		})
	}
	wg.Wait()

	for _, k := range keys { // no stored value should ever be the 'Z' we scribbled
		if p, err := kv.Get(k); err == nil && string(p.Content) == "Z" {
			t.Errorf("key %q holds a value written through a returned slice", k)
		}
	}
}

// ───────────────────────── Close / goroutine leaks ─────────────────────────

func TestCloseTwiceAndConcurrently(t *testing.T) {
	kv := New()
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(kv.Close) // 20 goroutines close at once: no panic, nobody hangs
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("concurrent Close calls hung")
	}
	kv.Close() // and once more afterwards
}

// Every New() starts a cleaner goroutine; Close must really stop it.
func TestCloseStopsCleanerGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()

	stores := make([]*Store, 50)
	for i := range stores {
		stores[i] = New()
	}
	if during := runtime.NumGoroutine(); during < before+50 {
		t.Fatalf("expected ≥ %d goroutines while 50 stores are open, got %d", before+50, during)
	}
	for _, s := range stores {
		s.Close()
	}

	// goroutines exit asynchronously; give them a moment
	for range 50 {
		if runtime.NumGoroutine() <= before {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Errorf("goroutine leak: %d before, %d after Close", before, runtime.NumGoroutine())
}

// The store keeps working after Close (only the background cleaner stops).
func TestUsableAfterClose(t *testing.T) {
	kv := New()
	kv.Close()
	mustPut(t, kv, "a", "x", TypeString, 0)
	mustGet(t, kv, "a")
}
