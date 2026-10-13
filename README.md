# kv-cache

A goroutine-safe, in-memory key-value store in Go. It works like a small Redis: typed values (string, int, float, JSON), per-key TTL, a version on every write, compare-and-put, atomic `Incr`, and a background cleaner that removes expired keys every 5 minutes. It only uses the Go standard library.

```go
kv := kvcache.New()
defer kv.Close()

v, _ := kv.Put("user:1", "manthan", kvcache.TypeString, 10*time.Minute)
p, _ := kv.Get("user:1")   // a copy: p.Content, p.Version, p.ExpiresAt
n, _ := kv.Incr("hits")
```

One `sync.RWMutex` guards the map and the version counter. `Get` takes a read lock and never deletes, so expired keys are just reported as missing. Every write does its whole read-check-write in one critical section.

## Compare-and-put (CAS)

Every write gets a new global version. `CompareAndPut` writes only if the key's version is still the one you read. Otherwise it returns `ErrVersionMismatch`, and nothing changes.

```go
for {
    p, _ := kv.Get("balance")
    newVal := compute(p.Content)
    _, err := kv.CompareAndPut("balance", p.Version, newVal, kvcache.TypeInt, 0)
    if err == nil {
        break                                        // nobody changed it in between
    }
    if !errors.Is(err, kvcache.ErrVersionMismatch) {
        return err
    }
    // someone else wrote first → re-read and retry
}
```

This is **optimistic concurrency**: no lock is held while computing, and conflicts are detected at write time. It's the same idea as `UPDATE ... WHERE version = ?` in SQL, DynamoDB conditional writes, and etcd transactions. Checking the version and writing happen under one lock, so two writers can never both succeed with the same version.

## Thread-safety tests

```
go test -race -count=1 -v .
```

| Test | What it proves |
|---|---|
| `TestConcurrentIncrUniqueResults` | 100 goroutines × 100 `Incr`: every call returns a **different** number from 1 to 10000. That catches lost updates, which a final-total check alone could miss. |
| `TestConcurrentCASExactlyOneWinner` | 100 goroutines, released at once with `close(start)`, race to CAS on the same version: **exactly 1** wins and 99 get `ErrVersionMismatch`. |
| `TestConcurrentCASRetryLoopCounter` | 50×100 increments via read → CAS → retry: the result is exactly **5000** (about 3,000 retries happen under contention). |
| `TestConcurrentVersionsUniqueNoGaps` | 10,000 concurrent Puts get versions 1 to 10000, each exactly once. |
| `TestConcurrentChaos` | 16 goroutines running every operation, plus a nonstop cleaner: **no data race** under `-race`, no panic. |
| `TestCloseStopsCleanerGoroutine` | `Close()` really stops the cleaner goroutine: no leak across 50 stores. |

Removing the lock from `Incr` makes these fail with `concurrent map read and map write` (and `DATA RACE` under `-race`).
