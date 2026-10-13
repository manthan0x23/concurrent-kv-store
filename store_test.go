package kvcache

import (
	"testing"
)

func TestPutGet(t *testing.T) {
	kv := New()

	v, err := kv.Put("a", "hi", TypeString, 0)
	if err != nil {
		t.Fatalf("put: unexpected error %v", err)
	}

	p, err := kv.Get("a")
	if err != nil {
		t.Fatalf("get: unexpected error %v", err)
	}
	if string(p.Content) != "hi" {
		t.Errorf("content = %q, want %q", p.Content, "hi")
	}
	if p.Version != v {
		t.Errorf("version = %d, want %d", p.Version, v)
	}
}
