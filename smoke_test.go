package kv

import (
	"errors"
	"testing"
)

func TestSmoke(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		k := []byte(string(rune('a'+i%5)) + "-" + itoa(i))
		if err := db.Put(k, []byte("v-"+itoa(i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Delete([]byte("a-0")); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("a-0")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key resurfaced: %v", err)
	}
	if v, err := db.Get([]byte("b-1")); err != nil || string(v) != "v-1" {
		t.Fatalf("got %q, %v", v, err)
	}
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if v, err := db.Get([]byte("b-1")); err != nil || string(v) != "v-1" {
		t.Fatalf("post-compact got %q, %v", v, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	db, err = Open(DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get([]byte("a-0")); !errors.Is(err, ErrNotFound) {
		t.Fatalf("deleted key resurfaced after compact+reopen: %v", err)
	}
	if v, err := db.Get([]byte("c-2")); err != nil || string(v) != "v-2" {
		t.Fatalf("post-reopen got %q, %v want v-2", v, err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	n := len(b)
	for i > 0 {
		n--
		b[n] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		n--
		b[n] = '-'
	}
	return string(b[n:])
}
