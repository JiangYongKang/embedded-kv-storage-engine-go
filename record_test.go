package kv

import (
	"errors"
	"io"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	b, err := encodeFrame(frame{typ: recPut, seq: 7, key: []byte("k"), value: []byte("v")})
	if err != nil {
		t.Fatal(err)
	}
	f, n, err := scanFrame(b)
	if err != nil || n != len(b) || f.seq != 7 || string(f.key) != "k" || string(f.value) != "v" {
		t.Fatalf("round trip failed: %+v n=%d err=%v", f, n, err)
	}
}

func TestScanFrameErrorKinds(t *testing.T) {
	// 空缓冲 = 干净结尾。
	if _, _, err := scanFrame(nil); err != io.EOF {
		t.Fatalf("empty buffer should be EOF, got %v", err)
	}
	// 不足一个头部 = 尾部不完整。
	if _, _, err := scanFrame(make([]byte, 4)); !errors.Is(err, ErrIncompleteTail) {
		t.Fatalf("short header should be incomplete-tail, got %v", err)
	}
	// 完整帧被翻转一个字节 = 校验不一致。
	b, _ := encodeFrame(frame{typ: recPut, seq: 1, key: []byte("abc"), value: []byte("xyz")})
	b[headerSize] ^= 0xFF
	if _, _, err := scanFrame(b); !errors.Is(err, ErrChecksumMismatch) {
		t.Fatalf("flipped payload should be checksum error, got %v", err)
	}
}
