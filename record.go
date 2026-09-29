package kv

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

// 帧格式（定长头 25 字节 + 变长载荷）：
//
//	[0:4]   magic "KVSG"
//	[4]     记录类型：1=PUT，2=DELETE
//	[5:13]  版本序号（大端 uint64）
//	[13:17] key 长度（大端 uint32）
//	[17:21] value 长度（大端 uint32；DELETE 记录为 0）
//	[21:25] CRC32-IEEE，覆盖类型/序号/长度以及 key/value
//	随后紧跟 key、value 原始字节。
//
// 一条记录的写入与 fsync 是一次性提交的：崩溃只可能让最后一帧缺失，
// 不可能出现“半帧已提交”。恢复时损坏按位置区分为尾部不完整 / 校验不一致 / 序号回退。
const (
	frameMagic = "KVSG"
	headerSize = 25

	recPut        byte = 1
	recDelete     byte = 2
	recCheckpoint byte = 3 // 回收产物头部的检查点帧，seq 字段即回收后的 nextSeq
)

type frame struct {
	typ   byte
	seq   uint64
	key   []byte
	value []byte
}

func encodeFrame(f frame) ([]byte, error) {
	isCheckpoint := f.typ == recCheckpoint
	if !isCheckpoint && len(f.key) == 0 {
		return nil, errors.New("kv: empty key")
	}
	if uint64(len(f.key)) > uint64(^uint32(0)) ||
		uint64(len(f.value)) > uint64(^uint32(0)) {
		return nil, errors.New("kv: key or value too large")
	}
	if f.typ != recPut && f.typ != recDelete && f.typ != recCheckpoint {
		return nil, errors.New("kv: invalid record type")
	}
	if isCheckpoint && (len(f.key) != 0 || len(f.value) != 0) {
		return nil, errors.New("kv: checkpoint frame must have empty key and value")
	}

	total := headerSize + len(f.key) + len(f.value)
	buf := make([]byte, total)
	copy(buf[0:4], frameMagic)
	buf[4] = f.typ
	binary.BigEndian.PutUint64(buf[5:13], f.seq)
	binary.BigEndian.PutUint32(buf[13:17], uint32(len(f.key)))
	binary.BigEndian.PutUint32(buf[17:21], uint32(len(f.value)))
	copy(buf[headerSize:], f.key)
	copy(buf[headerSize+len(f.key):], f.value)
	sum := crc32.ChecksumIEEE(append(append([]byte(nil), buf[4:21]...),
		buf[headerSize:total]...))
	binary.BigEndian.PutUint32(buf[21:25], sum)
	return buf, nil
}

// scanFrame 从 buffer 起始处解析一帧。
// 返回帧、整帧字节数；buffer 为空时 consumed==0 且 err==io.EOF 表示干净结尾；
// buffer 数据不足以凑成完整一帧时返回 ErrIncompleteTail 类别错误；
// 帧完整但 CRC/magic 不对时返回 ErrChecksumMismatch 类别错误。
func scanFrame(buf []byte) (frame, int, error) {
	if len(buf) == 0 {
		return frame{}, 0, io.EOF
	}
	at := func(detail string, kind CorruptionKind) error {
		return &CorruptionError{Kind: kind, Offset: -1, Detail: detail}
	}
	if len(buf) < headerSize {
		return frame{}, len(buf), at("header truncated: only "+itoa(len(buf))+
			" of "+itoa(headerSize)+" bytes present", KindIncompleteTail)
	}
	if string(buf[0:4]) != frameMagic {
		return frame{}, 0, at("bad frame magic", KindChecksum)
	}
	typ := buf[4]
	if typ != recPut && typ != recDelete && typ != recCheckpoint {
		return frame{}, 0, at("unknown record type", KindChecksum)
	}
	seq := binary.BigEndian.Uint64(buf[5:13])
	kl := int(binary.BigEndian.Uint32(buf[13:17]))
	vl := int(binary.BigEndian.Uint32(buf[17:21]))
	want := binary.BigEndian.Uint32(buf[21:25])
	// checkpoint 帧没有 key/value；其余帧必须有非空 key，DELETE 帧 value 必须为空。
	if typ == recCheckpoint {
		if kl != 0 || vl != 0 {
			return frame{}, 0, at("checkpoint frame must have empty key and value", KindChecksum)
		}
	} else {
		if kl == 0 {
			return frame{}, 0, at("empty key in frame", KindChecksum)
		}
		if typ == recDelete && vl != 0 {
			return frame{}, 0, at("delete frame must carry empty value", KindChecksum)
		}
	}
	total := headerSize + kl + vl

	if len(buf) < total {
		return frame{}, len(buf), at("payload truncated: frame declares "+itoa(total)+
			" bytes but only "+itoa(len(buf))+" present", KindIncompleteTail)
	}
	payload := buf[headerSize:total]
	got := crc32.ChecksumIEEE(append(append([]byte(nil), buf[4:21]...), payload...))
	if got != want {
		return frame{}, 0, at("crc mismatch", KindChecksum)
	}
	f := frame{typ: typ, seq: seq,
		key:   append([]byte(nil), buf[headerSize:headerSize+kl]...),
		value: append([]byte(nil), buf[headerSize+kl:total]...),
	}
	return f, total, nil
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		b[i] = '-'
	}
	return string(b[i:])
}
