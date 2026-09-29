package kv

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
)

// opType 是单条日志记录的操作类型。
type opType uint8

const (
	opPut    opType = 1
	opDelete opType = 2
)

const (
	frameMagic   = "KVR1"
	frameHdrLen  = 27 // magic(4)+totalLen(4)+hdrLen(2)+seq(8)+op(1)+klen(4)+vlen(4)
	frameMinLen  = frameHdrLen + 4
	frameTrailer = 4 // crc32
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// errIncompleteFrame 表示缓冲区连一整帧都凑不齐（长度不足），
// 这是"尾部不完整"区别于"内容损坏"的唯一判定：帧声明长度大于剩余字节。
var errIncompleteFrame = errors.New("kv: incomplete frame")

// record 是 WAL 与快照中一条不可分割的编码单元。
type record struct {
	seq uint64
	op  opType
	key []byte
	val []byte // 删除操作时为 nil
}

// encodeRecord 把记录编码为自描述、带 CRC 的完整帧。
//
// 帧布局（大端）：
//
//	magic    [4] "KVR1"
//	totalLen uint32  整帧总字节数（含头与 CRC）
//	hdrLen   uint16  固定 27，便于以后扩展头部
//	seq      uint64  全局单调递增序号
//	op       uint8   1=put 2=delete
//	klen     uint32
//	vlen     uint32  delete 时为 0
//	key      [klen]
//	value    [vlen]
//	crc32    uint32  覆盖 seq..payload（即偏移 10 起到 CRC 前）
//
// 编码结果在单次 Write 中落盘，因此崩溃后要么读到完整帧，
// 要么在文件末尾读到一个残缺前缀（errIncompleteFrame）。
func encodeRecord(r *record) []byte {
	vlen := len(r.val)
	if r.op == opDelete {
		vlen = 0
	}
	total := frameHdrLen + len(r.key) + vlen + frameTrailer
	buf := make([]byte, total)
	copy(buf[0:4], frameMagic)
	binary.BigEndian.PutUint32(buf[4:8], uint32(total))
	binary.BigEndian.PutUint16(buf[8:10], frameHdrLen)
	binary.BigEndian.PutUint64(buf[10:18], r.seq)
	buf[18] = byte(r.op)
	binary.BigEndian.PutUint32(buf[19:23], uint32(len(r.key)))
	binary.BigEndian.PutUint32(buf[23:27], uint32(vlen))
	copy(buf[frameHdrLen:], r.key)
	copy(buf[frameHdrLen+len(r.key):], r.val)
	sum := crc32.Checksum(buf[10:total-frameTrailer], crcTable)
	binary.BigEndian.PutUint32(buf[total-frameTrailer:], sum)
	return buf
}

// decodeRecord 从 b 的起始处解码一条记录。
// 返回记录、本帧占用字节数以及错误：
//   - errIncompleteFrame：剩余字节不足以构成声明长度的整帧（尾部被截断）；
//   - *CorruptionError(KindChecksumMismatch)：帧完整但 magic/长度/CRC/操作码不一致。
func decodeRecord(b []byte) (*record, int, error) {
	// 连最小头都读不全：一定是被截断的尾部。
	if len(b) < 10 {
		return nil, 0, errIncompleteFrame
	}
	if string(b[0:4]) != frameMagic {
		// 帧边界上 magic 错误不可能来自半截写入（半截帧的前缀完好），
		// 属于内容损坏，交给 CRC 类别报告。
		return nil, 0, errBadFrame("magic mismatch")
	}
	total := int(binary.BigEndian.Uint32(b[4:8]))
	hdr := int(binary.BigEndian.Uint16(b[8:10]))
	if hdr != frameHdrLen || total < frameMinLen {
		return nil, 0, errBadFrame("invalid frame length header")
	}
	if len(b) < total {
		// 声明长度大于剩余字节：写入中途被打断。
		return nil, 0, errIncompleteFrame
	}
	frame := b[:total]
	klen := int(binary.BigEndian.Uint32(frame[19:23]))
	vlen := int(binary.BigEndian.Uint32(frame[23:27]))
	if frameHdrLen+klen+vlen+frameTrailer != total {
		return nil, 0, errBadFrame("declared field lengths inconsistent with frame")
	}
	want := binary.BigEndian.Uint32(frame[total-frameTrailer:])
	got := crc32.Checksum(frame[10:total-frameTrailer], crcTable)
	if want != got {
		return nil, 0, errBadFrame("crc mismatch")
	}
	op := opType(frame[18])
	if op != opPut && op != opDelete {
		return nil, 0, errBadFrame("unknown op type")
	}
	r := &record{
		seq: binary.BigEndian.Uint64(frame[10:18]),
		op:  op,
		key: append([]byte(nil), frame[frameHdrLen:frameHdrLen+klen]...),
	}
	if op == opPut && vlen > 0 {
		r.val = append([]byte(nil), frame[frameHdrLen+klen:total-frameTrailer]...)
	}
	return r, total, nil
}

// errBadFrame 表示帧结构完整但内容不被信任，统一归为校验不一致类别。
func errBadFrame(detail string) error {
	return newCorrupt(KindChecksumMismatch, "", "%s", detail)
}
