package kv

import (
	"encoding/binary"
	"hash/crc32"
	"os"
)

const (
	snapMagic  = "KVS1"
	snapHdrLen = 30 // magic(4)+hdrLen(2)+lastSeq(8)+dropBeforeSeq(8)+count(4)+crc(4)
)

// writeSnapshot 把索引状态写成自描述、带 CRC 的快照文件。
//
// 快照与普通 WAL 一样用 KVR1 记录帧承载每个键的最新版本，区别在于：
//   - 文件头记录 lastSeq（快照覆盖到的序号）与条目数；
//   - 快照条目之间不保证按 seq 排序（不同键互不相干），因此重放快照
//     不做序号回退检查，快照整体是"某一时刻的完整状态"的权威镜像；
//   - seq < dropBeforeSeq 的墓碑可以直接丢弃：快照之后的尾巴段
//     不可能再含比该墓碑更旧的记录，墓碑的"压制"使命已经完成。
//
// 调用方先写到 path（通常是 *.tmp）并 fsync，之后由 MANIFEST 的原子
// 重命名让快照生效；因此崩溃要么发生在生效前（文件是孤儿，被清理），
// 要么发生在生效后（文件完整，被新 MANIFEST 引用），不存在中间态。
func writeSnapshot(path string, lastSeq uint64, idx memIndex, dropBeforeSeq uint64) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}

	// 第一遍：统计真正要写入的条目数。
	count := 0
	for _, e := range idx {
		if e.deleted && e.seq < dropBeforeSeq {
			continue
		}
		count++
	}

	hdr := make([]byte, snapHdrLen)
	copy(hdr[0:4], snapMagic)
	binary.BigEndian.PutUint16(hdr[4:6], snapHdrLen)
	binary.BigEndian.PutUint64(hdr[6:14], lastSeq)
	binary.BigEndian.PutUint64(hdr[14:22], dropBeforeSeq)
	binary.BigEndian.PutUint32(hdr[22:26], uint32(count))
	sum := crc32.Checksum(hdr[4:26], crcTable)
	binary.BigEndian.PutUint32(hdr[26:30], sum)

	writeErr := func() error {
		if _, err := f.Write(hdr); err != nil {
			return err
		}
		for k, e := range idx {
			if e.deleted && e.seq < dropBeforeSeq {
				continue
			}
			r := &record{seq: e.seq, key: []byte(k)}
			if e.deleted {
				r.op = opDelete
			} else {
				r.op = opPut
				r.val = e.value
			}
			if _, err := f.Write(encodeRecord(r)); err != nil {
				return err
			}
		}
		return fsyncFile(f)
	}()

	if cerr := f.Close(); writeErr == nil {
		writeErr = cerr
	}
	return writeErr
}

// readSnapshot 读取快照，返回重建的索引与快照覆盖到的最大序号。
// 快照一旦被 MANIFEST 引用就必然是完整的；任何读不全都按损坏报错，
// 绝不返回部分索引（防止重启后悄悄丢失一部分键）。
func readSnapshot(path string) (memIndex, uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	if len(data) < snapHdrLen {
		return nil, 0, newCorrupt(KindChecksumMismatch, path,
			"snapshot truncated: header only %d bytes", len(data))
	}
	hdr := data[:snapHdrLen]
	if string(hdr[0:4]) != snapMagic {
		return nil, 0, newCorrupt(KindChecksumMismatch, path, "snapshot magic mismatch")
	}
	if binary.BigEndian.Uint16(hdr[4:6]) != snapHdrLen {
		return nil, 0, newCorrupt(KindChecksumMismatch, path, "snapshot header length invalid")
	}
	want := binary.BigEndian.Uint32(hdr[26:30])
	if crc32.Checksum(hdr[4:26], crcTable) != want {
		return nil, 0, newCorrupt(KindChecksumMismatch, path, "snapshot header crc mismatch")
	}
	lastSeq := binary.BigEndian.Uint64(hdr[6:14])
	count := binary.BigEndian.Uint32(hdr[22:26])

	idx := make(memIndex, count)
	off := snapHdrLen
	for i := uint32(0); i < count; i++ {
		rec, n, derr := decodeRecord(data[off:])
		if derr != nil {
			// 引用中的快照不应存在半截帧：截断与 CRC 失败都按内容损坏处理。
			if derr == errIncompleteFrame {
				return nil, 0, newCorrupt(KindChecksumMismatch, path,
					"snapshot truncated inside entry %d", i)
			}
			if ce, ok := derr.(*CorruptionError); ok {
				ce.File = path
			}
			return nil, 0, derr
		}
		applySnapshotEntry(idx, rec)
		off += n
	}
	if off != len(data) {
		return nil, 0, newCorrupt(KindChecksumMismatch, path,
			"snapshot has %d trailing bytes beyond declared entries", len(data)-off)
	}
	return idx, lastSeq, nil
}

// applySnapshotEntry 把一条快照记录写入索引；快照内不做 seq 比较，
// 每个键在快照中恰好出现一次。
func applySnapshotEntry(idx memIndex, r *record) {
	e := indexEntry{seq: r.seq}
	if r.op == opDelete {
		e.deleted = true
	} else {
		e.value = r.val
	}
	idx[string(r.key)] = e
}
