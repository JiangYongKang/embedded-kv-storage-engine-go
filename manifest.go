package kv

import (
	"bufio"
	"fmt"
	"hash/crc32"
	"os"
	"strconv"
	"strings"
)

// walFileInfo 描述 MANIFEST 中登记的一个 WAL 段文件。
type walFileInfo struct {
	ID     uint64
	MinSeq uint64 // 该段第一条记录的序号（段可能为空，为 0）
}

// manifestData 是 MANIFEST 的全部内容。
// 恢复时只信任 MANIFEST 中登记的文件集合，其它文件一律视为孤儿。
type manifestData struct {
	Version  int
	Snapshot string // 当前基线快照文件名；空表示没有快照
	SnapSeq  uint64 // 快照覆盖到的最大序号
	Tails    []walFileInfo
	ActiveID uint64 // 当前追加写入的活动段 ID
	NextSnap uint64 // 下一个快照编号
}

const manifestVersion = 1

// MANIFEST 是逐行自描述文本，每个数据行自带 CRC：
//
//	version=1
//	snapshot=snap-00000003 snapseq=1024
//	active=5 nextsnap=4
//	wal id=3 minseq=900
//	wal id=4 minseq=980
//
// 每个逻辑行后面跟一行 "crc=<10进制>"，覆盖该行文本。
// 任何一行缺失/校验失败都整体按 KindManifestCorrupt 失败，
// 绝不接受半个清单（半套文件集合本身就是"中间态"）。
func writeManifest(dir string, m *manifestData) error {
	var lines []string
	lines = append(lines, fmt.Sprintf("version=%d", m.Version))
	if m.Snapshot != "" {
		lines = append(lines, fmt.Sprintf("snapshot=%s snapseq=%d", m.Snapshot, m.SnapSeq))
	} else {
		lines = append(lines, "snapshot= snapseq=0")
	}
	lines = append(lines, fmt.Sprintf("active=%d nextsnap=%d", m.ActiveID, m.NextSnap))
	for _, w := range m.Tails {
		lines = append(lines, fmt.Sprintf("wal id=%d minseq=%d", w.ID, w.MinSeq))
	}

	var b strings.Builder
	for _, ln := range lines {
		b.WriteString(ln)
		b.WriteByte('\n')
		b.WriteString(fmt.Sprintf("crc=%d\n", crc32.Checksum([]byte(ln), crcTable)))
	}

	tmp := manifestTmpPath(dir)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	_, werr := f.WriteString(b.String())
	if werr == nil {
		werr = fsyncFile(f)
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return werr
	}
	// 原子替换 + 同步目录：MANIFEST 的提交点。
	return syncRename(dir, tmp, manifestPath(dir))
}

func readManifest(dir string) (*manifestData, error) {
	path := manifestPath(dir)
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	m := &manifestData{Version: -1}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	lineNo := 0
	pending := ""
	for sc.Scan() {
		lineNo++
		line := sc.Text()
		if pending == "" {
			pending = line
			continue
		}
		// 上一逻辑行的 CRC 行。
		if !checkLineCRC(pending, line) {
			return nil, newCorrupt(KindManifestCorrupt, manifestName,
				"line %d checksum mismatch", lineNo-1)
		}
		if err := applyManifestLine(m, pending); err != nil {
			return nil, err
		}
		pending = ""
	}
	if err := sc.Err(); err != nil {
		return nil, newCorrupt(KindManifestCorrupt, manifestName, "read: %v", err)
	}
	// 缺少最后的 CRC 行 => 清单写了一半。
	if pending != "" {
		return nil, newCorrupt(KindManifestCorrupt, manifestName,
			"last line missing crc (torn manifest)")
	}
	if m.Version != manifestVersion {
		return nil, newCorrupt(KindManifestCorrupt, manifestName,
			"unsupported version %d", m.Version)
	}
	if m.ActiveID == 0 {
		return nil, newCorrupt(KindManifestCorrupt, manifestName, "missing active segment")
	}
	if m.NextSnap == 0 {
		m.NextSnap = 1
	}
	return m, nil
}

func checkLineCRC(line, crcLine string) bool {
	got, err := strconv.ParseUint(strings.TrimPrefix(crcLine, "crc="), 10, 64)
	if err != nil {
		return false
	}
	return uint64(crc32.Checksum([]byte(line), crcTable)) == got
}

func applyManifestLine(m *manifestData, line string) error {
	fields := strings.Fields(line)
	switch fields[0] {
	case "version=1":
		m.Version = 1
	case "version=":
		return newCorrupt(KindManifestCorrupt, manifestName, "empty version")
	default:
		if strings.HasPrefix(fields[0], "version=") {
			return newCorrupt(KindManifestCorrupt, manifestName, "unsupported %q", fields[0])
		}
	}
	switch {
	case strings.HasPrefix(line, "snapshot="):
		for _, f := range fields {
			switch {
			case strings.HasPrefix(f, "snapshot="):
				m.Snapshot = strings.TrimPrefix(f, "snapshot=")
			case strings.HasPrefix(f, "snapseq="):
				v, err := strconv.ParseUint(strings.TrimPrefix(f, "snapseq="), 10, 64)
				if err != nil {
					return newCorrupt(KindManifestCorrupt, manifestName, "bad snapseq")
				}
				m.SnapSeq = v
			default:
				return newCorrupt(KindManifestCorrupt, manifestName, "bad snapshot line: %q", line)
			}
		}
	case strings.HasPrefix(line, "active="):
		for _, f := range fields {
			switch {
			case strings.HasPrefix(f, "active="):
				v, err := strconv.ParseUint(strings.TrimPrefix(f, "active="), 10, 64)
				if err != nil {
					return newCorrupt(KindManifestCorrupt, manifestName, "bad active")
				}
				m.ActiveID = v
			case strings.HasPrefix(f, "nextsnap="):
				v, err := strconv.ParseUint(strings.TrimPrefix(f, "nextsnap="), 10, 64)
				if err != nil {
					return newCorrupt(KindManifestCorrupt, manifestName, "bad nextsnap")
				}
				m.NextSnap = v
			default:
				return newCorrupt(KindManifestCorrupt, manifestName, "bad active line: %q", line)
			}
		}
	case strings.HasPrefix(line, "wal "):
		var w walFileInfo
		for _, f := range fields[1:] {
			switch {
			case strings.HasPrefix(f, "id="):
				v, err := strconv.ParseUint(strings.TrimPrefix(f, "id="), 10, 64)
				if err != nil {
					return newCorrupt(KindManifestCorrupt, manifestName, "bad wal id")
				}
				w.ID = v
			case strings.HasPrefix(f, "minseq="):
				v, err := strconv.ParseUint(strings.TrimPrefix(f, "minseq="), 10, 64)
				if err != nil {
					return newCorrupt(KindManifestCorrupt, manifestName, "bad wal minseq")
				}
				w.MinSeq = v
			default:
				return newCorrupt(KindManifestCorrupt, manifestName, "bad wal line: %q", line)
			}
		}
		if w.ID == 0 {
			return newCorrupt(KindManifestCorrupt, manifestName, "wal id must be > 0")
		}
		m.Tails = append(m.Tails, w)
	}
	return nil
}
