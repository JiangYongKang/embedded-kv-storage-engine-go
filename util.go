package kv

import (
	"os"
	"path/filepath"
	"strconv"
)

// 目录中的文件名约定：
//
//	manifest.toml   元数据清单（哪些文件构成当前已提交状态）
//	wal-00000001    WAL 段，序号即文件 ID，单调递增
//	snap-00000001   快照文件，编号独立单调递增
//	*.tmp           临时文件，崩溃后可由恢复流程安全删除
const (
	manifestName = "manifest.toml"
	manifestTmp  = "manifest.toml.tmp"
	walPrefix    = "wal-"
	snapPrefix   = "snap-"
	tmpSuffix    = ".tmp"
)

func manifestPath(dir string) string    { return filepath.Join(dir, manifestName) }
func manifestTmpPath(dir string) string { return filepath.Join(dir, manifestTmp) }

func walName(id uint64) string {
	return walPrefix + strconv.FormatUint(id, 10)
}

func walPathOf(dir string, id uint64) string {
	return filepath.Join(dir, walName(id))
}

func snapName(id uint64) string {
	return snapPrefix + strconv.FormatUint(id, 10)
}

func snapPathOf(dir string, id uint64) string {
	return filepath.Join(dir, snapName(id))
}

// fsyncFile 强制把文件已写入数据刷到持久介质。
func fsyncFile(f *os.File) error {
	return f.Sync()
}

// fsyncDir 对目录本身 fsync，保证该目录下新建/重命名/删除的目录项变更落盘。
// 只 fsync 文件本身、不 fsync 父目录时，崩溃后新文件可能"消失"，
// 因此所有 create/rename/remove 后都必须同步目录。
func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}

// fileExists 报告路径是否存在；其它（非 NotExist）错误按"不存在"处理，
// 因为后续真正打开文件时错误会再次暴露。
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// syncRename 把 tmp 原子替换为 dst（同目录 rename 在 POSIX 上原子），
// 并 fsync 父目录让替换结果持久化。
func syncRename(dir, tmp, dst string) error {
	if err := os.Rename(tmp, dst); err != nil {
		return err
	}
	return fsyncDir(dir)
}

// removeSynced 删除文件并同步目录，确保删除结果持久化。
func removeSynced(dir, name string) error {
	if err := os.Remove(filepath.Join(dir, name)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return fsyncDir(dir)
}

// readDirEntries 是 os.ReadDir 的薄封装，便于其它文件复用。
func readDirEntries(dir string) ([]os.DirEntry, error) {
	return os.ReadDir(dir)
}
