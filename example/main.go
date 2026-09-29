// 最小可嵌入示例：写入、覆盖、读取、删除、手动回收与重启恢复。
package main

import (
	"errors"
	"fmt"
	"log"

	kv "github.com/highcumontoa/embedded-kv-storage-engine-go"
)

func main() {
	dir := "./data"

	s, err := kv.Open(dir, kv.WithAutoCompactThreshold(4<<20))
	if err != nil {
		// 重启遇到损坏不会静默跳过：尾部不完整可显式修复，其余类别需人工介入。
		if errors.Is(err, kv.ErrIncompleteTail) {
			if _, rerr := kv.Repair(dir); rerr != nil {
				log.Fatal(rerr)
			}
			s, err = kv.Open(dir)
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	defer s.Close()

	if err := s.Put([]byte("hello"), []byte("world")); err != nil {
		log.Fatal(err)
	}
	if err := s.Put([]byte("hello"), []byte("WORLD")); err != nil { // 覆盖：旧版本立即不可见
		log.Fatal(err)
	}
	v, ok, err := s.Get([]byte("hello"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("hello=%q ok=%v live=%d\n", v, ok, s.Len())

	if err := s.Delete([]byte("hello")); err != nil { // 删除：墓碑防止旧值复活
		log.Fatal(err)
	}
	if err := s.Compact(); err != nil { // 回收墓碑与旧版本
		log.Fatal(err)
	}
}
