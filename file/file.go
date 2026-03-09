package file

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
)

const (
	fileSize            = 1024 // 1 KB
	POSIX_FADV_DONTNEED = 4
)

// fadvise64: 根据当前架构选择 syscall 编号
func fadvise(fd int, offset, length int64, advice int) error {
	var SYS_FADVISE64 uintptr
	switch runtime.GOARCH {
	case "amd64":
		SYS_FADVISE64 = 221
	case "arm64":
		SYS_FADVISE64 = 223
		//fmt.Println("SYS_FADVISE64", SYS_FADVISE64)
	default:
		return fmt.Errorf("unsupported architecture: %s", runtime.GOARCH)
	}

	_, _, errno := syscall.Syscall6(
		SYS_FADVISE64,
		uintptr(fd),
		uintptr(offset),
		uintptr(length),
		uintptr(advice),
		0, 0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

func WriteFiles(dir string, n int) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}
	buf := make([]byte, fileSize)
	for i := 0; i < n; i++ {
		if _, err := rand.Read(buf); err != nil {
			return err
		}
		name := filepath.Join(dir, fmt.Sprintf("file_%03d.bin", i))
		f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			return err
		}
		if _, err := f.Write(buf); err != nil {
			f.Close()
			return err
		}
		// 强制写入磁盘
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
		// 建议内核不要保留该文件的页缓存（写完后丢弃缓存）
		fd := int(f.Fd())
		_ = fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) // 写后清缓存
		f.Close()
	}
	return nil
}

// ReadOnce 打开并读取整个文件，然后在读后调用 POSIX_FADV_DONTNEED 以尽量清除缓存。
func ReadOnce(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	buf := make([]byte, fileSize)
	_, err = io.ReadFull(f, buf)
	if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return err
	}
	// 读后建议内核丢弃缓存页，这样下次再读同一个文件或其他文件时更可能触发磁盘IO
	fd := int(f.Fd())
	_ = fadvise(fd, 0, 0, POSIX_FADV_DONTNEED) // 读后清缓存
	return nil
}
