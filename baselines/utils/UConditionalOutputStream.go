package utils

import (
	"fmt"
	"io"
	"os"
)

// isOutputEnabled 控制是否允许输出
var IsOutputEnabled = true

// ConditionalOutputStream 是一个条件输出流
// 当 IsOutputEnabled 为 true 时输出，否则忽略。
type ConditionalOutputStream struct {
	writer io.Writer
}

// NewConditionalOutputStream 创建一个新的 ConditionalOutputStream。
// 默认输出到标准输出（os.Stdout）
func NewConditionalOutputStream() *ConditionalOutputStream {
	return &ConditionalOutputStream{writer: os.Stdout}
}

// Print 输出任意类型的值（如果启用）
func (s *ConditionalOutputStream) Print(v ...interface{}) *ConditionalOutputStream {
	if IsOutputEnabled {
		fmt.Fprint(s.writer, v...)
	}
	return s
}

// Println 输出一行（带换行符）
func (s *ConditionalOutputStream) Println(v ...interface{}) *ConditionalOutputStream {
	if IsOutputEnabled {
		fmt.Fprintln(s.writer, v...)
	}
	return s
}

// Printf 格式化输出
func (s *ConditionalOutputStream) Printf(format string, v ...interface{}) *ConditionalOutputStream {
	if IsOutputEnabled {
		fmt.Fprintf(s.writer, format, v...)
	}
	return s
}
