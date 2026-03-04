package file

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
)

// ReadCsv csv文件读取
func ReadCsv(filepath string) {
	//打开文件(只读模式)，创建io.read接口实例
	opencast, err := os.Open(filepath)
	if err != nil {
		log.Println("csv文件打开失败！")
	}
	defer opencast.Close()

	//创建csv读取接口实例
	ReadCsv := csv.NewReader(opencast)

	//获取一行内容，一般为第一行内容
	read, _ := ReadCsv.Read() //返回切片类型：[chen  hai wei]
	log.Println(read)

	//读取所有内容
	ReadAll, err := ReadCsv.ReadAll() //返回切片类型：[[s s ds] [a a a]]
	log.Println(ReadAll)

	/*
	  说明：
	   1、读取csv文件返回的内容为切片类型，可以通过遍历的方式使用或Slicer[0]方式获取具体的值。
	   2、同一个函数或线程内，两次调用Read()方法时，第二次调用时得到的值为每二行数据，依此类推。
	   3、大文件时使用逐行读取，小文件直接读取所有然后遍历，两者应用场景不一样，需要注意。
	*/

}

type WriteCSV struct {
	file  *os.File
	write *csv.Writer
}

func NewWriteCSV(path string) *WriteCSV {
	//OpenFile读取文件，不存在时则创建，使用追加模式
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("文件打开失败！", err)
		return nil
	}
	return &WriteCSV{
		file:  file,
		write: csv.NewWriter(file),
	}
}

// Writer csv文件写入 (写入csv的数据需为切片类型)
func (w *WriteCSV) Write(content *[]string) {

	//写入一条数据，传入数据为切片(追加模式)
	err1 := w.write.Write(*content)
	if err1 != nil {
		fmt.Println("WriterCsv写入文件失败")
	}
	w.write.Flush() //刷新，不刷新是无法写入的
}

func (w *WriteCSV) Close() {
	w.file.Close()
}
