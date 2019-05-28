package img_worker

import (
	"fmt"
	"log"
	"os"
)

type Logger struct {

}

var (
	G_logger *Logger
)

// 记录日志到本地文件
func (logger *Logger ) Logprint(log_level string, log_content string){
	var(
		err error
		file *os.File
		loger *log.Logger
	)
	if file, err = os.OpenFile(G_config.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666); err != nil{
		fmt.Println("打开日志文件出错：" + err.Error())
	}
	defer file.Close()

	loger = log.New(file, log_level + ": ", log.Ldate|log.Ltime|log.Lmicroseconds)
	loger.Println(log_content)
}