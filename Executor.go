package img_worker

import (
	"errors"
	"obs"
	"os"
	"os/exec"
	"path"
	//_ "github.com/go-sql-driver/mysql"
	"time"
)

// 任务执行器
type Executor struct {

}

var (
	G_executor *Executor
)


type TaskLog struct {
	WorkerName string `bson:"workname"` // 节点名称
	Status string  `bson:"status"`// 处理状态
	UpdateTime time.Time  `bson:"updatetime"`// 更新时的时间戳
	Remark string  `bson:"remark"`// 备注
}

type UpdateLog struct {
	Taskuuid string  // 任务的uuid
	TaskUrl string  // 任务处理的url
	Tasklog *TaskLog  // 任务日志内容
}

// 将处理结果更新到mongoDB中
func updateMongo(taskstatus string,reamrk string ,url string, taskuuid string) {
	var(
		tasklog *TaskLog
		updatelog *UpdateLog
	)

	tasklog = &TaskLog{
		WorkerName:G_config.WorkerName,
		Remark: reamrk,
		Status: taskstatus,
		UpdateTime:time.Now(),
	}
	updatelog = &UpdateLog{
		Taskuuid:taskuuid,
		TaskUrl:url,
		Tasklog:tasklog,
	}
	G_logSink.Append(updatelog)
}


// 移动文件
func mvfile(srcpath string, tagpath string) (err error){
	var(
		cmd *exec.Cmd
		info os.FileInfo
		tagdir string
	)
	// 获取来源文件信息
	if info, err = os.Stat(srcpath); err != nil{
		err = errors.New("获取文件信息出错:" + err.Error())
		return
	}
	// 传入文件为目录时，返回错误
	if info.IsDir(){
		err = errors.New("传入了一个文件夹")
		return
	}
	tagdir = path.Dir(tagpath)
	// 创建目标目录
	if err = os.MkdirAll(tagdir, 0755); err != nil{
		err  = errors.New("创建目标文件夹出错:" + err.Error())
		return
	}

	// 生成Cmd
	cmd = exec.Command("/bin/mv", srcpath, tagpath)
	// 执行了命令, 捕获了子进程的输出( pipe )
	if _, err = cmd.CombinedOutput(); err != nil {
		err  = errors.New("系统执行MV命令视出错:" + err.Error())
		return
	}
	return nil
}

func (executor *Executor) RunTask(url string, srcpath string, tagpath string, taskuuid string){
	var(
		err error


	)
	if err = mvfile(srcpath, tagpath); err != nil{
		G_logger.Logprint("ERROR", "移动文件出错：" + err.Error())

		updateMongo("danger", err.Error(), url, taskuuid)
	} else {

		updateMongo("success", "任务执行成功", url, taskuuid)
		G_logger.Logprint("DEBUG", "成功mv文件：" + srcpath + "==> " + tagpath)
	}
}

func OBS_Mv(srcbucket string, destbucket string, objkey string) (err error) {
	var(
		obsClient *obs.ObsClient
		cpinput *obs.CopyObjectInput
		deinput *obs.DeleteObjectInput
	)
	if obsClient, err = obs.New(HW_ACCESS_KEY_ID, HW_SECRET_ACCESS_KEY, HW_ENDPOINT); err != nil{
		return
	}
	defer obsClient.Close()
	cpinput = &obs.CopyObjectInput{
		CopySourceBucket: srcbucket,
		CopySourceKey: objkey,
	}
	cpinput.Key = objkey
	cpinput.Bucket = destbucket
	if _,err = obsClient.CopyObject(cpinput);err != nil{
		return
	}
	deinput = &obs.DeleteObjectInput{
		Key:objkey,
		Bucket:srcbucket,
	}
	if _, err= obsClient.DeleteObject(deinput);err != nil{
		return
	}
	return nil
}


func (executor *Executor) RunOBSTask(url string, srcpbucket string, destbucket string, objkey string, taskuuid string){
	var(
		err error
	)
	if err = OBS_Mv(srcpbucket, destbucket,objkey ); err != nil{
		//fmt.Println(err)
		G_logger.Logprint("ERROR", "移动文件出错：" + err.Error())

		updateMongo("danger", err.Error(), url, taskuuid)
	} else {
		updateMongo("success", "任务执行成功", url, taskuuid)
		G_logger.Logprint("DEBUG", "成功mv文件：" + srcpbucket + ":" + objkey + " ==> " + destbucket)
	}
}