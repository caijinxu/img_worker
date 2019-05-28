package img_worker

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"os"
	"regexp"
	"time"
)

// 死链任务执行器
type DaedLinkExec struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
	watcher clientv3.Watcher
}

// 接收ETCD任务结构体
type DeadLinksTask struct {
	TaskUUID string `json:"taskuuid"`
	URL string `json:"url"`
}

var (
	G_deadlinexec *DaedLinkExec
)

type DeadLinkTaskLog struct {
	WorkerName string `bson:"workname"` // 节点名称
	Status string  `bson:"status"`// 处理状态
	UpdateTime time.Time  `bson:"updatetime"`// 更新时的时间戳
	Remark string  `bson:"remark"`// 备注
}

type UpdateDeadLinkTaskLog struct {
	Deadlinklog *DeadLinkTaskLog
	Taskuuid string // 任务uuid
	TaskUrl string  // 处理的url
}

func updateDeadlinkMongo(taskstatus string, reamrk string ,url string, taskuuid string) {
	var(
		tasklog *DeadLinkTaskLog
		updatedeadlog *UpdateDeadLinkTaskLog
	)

	tasklog = &DeadLinkTaskLog{
		WorkerName:G_config.WorkerName,
		Remark: reamrk,
		Status: taskstatus,
		UpdateTime:time.Now(),
	}
	updatedeadlog = &UpdateDeadLinkTaskLog{
		Taskuuid:taskuuid,
		TaskUrl:url,
		Deadlinklog:tasklog,
	}
	G_logSink.AppendDeadlog(updatedeadlog)
}

func SendDeadlinkToFile(deadfile string, deadlink string) (err error){
	var(
		file *os.File
	)
	if file, err = os.OpenFile(deadfile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0664); err != nil{
		fmt.Println("打开死链文件出错：" + err.Error())
		return
	}
	defer file.Close()
	if _, err = file.WriteString(deadlink + "\n"); err != nil{
		return
	}
	return nil
}

func addDeadlinks(deadfile string, deadlink string, taskuuid string){
	var(
		err error
		deadurl string
	)
	deadurl = string(deadlink)
	if err = SendDeadlinkToFile(deadfile, deadlink); err != nil{
		//fmt.Println(err.Error())

		updateDeadlinkMongo("danger", err.Error(), deadurl, taskuuid)
	} else {
		updateDeadlinkMongo("success", "任务执行成功", deadurl, taskuuid)
		G_logger.Logprint("DEBUG", "死链成功添加：" + deadurl + "==> " + deadfile)
	}
}

func (DaedLinkExec *DaedLinkExec) whatchDeadlinks(){
	var(
		dwatchResp clientv3.WatchResponse
		dwatchEvent *clientv3.Event
		dwatchRespChan <-chan clientv3.WatchResponse
		dtask *DeadLinksTask
		err error
		servername string
		paths interface{}
		servernameMatched bool
		re *regexp.Regexp
		pathsvarry []interface{}
		deadfile string
		dconfig clientv3.Config
		dclient *clientv3.Client
		dwatcher clientv3.Watcher
	)
	// 初始化配置
	dconfig = clientv3.Config{
		Endpoints: G_config.EtcdEndpoints, // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, // 连接超时
		Username: G_config.EtcdUser,
		Password: G_config.EtcdPasswd,
	}

	// 建立连接
	if dclient, err = clientv3.New(dconfig); err != nil {
		return
	}

	// 监听/deadlinks/目录
	go func() {

		dwatcher = clientv3.NewWatcher(dclient)
		dwatchRespChan = dwatcher.Watch(context.TODO(), DEADLINKSDIR, clientv3.WithPrefix())
		for dwatchResp = range dwatchRespChan {
			for _, dwatchEvent = range dwatchResp.Events {
				G_logger.Logprint("DEBUG", "ETCD 信息：" + string(dwatchEvent.Type.String()) + " key: " + string(dwatchEvent.Kv.Key) + " value: " + string(dwatchEvent.Kv.Value))
				switch dwatchEvent.Type {
				case mvccpb.PUT: // 获取死链任务
					if err = json.Unmarshal(dwatchEvent.Kv.Value, &dtask); err != nil {
						G_logger.Logprint("ERROR", "反序列化出错："+ err.Error())
						break
					}
					for servername, paths = range G_config.WorkerConfig.Register_Info {
						if re, err = regexp.Compile("^" + servername); err != nil {
							G_logger.Logprint("ERROR", "regexp.Compile 出错："+err.Error())
						}
						if servernameMatched = re.Match([]byte(dtask.URL)); servernameMatched {
							pathsvarry = paths.([]interface{})
							deadfile = pathsvarry[0].(string) + DEADFILENAME
							go addDeadlinks(deadfile, dtask.URL, dtask.TaskUUID)
						}
					}
				}
			}
		}
	}()
}

// 初始化死链任务处理器
func InitDeadLindExec()(err error) {
	var (
		dconfig clientv3.Config
		dclient *clientv3.Client
		dwatcher clientv3.Watcher
	)

	// 初始化配置
	dconfig = clientv3.Config{
		Endpoints: G_config.EtcdEndpoints, // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, // 连接超时
		Username: G_config.EtcdUser,
		Password: G_config.EtcdPasswd,
	}

	// 建立连接
	if dclient, err = clientv3.New(dconfig); err != nil {
		return
	}

	// 得到KV和Lease的API子集
	dwatcher = clientv3.NewWatcher(dclient)

	// 赋值单例
	G_deadlinexec = &DaedLinkExec{
		client: dclient,
		watcher: dwatcher,
	}
	G_deadlinexec.whatchDeadlinks()
	return
}

