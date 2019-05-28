package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"img_worker"
	"regexp"
	"time"
)

type Task struct {
	TaskUUID string `json:"taskuuid"`
	URL string `json:"url"`
}

var (
	confFile string // 配置文件路径
)


// 解析命令行参数
func initArgs() {
	// worker -config ./worker.json
	// worker -h
	flag.StringVar(&confFile, "config", "./worker.json", "worker.json")
	flag.Parse()
}

func main()  {
	var (
		err error
		config clientv3.Config
		client *clientv3.Client
		watcher clientv3.Watcher
		watchRespChan <-chan clientv3.WatchResponse
		watchResp clientv3.WatchResponse
		watchEvent *clientv3.Event
		servername string
		paths interface{}
		servernameMatched bool
		jobMatched bool
		re *regexp.Regexp
		uri string
		pathsvarry []interface{}
		urlpath string
		tmppath string
		task Task
	)

	// 初始化命令行参数
	initArgs()

	// 加载配置
	if err = img_worker.InitConfig(confFile); err != nil {
		goto ERR
	}


	// 服务注册
	if err = img_worker.InitRegister(); err != nil {
		goto ERR
	}

	// 启动日志协程
	if err = img_worker.InitLogSink(); err != nil {
		goto ERR
	}

	// 启动死链处理协程
	if err = img_worker.InitDeadLindExec();err != nil{
		fmt.Println(err)
		goto ERR
	}

	// 初始化配置
	config = clientv3.Config{
		Endpoints: img_worker.G_config.EtcdEndpoints, // 集群地址
		DialTimeout: time.Duration(img_worker.G_config.EtcdDialTimeout) * time.Millisecond, // 连接超时
		Username: img_worker.G_config.EtcdUser,
		Password: img_worker.G_config.EtcdPasswd,
	}

	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}
	// 创建一个watcher
	watcher = clientv3.NewWatcher(client)
	watchRespChan = watcher.Watch(context.TODO(), "/services/", clientv3.WithPrefix())
	for watchResp = range watchRespChan {
		for _, watchEvent = range watchResp.Events {
			img_worker.G_logger.Logprint("DEBUG", "ETCD 信息：" + string(watchEvent.Type.String()) +
				" key: " + string(watchEvent.Kv.Key) + " value: " + string(watchEvent.Kv.Value))
			switch watchEvent.Type {
			case mvccpb.PUT:
				// 将kv.value反序列化
				if err = json.Unmarshal(watchEvent.Kv.Value, &task); err != nil {
					//fmt.Println("反序列化出错："+ err.Error())
					img_worker.G_logger.Logprint("ERROR", "反序列化出错："+ err.Error())
					break
				}
				for servername,paths = range img_worker.G_config.WorkerConfig.Register_Info{
					if re, err = regexp.Compile("^" + servername); err != nil{
						img_worker.G_logger.Logprint("ERROR", "regexp.Compile 出错：" + err.Error())
					}
					if servernameMatched = re.Match([]byte(task.URL)); servernameMatched{
						pathsvarry = paths.([]interface{})

						uri = re.ReplaceAllString(task.URL, "")
						if jobMatched, _ = regexp.Match(img_worker.JOB_REOCER_DIR,watchEvent.Kv.Key ); jobMatched{
							urlpath = pathsvarry[0].(string) + uri
							tmppath = pathsvarry[1].(string) + uri
							img_worker.G_logger.Logprint("INFO", "恢复文件任务：" + "urlpath:" + urlpath+" tmppath:"+ tmppath)
							go img_worker.G_executor.RunTask(task.URL,tmppath, urlpath, task.TaskUUID)
						}
						if jobMatched, _ = regexp.Match(img_worker.JOB_DELETE_DIR,watchEvent.Kv.Key ); jobMatched{
							urlpath = pathsvarry[0].(string) + uri
							tmppath = pathsvarry[1].(string) + uri
							img_worker.G_logger.Logprint("INFO", "删除文件任务：" + "urlpath:" + urlpath+" tmppath:"+ tmppath)
							go img_worker.G_executor.RunTask(task.URL, urlpath, tmppath, task.TaskUUID)
						}

					}
				}
			}
		}
	}
ERR:
	fmt.Println(err)
	img_worker.G_logger.Logprint("ERROR", err.Error())
}