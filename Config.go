package img_worker

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"io/ioutil"
	"time"
)

// 通过ETCD获取的配置信息
type ConfigInfo struct {
	ConfigName string `json:"config_name"`
	Updatetime string `json:"config_updatetime"`
	Register_Info map[string]interface{} `json:"config"`
}
// 程序配置
type Config struct {
	EtcdEndpoints []string `json:"etcdEndpoints"`
	EtcdDialTimeout int `json:"etcdDialTimeout"`
	EtcdUser string `json:"etcdUser"`
	EtcdPasswd string `json:"etcdPasswd"`
	WorkerName string `json:"worker_name"`
	LogFile string `json:"logFile"`
	WorkerConfig ConfigInfo
}


const (

	// 服务注册目录
	JOB_WORKER_DIR = "/online_work/"

	// 恢复任务保存目录
	JOB_REOCER_DIR = "/services/recoerjob/"

	// 删除任务保存目录
	JOB_DELETE_DIR = "/services/deletejob/"

	// 死链保存目录
	DEADLINKSDIR = "/deadlinks/"

	// 死链文件名
	DEADFILENAME = "deadlinks.txt"

	// 配置保存目录
	CONFIG_DIR = "/worker_config/"

	// 华为OBS连接信息
	HW_ACCESS_KEY_ID = "HW_ACCESS_KEY_ID"
	HW_SECRET_ACCESS_KEY = "HW_SECRET_ACCESS_KEY"
	HW_ENDPOINT = "HW_ENDPOINT"

	MONGODB_URI = "mongodb://mongouser:passwd@192.168.15.123:27017/task"
)

var (
	// 单例
	G_config *Config
)



// 加载配置
func InitConfig(filename string) (err error) {
	var (
		content []byte
		conf Config
	)

	// 1, 把配置文件读进来
	if content, err = ioutil.ReadFile(filename); err != nil {
		return
	}

	// 2, 做JSON反序列化
	if err = json.Unmarshal(content, &conf); err != nil {
		return
	}

	// 3, 赋值单例
	G_config = &conf

	// 获取Etcd上配置
	Getconfig ()
	return
}


func Getconfig ()  {
	var (
		etcdconfig clientv3.Config
		err error
		client *clientv3.Client
		getresponse *clientv3.GetResponse
		kv clientv3.KV
		cwatcher clientv3.Watcher
		cwatchRespChan <-chan clientv3.WatchResponse
		cwatchResp clientv3.WatchResponse
		cwatchEvent *clientv3.Event

	)

	etcdconfig = clientv3.Config{
		Endpoints: G_config.EtcdEndpoints, // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, // 连接超时
		Username: G_config.EtcdUser,
		Password: G_config.EtcdPasswd,
	}

	// 建立连接
	if client, err = clientv3.New(etcdconfig); err != nil {
		return
	}
	// 获得kv API子集
	kv = clientv3.NewKV(client)
	fmt.Println(CONFIG_DIR + G_config.WorkerName)
	if getresponse, err = kv.Get(context.TODO(), CONFIG_DIR + G_config.WorkerName); err != nil{
		fmt.Println(err)
	}
	if getresponse.Count == 0 {
		fmt.Println("无法通过etcd获取配置信息")
		panic("无法通过etcd获取配置信息")
	} else {
		if err = json.Unmarshal([]byte(getresponse.Kvs[0].Value), &G_config.WorkerConfig); err != nil {
			fmt.Println("反序列化出错："+ err.Error())
		}
	}

	// 启动Goroutine，实时获取etcd中配置信息
	go func() {
		cwatcher = clientv3.NewWatcher(client)
		cwatchRespChan = cwatcher.Watch(context.TODO(), CONFIG_DIR + G_config.WorkerName)
		for cwatchResp = range cwatchRespChan {
			for _, cwatchEvent = range cwatchResp.Events {
				switch cwatchEvent.Type {
				case mvccpb.PUT:
					// 将kv.value反序列化
					if err = json.Unmarshal(cwatchEvent.Kv.Value, &G_config.WorkerConfig);err != nil{
						fmt.Println(err)
					}
					fmt.Println(G_config.WorkerConfig)
				}
			}
		}
	}()
}