package img_worker

import (
	"context"
	"encoding/json"
	"fmt"
	"go.etcd.io/etcd/clientv3"
	"time"
)

// 注册节点到etcd： /cron/workers/worker名
type Register struct {
	client *clientv3.Client
	kv clientv3.KV
	lease clientv3.Lease
}

var (
	G_register *Register
)

// 注册到/cron/workers/IP, 并自动续租
func (register *Register) keepOnline() {
	var (
		regKey string
		leaseGrantResp *clientv3.LeaseGrantResponse
		err error
		cancelCtx context.Context
		cancelFunc context.CancelFunc
		registerInfo []byte
	)

	for {
		// 注册路径
		regKey = JOB_WORKER_DIR + G_config.WorkerName
		cancelFunc = nil

		// 创建租约
		if leaseGrantResp, err = register.lease.Grant(context.TODO(), 10); err != nil {
			goto RETRY
		}

		cancelCtx, cancelFunc = context.WithCancel(context.TODO())

		if registerInfo, err = json.Marshal(G_config.WorkerConfig) ; err != nil{
			goto RETRY
		}
		// 注册到etcd
		if _, err = register.kv.Put(cancelCtx, regKey, string(registerInfo), clientv3.WithLease(leaseGrantResp.ID)); err != nil {
			fmt.Println(err)
			goto RETRY
		}

		time.Sleep(7 * time.Second)  // 休眠7秒，重新注册


	RETRY:
		time.Sleep(1 * time.Second)
		if cancelFunc != nil {
			cancelFunc()
		}
	}
}

func InitRegister() (err error) {
	var (
		config clientv3.Config
		client *clientv3.Client
		kv clientv3.KV
		lease clientv3.Lease
	)

	// 初始化配置
	config = clientv3.Config{
		Endpoints: G_config.EtcdEndpoints, // 集群地址
		DialTimeout: time.Duration(G_config.EtcdDialTimeout) * time.Millisecond, // 连接超时
		Username: G_config.EtcdUser,
		Password: G_config.EtcdPasswd,
	}

	// 建立连接
	if client, err = clientv3.New(config); err != nil {
		return
	}


	// 得到KV和Lease的API子集
	kv = clientv3.NewKV(client)
	lease = clientv3.NewLease(client)

	G_register = &Register{
		client: client,
		kv: kv,
		lease: lease,
	}

	// 服务注册协程
	go G_register.keepOnline()

	return
}