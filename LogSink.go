package img_worker

import (
	"context"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

// mongodb存储日志
type LogSink struct {
	client *mongo.Client
	logCollection *mongo.Collection
	logChan chan *UpdateLog
	deadlinksCollection *mongo.Collection
	deadlogChan chan *UpdateDeadLinkTaskLog
}


var (
// 单例
G_logSink *LogSink
)

// 日志存储协程
func (logSink *LogSink) writeLoop() {
	var(
		log *UpdateLog
		err error
		deadlog *UpdateDeadLinkTaskLog
	)
	for {
		select {
		case log = <-logSink.logChan:
			if _, err = logSink.logCollection.UpdateOne(context.TODO(), bson.M{"taskuuid":log.Taskuuid, "urls.url":log.TaskUrl}, bson.M{"$push":bson.M{"urls.$.taskinfo":log.Tasklog}});err != nil{
				fmt.Println(err)
				G_logger.Logprint("DEBUG", "上传日志到Mongodb出错" + err.Error())
			}

		case deadlog = <-logSink.deadlogChan:
			if _, err = logSink.deadlinksCollection.UpdateOne(context.TODO(), bson.M{"taskuuid":deadlog.Taskuuid, "urls.url":deadlog.TaskUrl}, bson.M{"$push":bson.M{"urls.$.taskinfo":deadlog.Deadlinklog}});err != nil{
				fmt.Println(err)
				G_logger.Logprint("DEBUG", "上传日志到Mongodb出错" + err.Error())
			}
		}
	}
}


func InitLogSink() (err error) {
	var (
		client *mongo.Client
		ctx context.Context

	)

	// 建立mongodb连接
	// 1, 建立连接
	ctx, _ = context.WithTimeout(context.Background(), 5*time.Second)
	if client, err = mongo.Connect(ctx, options.Client().ApplyURI(MONGODB_URI)); err != nil {
		fmt.Println(err)
		return
	}
	// 2, 选择数据库my_db,选择表my_collection
	//   选择db和collection
	G_logSink = &LogSink{
		client: client,
		logCollection:  client.Database("task").Collection("taskhistory"),
		deadlinksCollection: client.Database("task").Collection("deadlinkshistorty"),
		logChan: make(chan *UpdateLog, 10000),
		deadlogChan:make(chan *UpdateDeadLinkTaskLog, 10000),
	}

	// 启动一个mongodb处理协程
	go G_logSink.writeLoop()
	return
}


// 发送文件处理日志
func (logSink *LogSink) Append(updateLog *UpdateLog) {
	select {
	case logSink.logChan <- updateLog:
	default:
		// 队列满了就丢弃
	}
}

// 发送死链处理日志
func (logSink *LogSink) AppendDeadlog(updatedeadLog *UpdateDeadLinkTaskLog) {
	select {
	case logSink.deadlogChan <- updatedeadLog:
	default:
		// 队列满了就丢弃
	}
}