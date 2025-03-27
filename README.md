# syncbinlog
基于go-mysql库实现的订阅mysql binlog工具，通常用来做数据同步通道
## 使用方法
````
引入包：github.com/ielevenyu/syncbinlog

在服务启动添加如下初始化代码：

func main() {
    startMonitorDb()
}

func startMonitorDb() {
	go func() {
		com := utils.NewServerCommonByRequestId("data_monitor")
		kafkaConsumer := srv_basics.NewBizDataSyncKafka(com)
		handler, err := data_monitor.NewDataMonitor(
			data_monitor.WithServerId(100000),  //可以写死一个很大的数字即可eg:100000
			data_monitor.WithDbHost("127.0.0.1"),// 数据库主库host
			data_monitor.WithDbPort(3306),//数据库端口号
			data_monitor.WithDbUser("root"),//监听账号，需要有REPLICATION SLAVE和REPLICATION CLIENT权限
			data_monitor.WithDbPasswd(""),//账号密码
			data_monitor.WithDbNames(""),//监听数据库
			data_monitor.WithTableNames([]string{}),//监听table列表
			data_monitor.WithDataConsumer(dataConsumer),//消费数据的接口实例
			data_monitor.WithRedis(),// redis指针
		)
		if err != nil {
			panic(fmt.Sprintf("startMonitorDb error: %s", err.Error()))
		}
		handler.Start()
	}()
}


消费接口样例：
需要实现syncbinlog包中定义的数据消费接口

// DataConsumer 数据消费接口定义
type DataConsumer interface {
	Handler(data *MonitorDataMsg) error
}

// BizDataSyncKafka 数据同步kafka
type BizDataSyncKafka struct {

}

func NewBizDataSyncKafka() BizDataSyncKafka {
	return BizDataSyncKafka{
	}
}

// Handler 数据处理
func (com BizDataSyncKafka) Handler(msg *data_monitor.MonitorDataMsg) error {
	fmt.Printf("receive msg: %+v", msg)
	return nil
}