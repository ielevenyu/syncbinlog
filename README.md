## syncbinlog
基于go-mysql库实现的订阅mysql binlog工具，通常用来做数据同步通道
## feature
1、支持分布式环境下binlog订阅，确保只有一台容器同步。

2、支持错误自动恢复，故障切换，同步binlog容器发送故障后可以自动切换到其他容器同步binlog

3、支持多库、多表监听同步，松耦合接口方式消费数据。

## start
````
引入包：github.com/ielevenyu/syncbinlog@v1.0.0

在服务启动添加如下初始化代码：

func main() {
    startMonitorDb()
}

func startMonitorDb() {
	go func() {
		kafkaConsumer := NewBizDataSyncKafka()
		handler, err := syncbinlog.NewDataMonitor(
			syncbinlog.WithServerId(100000),  //可以写死一个很大的数字即可eg:100000
			syncbinlog.WithDbHost("127.0.0.1"),// 数据库主库host
			syncbinlog.WithDbPort(3306),//数据库端口号
			syncbinlog.WithDbUser("root"),//监听账号，需要有REPLICATION SLAVE和REPLICATION CLIENT权限
			syncbinlog.WithDbPasswd(""),//账号密码
			syncbinlog.WithDbNames([]string{}),//监听数据库列表
			syncbinlog.WithTableNames([]string{}),//监听table列表
			syncbinlog.WithDataConsumer(dataConsumer),//消费数据的接口实例
			syncbinlog.WithRedis(),// redis指针
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
