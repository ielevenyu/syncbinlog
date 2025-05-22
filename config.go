package syncbinlog

import (
	"github.com/go-mysql-org/go-mysql/replication"
	rds "github.com/redis/go-redis/v9"
	"github.com/siddontang/go-log/loggers"
)

type dataMonitor struct {
	serverId        uint32 // binlog同步客户端唯一id
	dbHost          string
	dbPort          uint64
	dbUser          string
	dbPasswd        string
	dbNames         []string
	tableNames      []string
	redis           *rds.Client
	Logger          loggers.Advanced
	syncer          *replication.BinlogSyncer
	dataConsumer    DataConsumer
	tables          map[string]bool
	dbs             map[string]bool
	lastSynTime     int64
	lastSynPos      uint32
	lastSyncBinFile string
}

// DataMonitorIter 数据监听接口定义
type DataMonitorIter interface {
	Start()
}

type MonitorAction string

var (
	MonitorActionInsert MonitorAction = "INSERT"
	MonitorActionUpdate MonitorAction = "UPDATE"
	MonitorActionDelete MonitorAction = "DELETE"
)

// MonitorDataMsg 数据消息定义
type MonitorDataMsg struct {
	DbName    string
	TableName string        // 表名
	Action    MonitorAction // 消息类型
	Rows      [][]any       // mysql表的数据行数组
}

// DataConsumer 数据消费接口定义
type DataConsumer interface {
	Handler(data *MonitorDataMsg) error
}

type Option func(dm *dataMonitor) error

func WithServerId(serverId uint32) Option {
	return func(db *dataMonitor) error {
		db.serverId = serverId
		return nil
	}
}

func WithDbHost(host string) Option {
	return func(db *dataMonitor) error {
		db.dbHost = host
		return nil
	}
}

func WithDbPort(port uint64) Option {
	return func(db *dataMonitor) error {
		db.dbPort = port
		return nil
	}
}

func WithDbUser(user string) Option {
	return func(dm *dataMonitor) error {
		dm.dbUser = user
		return nil
	}
}

func WithDbPasswd(passwd string) Option {
	return func(dm *dataMonitor) error {
		dm.dbPasswd = passwd
		return nil
	}
}

func WithDbNames(dbNames []string) Option {
	return func(dm *dataMonitor) error {
		dm.dbNames = dbNames
		return nil
	}
}

func WithTableNames(tableNames []string) Option {
	return func(dm *dataMonitor) error {
		dm.tableNames = tableNames
		return nil
	}
}

func WithRedis(r *rds.Client) Option {
	return func(dm *dataMonitor) error {
		dm.redis = r
		return nil
	}
}

func WithDataConsumer(c DataConsumer) Option {
	return func(dm *dataMonitor) error {
		dm.dataConsumer = c
		return nil
	}
}

const DataMonitorLockKeyPrefix = "data_monitor_lock:"
const DataMonitorSyncTimeKeyPrefix = "data_monitor_sync_time:"
const DataMonitorSyncPosPrefix = "data_monitor_sync_pos:"
const MaxOfflineTimeSpan = 300
const SetSyncTimeInterval = 10
const CheckRunInterval = 10
