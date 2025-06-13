package config

import (
	rds "github.com/redis/go-redis/v9"
)

type Option func(o *Options) error

type Options struct {
	ServerId    uint32 // binlog同步客户端唯一id
	DbHost      string
	DbPort      uint64
	DbUser      string
	DbPasswd    string
	DbNames     []string
	TableNames  []string
	RedisClient *rds.Client
}

// MonitorDataMsg 数据消息定义
type MonitorDataMsg struct {
	DbName    string
	TableName string        // 表名
	Action    MonitorAction // 消息类型
	Rows      [][]any       // mysql表的数据行数组
	TableInfo *TableInfo    // 记录表字段
}

// TableInfo 存储表结构信息
type TableInfo struct {
	Columns []ColumnInfo
}

// ColumnInfo 存储列信息
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
	Position int // 添加位置信息
}

type MonitorAction string

var (
	MonitorActionInsert MonitorAction = "INSERT"
	MonitorActionUpdate MonitorAction = "UPDATE"
	MonitorActionDelete MonitorAction = "DELETE"
)

func WithServerId(serverId uint32) Option {
	return func(o *Options) error {
		o.ServerId = serverId
		return nil
	}
}

func WithDbHost(host string) Option {
	return func(db *Options) error {
		db.DbHost = host
		return nil
	}
}

func WithDbPort(port uint64) Option {
	return func(db *Options) error {
		db.DbPort = port
		return nil
	}
}

func WithDbUser(user string) Option {
	return func(dm *Options) error {
		dm.DbUser = user
		return nil
	}
}

func WithDbPasswd(passwd string) Option {
	return func(dm *Options) error {
		dm.DbPasswd = passwd
		return nil
	}
}

func WithDbNames(dbNames []string) Option {
	return func(dm *Options) error {
		dm.DbNames = dbNames
		return nil
	}
}

func WithTableNames(tableNames []string) Option {
	return func(dm *Options) error {
		dm.TableNames = tableNames
		return nil
	}
}

func WithRedis(r *rds.Client) Option {
	return func(dm *Options) error {
		dm.RedisClient = r
		return nil
	}
}

const DataMonitorLockKeyPrefix = "data_monitor_lock:"
const DataMonitorSyncTimeKeyPrefix = "data_monitor_sync_time:"
const DataMonitorSyncPosPrefix = "data_monitor_sync_pos:"
const MaxOfflineTimeSpan = 300
const SetSyncTimeInterval = 1
const CheckRunInterval = 10
