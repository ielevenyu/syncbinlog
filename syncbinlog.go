package syncbinlog

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/ielevenyu/syncbinlog/config"
	"github.com/ielevenyu/syncbinlog/dataparse"
	rds "github.com/redis/go-redis/v9"
	"github.com/siddontang/go-log/log"
	"github.com/siddontang/go-log/loggers"
)

// DataMonitorIter 数据监听接口定义
type DataMonitorIter interface {
	Start()
	Close()
}

type dataMonitor struct {
	opts             *config.Options
	Logger           loggers.Advanced
	syncer           *replication.BinlogSyncer
	dataConsumer     DataConsumer
	tables           map[string]bool
	dbs              map[string]bool
	lastSynTime      int64
	lastSynPos       uint32
	lastSyncBinFile  string
	tableColumnCache map[string]*config.TableInfo
	db               *sql.DB
}

// DataConsumer 数据消费接口定义
type DataConsumer interface {
	Handler(data *config.MonitorDataMsg) error
}

func NewDataMonitor(opts ...config.Option) (DataMonitorIter, error) {
	dm := &dataMonitor{
		tableColumnCache: make(map[string]*config.TableInfo),
		opts:             &config.Options{},
	}
	for _, opt := range opts {
		err := opt(dm.opts)
		if err != nil {
			return nil, err
		}
	}
	if dm.opts.RedisClient == nil {
		return nil, fmt.Errorf("redis is nil")
	}
	if len(dm.opts.DbNames) == 0 {
		return nil, fmt.Errorf("dbNames is empty")
	}
	if len(dm.opts.DbHost) == 0 {
		return nil, fmt.Errorf("dbHost is empty")
	}
	if dm.opts.DbPort == 0 {
		return nil, fmt.Errorf("dbPort invalid")
	}
	if len(dm.opts.DbUser) == 0 {
		return nil, fmt.Errorf("dbUser is empty")
	}
	if len(dm.opts.DbPasswd) == 0 {
		return nil, fmt.Errorf("dbPasswd is empty")
	}
	if dm.Logger == nil {
		streamHandler, _ := log.NewStreamHandler(os.Stdout)
		dm.Logger = log.NewDefault(streamHandler)
	}
	dm.Logger.Infof("start NewDataMonitor cfg: %+v", dm)
	dm.newBinlogSyncer()
	if dm.syncer == nil {
		return nil, fmt.Errorf("NewBinlogSyncer error")
	}
	dm.fillDbTables()
	var err error
	dm.db, err = sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s",
		dm.opts.DbUser, dm.opts.DbPasswd, dm.opts.DbHost, dm.opts.DbPort, dm.opts.DbNames[0]))
	if err != nil {
		return nil, fmt.Errorf("connect db error: %s", err.Error())
	}
	dm.dataConsumer = dataparse.NewBizDataSync()
	return dm, nil
}

func (dm *dataMonitor) newBinlogSyncer() {
	cfg := replication.BinlogSyncerConfig{
		ServerID: dm.opts.ServerId,
		Flavor:   "mysql",
		Host:     dm.opts.DbHost,
		Port:     uint16(dm.opts.DbPort),
		User:     dm.opts.DbUser,
		Password: dm.opts.DbPasswd,
	}
	dm.syncer = replication.NewBinlogSyncer(cfg)
}

func (dm *dataMonitor) Start() {
	defer func() {
		dm.syncer.Close()
		dm.db.Close()
		if r := recover(); r != nil {
			dm.Logger.Errorf("dataMonitor panic err: %s", string(debug.Stack()))
		}
	}()
	ok, err := dm.checkRun()
	if err != nil || !ok { // 实例不运行
		dm.Logger.Infof("checkRun fail err: %v", ok, err)
		return
	}
	dm.Logger.Infof("dataMonitor start run.......")
	lastSyncTime, err := dm.getSyncLastTime()
	dm.Logger.Debugf("getSyncLastTime is: %v", time.Unix(lastSyncTime, 0).Local())
	var startSynTime time.Time
	if err != nil || lastSyncTime == 0 {
		startSynTime = time.Now()
	} else {
		startSynTime = time.Unix(lastSyncTime, 0).Local()
	}
	cacheBinlogPos, _ := dm.getSyncPos()
	dm.Logger.Infof("缓存binlog文件位置： %v", cacheBinlogPos)
	dm.Logger.Debugf("startSynTime is: %v", startSynTime)
	streamer, err := dm.syncer.StartSync(mysql.Position{Name: cacheBinlogPos.Name, Pos: cacheBinlogPos.Pos})
	if err != nil {
		dm.Logger.Errorf("Failed to start sync with error: %v", err)
		return
	}
	// 开启续约
	go dm.renewal()
	var ev *replication.BinlogEvent
	for {
		//ev, err = streamer.GetEvent(context.Background())
		ev, err = streamer.GetEventWithStartTime(context.Background(), startSynTime)
		if err != nil { // 这里失败了不退出，继续下一次循环
			dm.Logger.Errorf("Failed to get event with error: %v", err)
			lastSyncTime, err = dm.getSyncLastTime()
			dm.Logger.Debugf("getSyncLastTime is: %v", time.Unix(lastSyncTime, 0).Local())
			if err != nil || lastSyncTime == 0 {
				startSynTime = time.Now()
			} else {
				startSynTime = time.Unix(lastSyncTime, 0).Local()
			}
			dm.Logger.Debugf("startSynTime is: %v", startSynTime)
			dm.syncer.Close()
			dm.newBinlogSyncer()
			//streamer, err = dm.syncer.StartSync(mysql.Position{Name: cacheBinlogPos.Name, Pos: 0})
			streamer, err = dm.syncer.StartSync(mysql.Position{})
			if err != nil {
				dm.Logger.Errorf("Failed to StartSync error: %v", err)
			}
			continue
		}
		if ev == nil { // 跳过比当前时间早的事件
			continue
		}
		switch e := ev.Event.(type) {
		case *replication.RowsEvent:
			if dm.checkDb(string(e.Table.Schema)) && dm.checkTable(string(e.Table.Table)) {
				tableInfo := dm.getTableStruct(string(e.Table.Schema), string(e.Table.Table))
				dm.Logger.Infof("RowsEvent schema: %s, table: %s, eventType: %v", string(e.Table.Schema), string(e.Table.Table), ev.Header.EventType)
				msg := &config.MonitorDataMsg{
					Rows:      e.Rows,
					DbName:    string(e.Table.Schema),
					TableName: string(e.Table.Table),
					TableInfo: tableInfo,
				}
				switch ev.Header.EventType {
				case replication.WRITE_ROWS_EVENTv2, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv0:
					msg.Action = config.MonitorActionInsert
					dm.dataConsumer.Handler(msg)
					dm.Logger.Infof("rows data: %v", e.Rows)
				case replication.UPDATE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv0:
					msg.Action = config.MonitorActionUpdate
					dm.dataConsumer.Handler(msg)
					dm.Logger.Infof("rows data: %v", e.Rows)
				case replication.DELETE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv0:
					msg.Action = config.MonitorActionDelete
					dm.dataConsumer.Handler(msg)
					dm.Logger.Infof("rows data: %v", e.Rows)
				default:
				}
			}
			currentSynTimeStamp := int64(ev.Header.Timestamp)
			if currentSynTimeStamp > dm.lastSynTime && ev.Header.Timestamp%config.SetSyncTimeInterval == 0 {
				currentPos := dm.syncer.GetNextPosition()
				dm.lastSynTime = currentSynTimeStamp
				dm.setSyncLastTime(currentSynTimeStamp - 1) //减1s避免异常情况漏binlog日志
				if currentPos.Pos > 0 {
					dm.lastSynPos = currentPos.Pos
					dm.lastSyncBinFile = currentPos.Name
					dm.setSyncPos(currentPos)
				}
			}
		}
	}
}

func (dm *dataMonitor) fillDbTables() {
	dm.tables = make(map[string]bool)
	dm.dbs = make(map[string]bool)
	// fill monitor table
	if len(dm.opts.TableNames) > 0 {
		for _, table := range dm.opts.TableNames {
			dm.tables[table] = true
		}
	}
	// fill monitor db
	if len(dm.opts.DbNames) > 0 {
		for _, db := range dm.opts.DbNames {
			dm.dbs[db] = true
		}
	}
}

func (dm *dataMonitor) checkTable(tableName string) bool {
	if len(dm.tables) == 0 {
		return true
	}
	_, ok := dm.tables[tableName]
	return ok
}

func (dm *dataMonitor) checkDb(dbName string) bool {
	if len(dm.dbs) == 0 {
		return true
	}
	_, ok := dm.dbs[dbName]
	return ok
}

func (dm *dataMonitor) checkRun() (bool, error) {
	lockKey := fmt.Sprintf("%s%d", config.DataMonitorLockKeyPrefix, dm.opts.ServerId)
	ok, err := dm.opts.RedisClient.SetNX(context.Background(), lockKey, 1, time.Second*config.MaxOfflineTimeSpan).Result()
	if err != nil {
		return false, err
	}
	if ok { // 没有实例运行，则运行
		dm.Logger.Infof("checkRun ok key: %s", lockKey)
		return true, nil
	}
	ticker := time.NewTicker(config.CheckRunInterval * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_, err = dm.opts.RedisClient.Get(context.Background(), lockKey).Int64()
			if err != nil && err != rds.Nil {
				return false, err
			}
			dm.Logger.Debugf("checkRun lockKey: %s running....", lockKey)
			if err == rds.Nil { // MaxOfflineTimeSpan时间没更新则启动该实例
				ok, _ = dm.opts.RedisClient.SetNX(context.Background(), lockKey, 1, time.Second*config.MaxOfflineTimeSpan).Result()
				if ok {
					return true, nil
				}
			}
		}
	}
}

func (dm *dataMonitor) setSyncPos(position mysql.Position) error {
	redisKey := fmt.Sprintf("%s%d", config.DataMonitorSyncPosPrefix, dm.opts.ServerId)
	values, err := json.Marshal(position)
	if err != nil {
		return err
	}
	return dm.opts.RedisClient.Set(context.Background(), redisKey, string(values), 0).Err()
}

func (dm *dataMonitor) getSyncPos() (mysql.Position, error) {
	position := mysql.Position{}
	redisKey := fmt.Sprintf("%s%d", config.DataMonitorSyncPosPrefix, dm.opts.ServerId)
	values, err := dm.opts.RedisClient.Get(context.Background(), redisKey).Bytes()
	if err != nil && err != rds.Nil {
		return position, err
	}
	if len(values) == 0 {
		return position, nil
	}
	err = json.Unmarshal(values, &position)
	if err != nil {
		return position, err
	}
	return position, nil
}

func (dm *dataMonitor) setSyncLastTime(t int64) error {
	redisKey := fmt.Sprintf("%s%d", config.DataMonitorSyncTimeKeyPrefix, dm.opts.ServerId)
	return dm.opts.RedisClient.Set(context.Background(), redisKey, t, 0).Err()
}

func (dm *dataMonitor) getSyncLastTime() (int64, error) {
	redisKey := fmt.Sprintf("%s%d", config.DataMonitorSyncTimeKeyPrefix, dm.opts.ServerId)
	lastTime, err := dm.opts.RedisClient.Get(context.Background(), redisKey).Int64()
	if err != nil && err != rds.Nil {
		return 0, err
	}
	return lastTime, nil
}

func (dm *dataMonitor) renewal() {
	defer func() {
		if r := recover(); r != nil {
			dm.Logger.Errorf("renewal recover err: %s", string(debug.Stack()))
		}
	}()
	tick := time.NewTicker(5 * time.Second)
	key := fmt.Sprintf("%s%d", config.DataMonitorLockKeyPrefix, dm.opts.ServerId)
	for {
		select {
		case <-tick.C:
			dm.opts.RedisClient.Expire(context.Background(), key, time.Second*config.MaxOfflineTimeSpan).Err()
		}
	}
}

func (dm *dataMonitor) getTableStruct(dbName string, tableName string) *config.TableInfo {
	key := fmt.Sprintf("%s.%s", dbName, tableName)

	if table, exists := dm.tableColumnCache[key]; exists {
		return table
	}

	table, err := dm.loadTableStructure(dbName, tableName)
	if err != nil {
		return nil
	}

	dm.tableColumnCache[key] = table
	return table
}

func (dm *dataMonitor) loadTableStructure(dbName string, tableName string) (*config.TableInfo, error) {
	query := `
		SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
		ORDER BY ORDINAL_POSITION
	`
	rows, err := dm.db.Query(query, dbName, tableName)
	if err != nil {
		return nil, fmt.Errorf("query table info: %v", err)
	}
	defer rows.Close()

	var info config.TableInfo
	for rows.Next() {
		var col config.ColumnInfo
		var nullable string
		if err := rows.Scan(&col.Name, &col.Type, &nullable, &col.Position); err != nil {
			return nil, fmt.Errorf("scan column info: %v", err)
		}
		col.Nullable = nullable == "YES"
		info.Columns = append(info.Columns, col)
	}
	return &info, nil
}

func (dm *dataMonitor) Close() {
	dm.syncer.Close()
	dm.db.Close()
}
