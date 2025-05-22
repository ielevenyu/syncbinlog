package syncbinlog

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime/debug"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	rds "github.com/redis/go-redis/v9"
	"github.com/siddontang/go-log/log"
)

func NewDataMonitor(opts ...Option) (DataMonitorIter, error) {
	dm := &dataMonitor{}
	for _, opt := range opts {
		err := opt(dm)
		if err != nil {
			return nil, err
		}
	}
	if dm.redis == nil {
		return nil, fmt.Errorf("redis is nil")
	}
	if len(dm.dbNames) == 0 {
		return nil, fmt.Errorf("dbNames is empty")
	}
	if len(dm.dbHost) == 0 {
		return nil, fmt.Errorf("dbHost is empty")
	}
	if dm.dbPort == 0 {
		return nil, fmt.Errorf("dbPort invalid")
	}
	if len(dm.dbUser) == 0 {
		return nil, fmt.Errorf("dbUser is empty")
	}
	if len(dm.dbPasswd) == 0 {
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
	return dm, nil
}

func (dm *dataMonitor) newBinlogSyncer() {
	cfg := replication.BinlogSyncerConfig{
		ServerID: dm.serverId,
		Flavor:   "mysql",
		Host:     dm.dbHost,
		Port:     uint16(dm.dbPort),
		User:     dm.dbUser,
		Password: dm.dbPasswd,
	}
	dm.syncer = replication.NewBinlogSyncer(cfg)
}

func (dm *dataMonitor) Start() {
	defer func() {
		dm.syncer.Close()
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
			streamer, err = dm.syncer.StartSync(mysql.Position{Name: cacheBinlogPos.Name, Pos: 0})
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
				dm.Logger.Infof("RowsEvent schema: %s, table: %s, eventType: %v", string(e.Table.Schema), string(e.Table.Table), ev.Header.EventType)
				msg := &MonitorDataMsg{
					Rows:      e.Rows,
					DbName:    string(e.Table.Schema),
					TableName: string(e.Table.Table),
				}
				switch ev.Header.EventType {
				case replication.WRITE_ROWS_EVENTv2, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv0:
					msg.Action = MonitorActionInsert
					dm.dataConsumer.Handler(msg)
					dm.Logger.Infof("rows data: %v", e.Rows)
				case replication.UPDATE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv0:
					msg.Action = MonitorActionUpdate
					dm.dataConsumer.Handler(msg)
					dm.Logger.Infof("rows data: %v", e.Rows)
				case replication.DELETE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv0:
					msg.Action = MonitorActionDelete
					dm.dataConsumer.Handler(msg)
					dm.Logger.Infof("rows data: %v", e.Rows)
				default:
				}
			}
			currentSynTimeStamp := int64(ev.Header.Timestamp)
			if currentSynTimeStamp > dm.lastSynTime && ev.Header.Timestamp%SetSyncTimeInterval == 0 {
				currentPos := dm.syncer.GetNextPosition()
				dm.lastSynTime = currentSynTimeStamp
				dm.setSyncLastTime(currentSynTimeStamp - 1) //减1s避免异常情况漏binlog日志
				if currentPos.Pos > 0 {
					dm.lastSynPos = currentPos.Pos
					dm.lastSyncBinFile = currentPos.Name
					dm.setSyncPos(currentPos)
				}
			}
		default:
		}
	}
}

func (dm *dataMonitor) fillDbTables() {
	dm.tables = make(map[string]bool)
	dm.dbs = make(map[string]bool)
	// fill monitor table
	if len(dm.tableNames) > 0 {
		for _, table := range dm.tableNames {
			dm.tables[table] = true
		}
	}
	// fill monitor db
	if len(dm.dbNames) > 0 {
		for _, db := range dm.dbNames {
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
	lockKey := fmt.Sprintf("%s%d", DataMonitorLockKeyPrefix, dm.serverId)
	ok, err := dm.redis.SetNX(context.Background(), lockKey, 1, time.Second*MaxOfflineTimeSpan).Result()
	if err != nil {
		return false, err
	}
	if ok { // 没有实例运行，则运行
		dm.Logger.Infof("checkRun ok key: %s", lockKey)
		return true, nil
	}
	ticker := time.NewTicker(CheckRunInterval * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			_, err = dm.redis.Get(context.Background(), lockKey).Int64()
			if err != nil && err != rds.Nil {
				return false, err
			}
			dm.Logger.Debugf("checkRun lockKey: %s running....", lockKey)
			if err == rds.Nil { // MaxOfflineTimeSpan时间没更新则启动该实例
				ok, _ = dm.redis.SetNX(context.Background(), lockKey, 1, time.Second*MaxOfflineTimeSpan).Result()
				if ok {
					return true, nil
				}
			}
		}
	}
}

func (dm *dataMonitor) setSyncPos(position mysql.Position) error {
	redisKey := fmt.Sprintf("%s%d", DataMonitorSyncPosPrefix, dm.serverId)
	values, err := json.Marshal(position)
	if err != nil {
		return err
	}
	return dm.redis.Set(context.Background(), redisKey, string(values), 0).Err()
}

func (dm *dataMonitor) getSyncPos() (mysql.Position, error) {
	position := mysql.Position{}
	redisKey := fmt.Sprintf("%s%d", DataMonitorSyncPosPrefix, dm.serverId)
	values, err := dm.redis.Get(context.Background(), redisKey).Bytes()
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
	redisKey := fmt.Sprintf("%s%d", DataMonitorSyncTimeKeyPrefix, dm.serverId)
	return dm.redis.Set(context.Background(), redisKey, t, 0).Err()
}

func (dm *dataMonitor) getSyncLastTime() (int64, error) {
	redisKey := fmt.Sprintf("%s%d", DataMonitorSyncTimeKeyPrefix, dm.serverId)
	lastTime, err := dm.redis.Get(context.Background(), redisKey).Int64()
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
	key := fmt.Sprintf("%s%d", DataMonitorLockKeyPrefix, dm.serverId)
	for {
		select {
		case <-tick.C:
			dm.redis.Expire(context.Background(), key, time.Second*MaxOfflineTimeSpan).Err()
		}
	}
}
