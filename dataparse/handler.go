package dataparse

import (
	"fmt"
	"sync"

	"github.com/ielevenyu/syncbinlog/config"
	"github.com/siddontang/go-log/log"
)

var handlerMap sync.Map

func RegisterHandler(dbName, table string, handler SyncDataHandler) {
	handlerKey := fmt.Sprintf("%s_%s", dbName, table)
	handlerMap.Store(handlerKey, handler)
	log.Debugf("add handlerKey[%s] to handlerMap", handlerKey)
}

type bizDataSync struct {
}

func NewBizDataSync() *bizDataSync {
	return &bizDataSync{
		//handlerMap: make(map[string]SyncDataHandler),
	}
}

func (b *bizDataSync) Handler(msg *config.MonitorDataMsg) error {
	log.Debugf("receive msg: %+v", msg)
	handlerAct := fmt.Sprintf("%s_%s", msg.DbName, msg.TableName)
	handler, ok := handlerMap.Load(handlerAct)
	if !ok {
		return fmt.Errorf("not found handler for[%s] ", handlerAct)
	}
	log.Debugf("run handlerAct: %s", handlerAct)
	h, ok := handler.(SyncDataHandler)
	if !ok {
		return fmt.Errorf("handler type error! ")
	}
	return h.dataHandler(msg)
}

type SyncDataHandler interface {
	dataHandler(msg *config.MonitorDataMsg) error
}

func NewTableDataHandler[T any](handler func(msg *config.MonitorDataMsg, records []T) error) *tableDataHandler[T] {
	return &tableDataHandler[T]{
		handler: handler,
	}
}

type tableDataHandler[T any] struct {
	handler func(msg *config.MonitorDataMsg, records []T) error
}

func (h *tableDataHandler[T]) dataHandler(msg *config.MonitorDataMsg) error {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("dataHandler err: %+v", r)
		}
	}()
	var records []T
	if len(msg.Rows) == 0 { // 没有数据不处理
		return nil
	}
	for _, v := range msg.Rows {
		data, err := h.ConvertBinlogToStruct(v, msg.TableInfo)
		if err != nil {
			return err
		}
		records = append(records, data)
	}
	return h.handler(msg, records)
}
