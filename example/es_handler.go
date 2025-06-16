package example

import (
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/ielevenyu/syncbinlog/config"
	"github.com/ielevenyu/syncbinlog/example/elasticsearch"
	"github.com/olivere/elastic/v7"
)

type esDataHandler struct {
	esClient elasticsearch.EsClient
}

func NewEsDataHandler(es elasticsearch.EsClient) *esDataHandler {
	return &esDataHandler{
		esClient: es,
	}
}

func (c *esDataHandler) EsDataProcess(msg *config.MonitorDataMsg, records []any) error {
	if c.esClient == nil {
		return nil
	}
	if len(records) == 0 {
		return nil
	}
	indexName := fmt.Sprintf("%s_%s", msg.DbName, msg.TableName)
	if msg.Action == config.MonitorActionInsert {
		for _, v := range records {
			c.esClient.InsertIndex(indexName, v)
		}
	} else if msg.Action == config.MonitorActionUpdate { // 更新数据
		if len(records)%2 != 0 {
			return nil
		}
		for i := 1; i < len(records); i += 2 {
			if err := c.updateAll(indexName, records[i]); err != nil {
				fmt.Println(fmt.Sprintf("updateAll error err: %+v", err))
			}
		}
	}

	return nil
}

func (c *esDataHandler) updateAll(indexName string, record any) error {
	docId, err := c.queryDocIdByCourseTableId(indexName, record)
	if err != nil {
		return err
	}
	if len(docId) == 0 {
		return c.esClient.InsertIndex(indexName, record)
	}
	return c.esClient.UpdateAll(indexName, docId, record)
}

func (c *esDataHandler) queryDocIdByCourseTableId(indexName string, record any) (string, error) {
	boolQuery := c.buildBoolQuery(record)
	if boolQuery == nil {
		return "", fmt.Errorf("buildBoolQuery error! ")
	}
	// search
	hits, err := c.esClient.SearchIndex([]string{indexName}, boolQuery, "", false, 0, 0)
	if err != nil && !strings.Contains(err.Error(), "no such index") {
		return "", fmt.Errorf("queryDocIdByCourseTableId: SearchIndex failed: %s", err.Error())
	}
	if len(hits) == 0 {
		return "", nil
	}
	return hits[0].Id, nil
}

func (c *esDataHandler) buildBoolQuery(record any) *elastic.BoolQuery {
	boolQuery := elastic.NewBoolQuery()
	recordV := reflect.ValueOf(record)
	idFieldV := c.fieldNestByName(recordV, "ID")
	if !idFieldV.IsValid() {
		return nil
	}
	var id int64
	switch idFieldV.Type().Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		id = idFieldV.Int()
	case reflect.Uint, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		id = int64(idFieldV.Uint())
	default:
		return nil
	}
	if id > 0 {
		boolQuery.Filter(elastic.NewTermQuery("ID", id))
	}
	return boolQuery
}

func (c *esDataHandler) fieldNestByName(result reflect.Value, name string) reflect.Value {
	if result.Kind() == reflect.Ptr {
		result = result.Elem()
	}
	if result.Kind() != reflect.Struct {
		return reflect.Value{}
	}
	t := result.Type()
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Type.Kind() == reflect.Struct && f.Type != reflect.TypeOf(time.Time{}) {
			// 递归查找嵌套结构体
			nestedField := c.fieldNestByName(result.Field(i), name)
			if nestedField.IsValid() {
				return nestedField
			}
		} else {
			// 检查当前字段的tag
			if f.Name == name {
				return result.Field(i)
			}
		}
	}
	return reflect.Value{}
}
