package elasticsearch

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/olivere/elastic/v7"
)

const (
	DefaultDeleteBatchSize = 1000
)

type ErrorLog struct {
}

func (l *ErrorLog) Printf(format string, v ...interface{}) {
	fmt.Println(fmt.Sprintf(format, v...))
}

type InfoLog struct {
}

func (l *InfoLog) Printf(format string, v ...interface{}) {
	fmt.Println(fmt.Sprintf(format, v...))
}

type esClient struct {
	client *elastic.Client
}

type EsClient interface {
	InsertIndex(index string, obj interface{}) error
	BulkInsertIndex(index string, m map[string]string) (int64, error)

	CountIndex(indices []string, boolQuery *elastic.BoolQuery) (int64, error)
	CountInIndexes(indexNames []string) (int64, error)
	SearchIndex(indices []string, boolQuery *elastic.BoolQuery, orderItem string, isAsec bool, offset, limit int64) ([]*elastic.SearchHit, error)
	SearchTermsQuery(indices []string, termsQuery *elastic.TermsQuery, orderItem string, isAsec bool, offset, limit int64) ([]*elastic.SearchHit, error)

	UpdateIndex(trafficId, index string, boolQuery *elastic.BoolQuery, script *elastic.Script) error
	PutMapping(index string, mapping map[string]interface{}) error
	UpdateAll(index, docId string, obj interface{}) error

	DeleteIndexes(indexNames []string) error

	GetClient() *elastic.Client
}

func NewEsClient(url, user, passwd string) (EsClient, error) {

	// 创建自定义的HTTP客户端
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: tr}

	ec := &esClient{}
	var err error
	ec.client, err = elastic.NewSimpleClient(elastic.SetURL(url),
		elastic.SetHttpClient(httpClient),
		elastic.SetBasicAuth(user, passwd),
		elastic.SetSniff(false),
		elastic.SetErrorLog(&ErrorLog{}),
		elastic.SetInfoLog(&InfoLog{}))
	if err != nil {
		return nil, fmt.Errorf("NewEsClient: NewClient failed: %v", err)
	}

	return ec, nil
}

func NewLongLivedEsClient(url, user, passwd string) (EsClient, error) {

	// 创建自定义的HTTP客户端
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
	}
	httpClient := &http.Client{Transport: tr}
	//httpClient := &http.Client{}

	ec := &esClient{}
	var err error
	ec.client, err = elastic.NewClient(elastic.SetURL(url),
		elastic.SetHttpClient(httpClient),
		elastic.SetBasicAuth(user, passwd),
		elastic.SetSniff(false), // 不做地址自动转换
		elastic.SetErrorLog(&ErrorLog{}))
	if err != nil {
		return nil, fmt.Errorf("NewEsClient: NewClient failed: %v", err)
	}

	return ec, nil
}

func (ec *esClient) InsertIndex(index string, obj interface{}) error {
	m := ec.ConvertToMap(obj)
	//// 若需要改为datastream 则在插入数据时需要加上@timestamp字段
	//m[TimestampFieldKey] = time.Now().Format(time.RFC3339Nano)
	fmt.Println(fmt.Sprintf("InsertIndex m: %s", m))
	_, err := ec.client.Index().
		Index(index).
		BodyJson(m).
		Refresh("true").
		Do(context.Background())
	if err != nil {
		return err
	}

	return nil
}

func (ec *esClient) BulkInsertIndex(index string, docs map[string]string) (int64, error) {
	// Create a new bulk request
	bulkService := ec.client.Bulk().Index(index).Refresh("true")

	// Add documents to the request
	for id, doc := range docs {
		//// 若需要改为datastream 则在插入数据时需要加上@timestamp字段
		//mt := make(map[string]interface{})
		//_ = json.Unmarshal([]byte(doc), &mt)
		//mt[TimestampFieldKey] = time.Now().Format(time.RFC3339Nano)

		bulkService.Add(elastic.NewBulkCreateRequest().
			Index(index).
			Id(id).
			Doc(doc))
	}
	if bulkService.NumberOfActions() <= 0 {
		return 0, nil
	}

	// Execute the request
	rsp, err := bulkService.Do(context.Background())
	if err != nil {
		return 0, fmt.Errorf("BulkInsertIndex failed: %v\n", err)
	}

	return int64(len(rsp.Items)), nil
}

func (ec *esClient) IndexExists(index string) (bool, error) {
	exists, err := ec.client.IndexExists(index).Do(context.Background())
	if err != nil {
		return false, err
	}
	return exists, nil
}

func (ec *esClient) DeleteIndexes(indexNames []string) error {
	if len(indexNames) <= 0 {
		return nil
	}

	_, err := ec.client.DeleteIndex(indexNames...).Do(context.Background())
	if err != nil {
		return err
	}

	return nil
}

func (ec *esClient) CountIndex(indices []string, boolQuery *elastic.BoolQuery) (int64, error) {

	count, err := ec.client.Count().
		Index(indices...).
		Query(boolQuery).
		Do(context.Background())
	if err != nil {
		return 0, fmt.Errorf("CountIndex failed: %s\n", err)
	}

	return count, nil
}

func (ec *esClient) CountInIndexes(indexNames []string) (int64, error) {
	// 统计多个index的总文档数
	catCountRsp, err := ec.client.CatCount().
		Index(indexNames...).
		Do(context.Background())
	if err != nil {
		return 0, err
	}
	totalCount := int64(0)
	for _, rsp := range catCountRsp {
		totalCount += int64(rsp.Count)
	}
	return totalCount, nil
}

func (ec *esClient) SearchIndex(indices []string, boolQuery *elastic.BoolQuery, orderItem string, isAsec bool, offset, limit int64) ([]*elastic.SearchHit, error) {

	ss := ec.client.Search(indices...)
	if limit > 0 && offset >= 0 {
		ss.Size(int(limit))
		ss.From(int(offset))
	}

	if len(orderItem) != 0 {
		ss.Sort(orderItem, isAsec)
	}

	searchResults, err := ss.Query(boolQuery).Do(context.Background())
	if err != nil {
		return nil, fmt.Errorf("SearchIndex: Query failed: %v\n", err)
	}

	return searchResults.Hits.Hits, nil
}

func (ec *esClient) SearchTermsQuery(indices []string, query *elastic.TermsQuery, orderItem string, isAsec bool, offset, limit int64) ([]*elastic.SearchHit, error) {

	ss := ec.client.Search(indices...)
	if limit > 0 && offset >= 0 {
		ss.Size(int(limit))
		ss.From(int(offset))
	}

	if len(orderItem) != 0 {
		ss.Sort(orderItem, isAsec)
	}

	searchResults, err := ss.Query(query).Do(context.Background())
	if err != nil {
		return nil, fmt.Errorf("SearchTermsQuery: Query failed: %v\n", err)
	}

	return searchResults.Hits.Hits, nil
}

func (ec *esClient) UpdateIndex(trafficId, index string, boolQuery *elastic.BoolQuery, script *elastic.Script) error {
	ret, err := ec.client.UpdateByQuery(index).
		Query(boolQuery).
		Script(script).
		Refresh("true").
		Do(context.Background())
	if err != nil {
		return fmt.Errorf("UpdateIndex failed trafficId:%s err: %s", trafficId, err.Error())
	}
	if ret.Updated == 0 {
		return fmt.Errorf("UpdateIndex failed no record trafficId:%s", trafficId)
	}
	return nil
}

func (ec *esClient) PutMapping(index string, mapping map[string]interface{}) error {

	_, err := ec.client.PutMapping().
		Index(index).
		BodyJson(mapping).
		Do(context.Background())
	if err != nil {
		return fmt.Errorf("PutMapping failed: %s\n", err)
	}

	return nil
}

func (ec *esClient) UpdateAll(index, docId string, obj interface{}) error {
	_, err := ec.client.Index().
		Index(index).
		Id(docId). // 使用原文档ID
		BodyJson(obj).
		Do(context.Background())
	if err != nil {
		return fmt.Errorf("update failed: %s", err.Error())
	}
	return nil
}

func (ec *esClient) GetClient() *elastic.Client {
	return ec.client
}

func (ec *esClient) ConvertToMap(obj interface{}) map[string]interface{} {
	data, _ := json.Marshal(obj)
	m := make(map[string]interface{})
	_ = json.Unmarshal(data, &m)
	return m
}
