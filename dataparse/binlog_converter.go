package dataparse

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ielevenyu/syncbinlog/config"
	"github.com/siddontang/go-log/log"
)

// getStructFields 获取结构体的所有字段（包括嵌套结构体的字段）
func (h *tableDataHandler[T]) getStructFields(t reflect.Type) []reflect.StructField {
	var fields []reflect.StructField
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)

		// 如果是结构体类型（除了time.Time），递归获取其字段
		if field.Type.Kind() == reflect.Struct && !field.Type.ConvertibleTo(reflect.TypeOf(time.Time{})) {
			nestedFields := h.getStructFields(field.Type)
			// 为嵌套字段添加前缀
			for _, nestedField := range nestedFields {
				//nestedField.Name = field.Name + "." + nestedField.Name
				fields = append(fields, nestedField)
			}
		} else {
			fields = append(fields, field)
		}
	}

	return fields
}

// setFieldValue 设置字段值，支持指针类型和嵌套结构体
func (h *tableDataHandler[T]) setFieldValue(field reflect.Value, value any, fieldName string) error {
	// 处理指针类型
	if field.Kind() == reflect.Ptr {
		if value == nil {
			return nil // 如果值为nil，保持指针为nil
		}
		// 如果指针为nil，创建新的实例
		if field.IsNil() {
			field.Set(reflect.New(field.Type().Elem()))
		}
		// 递归处理指针指向的值
		return h.setFieldValue(field.Elem(), value, fieldName)
	}

	// 处理嵌套结构体
	if field.Kind() == reflect.Struct {
		if field.Type().ConvertibleTo(reflect.TypeOf(time.Time{})) {
			return h.setTimeValue(field, value, fieldName)
		}
		// 如果是其他结构体类型，尝试将值转换为结构体
		if structValue, ok := value.(map[string]any); ok {
			return h.setStructValue(field, structValue, fieldName)
		}
		return fmt.Errorf("cannot convert value to struct for field %s", fieldName)
	}

	// 处理基本类型
	switch field.Kind() {
	case reflect.String:
		field.SetString(fmt.Sprintf("%s", value))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		switch v := value.(type) {
		case int64:
			field.SetInt(v)
		case int32:
			field.SetInt(int64(v))
		case int:
			field.SetInt(int64(v))
		case int8:
			field.SetInt(int64(v))
		case uint64:
			if v > uint64(1<<63-1) {
				return fmt.Errorf("value %d overflows int64 for field %s", v, fieldName)
			}
			field.SetInt(int64(v))
		case uint32:
			field.SetInt(int64(v))
		case uint:
			field.SetInt(int64(v))
		case uint8:
			field.SetInt(int64(v))
		case string:
			// 处理字符串到整数的转换
			intVal, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return fmt.Errorf("cannot convert string '%s' to int for field %s", v, fieldName)
			}
			field.SetInt(intVal)
		default:
			return fmt.Errorf("cannot convert value to int for field %s", fieldName)
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		switch v := value.(type) {
		case uint64:
			field.SetUint(v)
		case uint32:
			field.SetUint(uint64(v))
		case uint16:
			field.SetUint(uint64(v))
		case uint:
			field.SetUint(uint64(v))
		case uint8:
			field.SetUint(uint64(v))
		case int64:
			if v < 0 {
				return fmt.Errorf("cannot convert negative value to uint for field %s", fieldName)
			}
			field.SetUint(uint64(v))
		case int32:
			if v < 0 {
				return fmt.Errorf("cannot convert negative value to uint for field %s", fieldName)
			}
			field.SetUint(uint64(v))
		case int16:
			if v < 0 {
				return fmt.Errorf("cannot convert negative value to uint for field %s", fieldName)
			}
			field.SetUint(uint64(v))
		case int:
			if v < 0 {
				return fmt.Errorf("cannot convert negative value to uint for field %s", fieldName)
			}
			field.SetUint(uint64(v))
		case int8:
			if v < 0 {
				return fmt.Errorf("cannot convert negative value to uint for field %s", fieldName)
			}
			field.SetUint(uint64(v))
		case string:
			// 处理字符串到无符号整数的转换
			uintVal, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return fmt.Errorf("cannot convert string '%s' to uint for field %s", v, fieldName)
			}
			field.SetUint(uintVal)
		default:
			return fmt.Errorf("cannot convert value to uint for field %s, type: %v", fieldName, v)
		}
	case reflect.Float32, reflect.Float64:
		switch v := value.(type) {
		case float64:
			field.SetFloat(v)
		case float32:
			field.SetFloat(float64(v))
		case string:
			floatV, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return fmt.Errorf("cannot convert field: %s from string value to float(%s)", fieldName, v)
			}
			field.SetFloat(floatV)
		default:
			return fmt.Errorf("cannot convert value to float for field %s, type: %v", fieldName, reflect.TypeOf(v))
		}
	case reflect.Bool:
		switch v := value.(type) {
		case bool:
			field.SetBool(v)
		case int64:
			field.SetBool(v != 0)
		case int32:
			field.SetBool(v != 0)
		case int8:
			field.SetBool(v != 0)
		case int:
			field.SetBool(v != 0)
		case uint8:
			field.SetBool(v != 0)
		case uint:
			field.SetBool(v != 0)
		case uint32:
			field.SetBool(v != 0)
		case uint64:
			field.SetBool(v != 0)
		case string:
			// 处理字符串到布尔值的转换
			boolVal, err := strconv.ParseBool(v)
			if err != nil {
				return fmt.Errorf("cannot convert string '%s' to bool for field %s", v, fieldName)
			}
			field.SetBool(boolVal)
		default:
			return fmt.Errorf("cannot convert value to bool for field %s, type: %v", fieldName, v)
		}
	case reflect.Slice:
		if field.Type().Elem().Kind() == reflect.Uint8 {
			// 处理 []uint8 类型
			switch v := value.(type) {
			case []uint8:
				field.SetBytes(v)
			case string:
				field.SetBytes([]byte(v))
			default:
				return fmt.Errorf("cannot convert value to []uint8 for field %s", fieldName)
			}
		} else {
			return fmt.Errorf("unsupported slice type for field %s", fieldName)
		}
	}
	return nil
}

// setTimeValue 设置时间类型的值
func (h *tableDataHandler[T]) setTimeValue(field reflect.Value, value any, fieldName string) error {
	switch v := value.(type) {
	case time.Time:
		field.Set(reflect.ValueOf(v).Convert(field.Type()))
	case string:
		t, err := h.parseTime(v)
		if err != nil {
			return fmt.Errorf("cannot parse time for field %s, err: %+v", fieldName, err)
		}
		field.Set(reflect.ValueOf(t).Convert(field.Type()))
	case int64, uint64:
		t, err := h.timestampToTime(v)
		if err != nil {
			return fmt.Errorf("cannot parse time for field %s, err: %s", fieldName, err.Error())
		}
		field.Set(reflect.ValueOf(t).Convert(field.Type()))
	default:
		return fmt.Errorf("cannot convert value[%+v] to time for field %s", reflect.TypeOf(v), fieldName)
	}
	return nil
}

// setStructValue 设置结构体类型的值
func (h *tableDataHandler[T]) setStructValue(field reflect.Value, value map[string]any, fieldName string) error {
	for i := 0; i < field.NumField(); i++ {
		subField := field.Field(i)
		if !subField.CanSet() {
			continue
		}

		// 获取字段标签中的列名
		subFieldName := field.Type().Field(i).Name
		tag := field.Type().Field(i).Tag.Get("gorm")
		if len(tag) == 0 {
			continue
		}
		tagProperties := strings.Split(tag, ";")
		if len(tagProperties) == 0 {
			continue
		}
		tagColumns := strings.Split(tagProperties[0], ":")
		if len(tagColumns) != 2 {
			continue
		}
		if len(tagColumns[0]) > 0 {
			subFieldName = tagColumns[0]
		}

		// 查找对应的值
		if val, ok := value[subFieldName]; ok {
			if err := h.setFieldValue(subField, val, subFieldName); err != nil {
				return fmt.Errorf("error setting field %s.%s: %v", fieldName, subFieldName, err)
			}
		}
	}
	return nil
}

// ConvertBinlogToStruct 将binlog数据转换为结构体
func (h *tableDataHandler[T]) ConvertBinlogToStruct(row []any, tableInfo *config.TableInfo) (T, error) {

	var result T
	tType := reflect.TypeOf(result)

	// 检查目标类型是否为结构体
	if tType.Kind() != reflect.Struct {
		return result, fmt.Errorf("target type must be a struct")
	}

	// 获取所有字段（包括嵌套结构体的字段）
	fields := h.getStructFields(tType)
	//
	//// 检查数据长度是否与字段数量匹配
	//if len(row) != len(tableInfo.Columns) {
	//	return result, fmt.Errorf("row length (%d) does not match column count (%d)",
	//		len(row), len(tableInfo.Columns))
	//}

	// 创建结构体的反射值
	value := reflect.ValueOf(&result).Elem()

	// 遍历字段并赋值
	for _, field := range fields {
		// 获取字段标签中的列名
		fieldName := field.Name
		tag := field.Tag.Get("gorm")
		tagProperties := strings.Split(tag, ";")
		if len(tagProperties) == 0 {
			continue
		}
		tagColumns := strings.Split(tagProperties[0], ":")
		if len(tagColumns) < 2 {
			continue
		}
		if len(tagColumns[1]) > 0 {
			fieldName = tagColumns[1]
		}

		// 查找对应的列信息
		var colInfo *config.ColumnInfo
		for _, col := range tableInfo.Columns {
			if strings.EqualFold(col.Name, fieldName) {
				colInfo = &col
				break
			}
		}

		if colInfo == nil {
			continue
		}

		// 使用列的位置信息获取对应的值
		rowValue := row[colInfo.Position-1] // Position 是 1-based

		// 处理NULL值
		if rowValue == nil {
			if !colInfo.Nullable {
				return result, fmt.Errorf("column %s is not nullable but got NULL value", colInfo.Name)
			}
			continue
		}

		// 设置字段值
		if err := h.setFieldValue(value.FieldByName(field.Name), rowValue, fieldName); err != nil {
			log.Errorf("setFieldValue error fieldName: %s, rowValue: %v, err: %s", fieldName, rowValue, err.Error())
			return result, fmt.Errorf("failed to set field %s: %w", fieldName, err)
		}
	}

	return result, nil
}

// parseTime 尝试用多种常见格式解析时间字符串。
// 支持的格式包括 "2006-01-02 15:04:05", "2006-01-02", "20060102", RFC3339 等。
func (h *tableDataHandler[T]) parseTime(dateStr string) (time.Time, error) {
	// 定义所有支持的时间格式布局
	// 顺序可以调整，但通常把最常见的放前面
	layouts := []string{
		"2006-01-02 15:04:05", // 标准格式
		time.RFC3339,          // "2006-01-02T15:04:05Z07:00"
		"2006-01-02",          // 只有日期
		"20060102",            // 紧凑型日期
		"2006/01/02 15:04:05", // 使用斜杠
		"2006/01/02",          // 使用斜杠的日期
		"01-02-2006",          // 月-日-年 格式
		"02-Jan-2006",         // 带月份缩写
	}

	// 遍历所有布局，尝试解析
	for _, layout := range layouts {
		t, err := time.Parse(layout, dateStr)
		if err == nil {
			// 解析成功，直接返回
			return t, nil
		}
	}

	// 如果所有布局都失败了，返回错误
	return time.Time{}, fmt.Errorf("unable to parse date: \"%s\"", dateStr)
}
func (h *tableDataHandler[T]) timestampToTime(timestamp any) (time.Time, error) {
	var stamp int64
	switch timestamp.(type) {
	case int64:
		stamp = timestamp.(int64)
	case uint64:
		stamp = int64(timestamp.(uint64))
	}
	if stamp <= 0 {
		return time.Time{}, nil
	}
	timestr := fmt.Sprintf("%d", stamp)
	switch len(timestr) {
	case 10: // 10位秒级时间戳
		return time.Unix(stamp, 0), nil
	case 13: // 13位毫秒时间戳
		sec := stamp / 1000
		nsec := (stamp % 1000) * 1e6
		return time.Unix(sec, nsec), nil
	default:
		return time.Time{}, fmt.Errorf("cant't support len[%d] timestamp! ", len(timestr))
	}
}
