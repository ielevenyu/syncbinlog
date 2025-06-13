package example

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ielevenyu/syncbinlog"
	"github.com/ielevenyu/syncbinlog/config"
	"github.com/ielevenyu/syncbinlog/dataparse"
	rds "github.com/redis/go-redis/v9"
)

func main() {
	rdb := rds.NewClient(&rds.Options{
		Addr:     "", // Redis 服务器地址
		Password: "", // 密码（无密码时留空）
		DB:       0,  // 默认数据库编号
	})
	registerBizDataHandler()
	startMonitorDb(rdb)
	quit := make(chan os.Signal)
	signal.Notify(quit, syscall.SIGKILL, syscall.SIGQUIT, syscall.SIGINT, syscall.SIGTERM)
	<-quit
}

func startMonitorDb(rdb *rds.Client) {
	go func() {
		handler, err := syncbinlog.NewDataMonitor(
			config.WithServerId(10000001),  //可以写死一个很大的数字即可eg:100000
			config.WithDbHost("127.0.0.1"), // 数据库主库host
			config.WithDbPort(3306),        //数据库端口号
			config.WithDbUser("root"),      //监听账号，需要有REPLICATION SLAVE和REPLICATION CLIENT权限
			config.WithDbPasswd("xxxxxxxx"),
			config.WithDbNames([]string{"user_db"}),   //监听数据库
			config.WithTableNames([]string{"t_user"}), //监听table列表
			config.WithRedis(rdb),                     // redis指针
		)
		if err != nil {
			panic(fmt.Sprintf("startMonitorDb error: %s", err.Error()))
		}
		handler.Start()
	}()
}

type User struct {
	ID         uint64    `gorm:"column:id;primary_key;"` // 主键id bigint
	CreateTime time.Time `gorm:"column:create_time;"`    // 创建时间；必填 datetime
	UpdateTime time.Time `gorm:"column:update_time;"`    // 修改时间；必填datetime
	Name       string    `gorm:"column:name;"`           // 真实姓名
	NickName   string    `gorm:"column:nick_name;"`      // 用户昵称
	Classify   uint8     `gorm:"column:classify;"`       // 志愿者类别&分类：1 非教学 2 教学用户
	Phone      string    `gorm:"column:phone;"`          // 手机号、加密
	IdCard     string    `gorm:"column:id_card;"`        // 身份证号、加密
	Avatar     string    `gorm:"column:avatar;"`         // 头像
	Education  uint8     `gorm:"column:education;"`      // 学历:0未设置;1初中; 2高中; 3大专; 4大学本科; 5硕士; 6博士;7博士后;8其它;9大专在读;10本科在读;11硕士在读;12博士生在读
	Level      uint8     `gorm:"column:level;"`          // 等级
	Email      string    `gorm:"column:email;"`          // 邮箱
	Sex        uint8     `gorm:"column:sex;"`            // 性别：0未设置 1男 2女
	Birthday   string    `gorm:"column:birthday;"`       // 生日
}

type userDataHandler struct {
}

func NewUserDataHandler() *userDataHandler {
	return &userDataHandler{}
}

func (u *userDataHandler) ProcessData(msg *config.MonitorDataMsg, records []User) error {
	fmt.Println(fmt.Sprintf("len records: %d ", len(records)))
	// 业务处理逻辑
	return nil
}

// 注册数据处理函数
func registerBizDataHandler() {
	// t_user binlog数据处理
	userDataHandler := NewUserDataHandler()
	dataparse.RegisterHandler("user_db", "t_user", dataparse.NewTableDataHandler(userDataHandler.ProcessData))
}
