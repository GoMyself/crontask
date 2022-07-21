package link

import (
	"cronTask/contrib/conn"
	"cronTask/contrib/helper"
	"cronTask/modules/common"
	"fmt"
	g "github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"
	"time"
)

var (
	db       *sqlx.DB
	td       *sqlx.DB
	prefix   string
	indexUrl string
	dialect  = g.Dialect("mysql")
	colsLink = helper.EnumFields(Link_t{})
)

type Link_t struct {
	ID               string `db:"id" json:"id"`
	UID              string `db:"uid" json:"uid"`
	Username         string `db:"username" json:"username"`
	ShortURL         string `db:"short_url" json:"short_url"`
	Prefix           string `db:"prefix" json:"prefix"`
	NoAd             int    `db:"no_ad" json:"no_ad"`                           //0展示广告页，1不展示广告页
	ZR               string `db:"zr" json:"zr"`                                 //真人返水
	QP               string `db:"qp" json:"qp"`                                 //棋牌返水
	TY               string `db:"ty" json:"ty"`                                 //体育返水
	DJ               string `db:"dj" json:"dj"`                                 //电竞返水
	DZ               string `db:"dz" json:"dz"`                                 //电子返水
	CP               string `db:"cp" json:"cp"`                                 //彩票返水
	FC               string `db:"fc" json:"fc"`                                 //斗鸡返水
	BY               string `db:"by" json:"by"`                                 //捕鱼返水
	CGHighRebate     string `db:"cg_high_rebate" json:"cg_high_rebate"`         //cg高频彩返点
	CGOfficialRebate string `db:"cg_official_rebate" json:"cg_official_rebate"` //cg高频彩返点
	CreatedAt        string `db:"created_at" json:"created_at"`
}

func Parse(endpoints []string, path string) {

	conf := common.ConfParse(endpoints, path)
	fmt.Println(conf.Db.Master.Addr)
	fmt.Println(conf.Td.Addr)

	prefix = conf.Prefix
	indexUrl = conf.IndexUrl
	// 初始化db
	db = conn.InitDB(conf.Db.Master.Addr, conf.Db.Master.MaxIdleConn, conf.Db.Master.MaxIdleConn)
	td = conn.InitDB(conf.Td.Addr, conf.Td.MaxIdleConn, conf.Td.MaxIdleConn)
	common.InitTD(td)

	updateLink()
}

func updateLink() {

	var data []Link_t
	ex := g.Ex{
		"prefix": prefix,
	}
	query, _, _ := dialect.From("tbl_member_link").Select(colsLink...).Where(ex).ToSQL()
	fmt.Println(query)
	err := db.Select(&data, query)
	if err != nil {
		fmt.Printf("query : %s \n error : %s \n", query, err.Error())
		return
	}

	for _, v := range data {
		ts := time.Now()
		record := g.Record{
			"ts":         ts.UnixMicro(),
			"prefix":     v.Prefix,
			"url":        fmt.Sprintf(`%s/entry/register?id=%s|%s`, indexUrl, v.UID, v.ID),
			"short_url":  v.ShortURL,
			"created_at": ts.Unix(),
			"no_ad":      v.NoAd,
		}
		query, _, _ = dialect.Insert("shorturl").Rows(record).ToSQL()
		fmt.Println(query)
		_, err = td.Exec(query)
		if err != nil {
			fmt.Println("insert shorturl = ", err.Error(), query)
		}
	}
}
