package upgrade

import (
	"cronTask/modules/common"
	"fmt"
	g "github.com/doug-martin/goqu/v9"
	"time"
)

type LevelCount struct {
	Prefix    string `json:"prefix" db:"prefix"`
	Level     string `json:"level" db:"level"`
	UserCount int64  `json:"user_count" db:"user_count"`
}

func updateMemberLevel() {

	time.Sleep(time.Duration(30) * time.Second) //睡30秒再查tidb的会员表
	var data []LevelCount
	query := fmt.Sprintf(`SELECT prefix,level, count(uid) as user_count FROM tbl_members  where tester=1 GROUP BY level,prefix`)
	err := tiDb.Select(&data, query)
	if err != nil {
		common.Log("updateMemberLevel", "error : %v", err)
		return
	}
	if len(data) > 0 {
		for _, v := range data {
			record := g.Record{
				"user_count": v.UserCount,
			}
			query, _, _ := dialect.Update("tbl_admin_group").Set(record).Where(g.Ex{"level": v.Level, "prefix": v.Prefix}).ToSQL()
			fmt.Println(query)
			_, err = tiDb.Exec(query)
			if err != nil {
				common.Log("updateMemberLevel", "error : %v", err)
			}
		}
	}
}
