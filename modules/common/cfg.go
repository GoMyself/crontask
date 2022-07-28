package common

import (
	"cronTask/contrib/apollo"
)

type Conf struct {
	Lang         string `json:"lang"`
	Prefix       string `json:"prefix"`
	PullPrefix   string `json:"pull_prefix"`
	IsDev        bool   `json:"is_dev"`
	Sock5        string `json:"sock5"`
	RPC          string `json:"rpc"`
	Fcallback    string `json:"fcallback"`
	IndexUrl     string `json:"index_url"`
	AutoPayLimit string `json:"autoPayLimit"`
	Nats         struct {
		Servers  []string `json:"servers"`
		Username string   `json:"username"`
		Password string   `json:"password"`
	} `json:"nats"`
	Beanstalkd struct {
		Addr    string `json:"addr"`
		MaxIdle int    `json:"maxIdle"`
		MaxCap  int    `json:"maxCap"`
	} `json:"beanstalkd"`
	Db struct {
		Master struct {
			Addr        string `json:"addr"`
			MaxIdleConn int    `json:"max_idle_conn"`
			MaxOpenConn int    `json:"max_open_conn"`
		} `json:"master"`
		Report struct {
			Addr        string `json:"addr"`
			MaxIdleConn int    `json:"max_idle_conn"`
			MaxOpenConn int    `json:"max_open_conn"`
		} `json:"report"`
		Bet struct {
			Addr        string `json:"addr"`
			MaxIdleConn int    `json:"max_idle_conn"`
			MaxOpenConn int    `json:"max_open_conn"`
		} `json:"bet"`
		Tidb struct {
			Addr        string `json:"addr"`
			MaxIdleConn int    `json:"max_idle_conn"`
			MaxOpenConn int    `json:"max_open_conn"`
		} `json:"tidb"`
		Td struct {
			Addr        string `json:"addr"`
			MaxIdleConn int    `json:"max_idle_conn"`
			MaxOpenConn int    `json:"max_open_conn"`
		} `json:"td"`
	} `json:"db"`
	Td struct {
		Log struct {
			Addr        string `json:"addr"`
			MaxIdleConn int    `json:"max_idle_conn"`
			MaxOpenConn int    `json:"max_open_conn"`
		} `json:"log"`
	} `json:"td"`
	Redis struct {
		Addr     []string `json:"addr"`
		Password string   `json:"password"`
	} `json:"redis"`
	Minio struct {
		ImagesBucket    string `json:"images_bucket"`
		JSONBucket      string `json:"json_bucket"`
		Endpoint        string `json:"endpoint"`
		AccessKeyID     string `json:"accessKeyID"`
		SecretAccessKey string `json:"secretAccessKey"`
		UseSSL          bool   `json:"useSSL"`
		UploadURL       string `json:"uploadUrl"`
	} `json:"minio"`
}

func ConfParse(endpoints []string, path string) Conf {

	cfg := Conf{}

	apollo.New(endpoints)
	apollo.Parse(path, &cfg)
	apollo.Close()

	return cfg
}
