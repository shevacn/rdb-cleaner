package connector

import (
	"github.com/go-redis/redis/v8"
	"github.com/rs/zerolog/log"
	"go.uber.org/ratelimit"
)

type NodeConfig struct {
	Id   string `json:"id"` //Id := fmt.Sprintf("%s%s%s", n.Addr, md5.Sum([]byte(n.Pwd)), n.Db)
	Addr string `json:"addr"`
	Pwd  string `json:"pwd"`
	Db   int    `json:"db"`
	Qps  int    `json:"qps"`
}

type RdbClient struct {
	Conn      *redis.Client
	ConnHash  string
	RateLimit ratelimit.Limiter
}

var nodeMap = make(map[string]*RdbClient)

func InitNodes(nodes []NodeConfig) {

	for _, n := range nodes {
		rdb := redis.NewClient(&redis.Options{
			Addr:     n.Addr,
			Password: n.Pwd, // no password set
			DB:       n.Db,  // use default DB
		})
		qps := n.Qps
		if qps == 0 {
			qps = 10000
		}
		nodeMap[n.Id] = &RdbClient{
			Conn:      rdb,
			ConnHash:  n.Addr + "_" + n.Pwd,
			RateLimit: ratelimit.New(qps),
		}
		log.Printf("Init rdb node: Id=%s, addr=%s", n.Id, n.Addr)
	}
}

func GetNodeConn(id string) *RdbClient {
	return nodeMap[id]
}
