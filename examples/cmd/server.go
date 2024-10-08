package main

import (
	penguinDB "github.com/paranoidxc/PenguinDB"
	"github.com/paranoidxc/PenguinDB/fedis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "127.0.0.1:6399"

type FedisServer struct {
	dbs    map[int]*fedis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	// 打开 Redis 数据结构服务
	redisDataStructure, err := fedis.NewRedisDataStructure(penguinDB.DefaultOptions)
	if err != nil {
		panic(err)
	}

	// 初始化 FedisServer
	fedisServer := &FedisServer{
		dbs: make(map[int]*fedis.RedisDataStructure),
	}
	fedisServer.dbs[0] = redisDataStructure

	// 初始化一个 Redis 服务端
	fedisServer.server = redcon.NewServer(addr, execClientCommand, fedisServer.accept, fedisServer.close)
	fedisServer.listen()
}

func (svr *FedisServer) listen() {
	log.Println("penguinDB server running, ready to accept connections")
	_ = svr.server.ListenAndServe()
}

func (svr *FedisServer) accept(conn redcon.Conn) bool {
	cli := new(FedisClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *FedisServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}
