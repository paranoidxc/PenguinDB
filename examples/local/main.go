package main

import (
	"bufio"
	"fmt"
	penguinDB "github.com/paranoidxc/PenguinDB"
	"github.com/paranoidxc/PenguinDB/lib/logger"
	"os"
	"strings"
)

const (
	CMD_SET    = "SET"
	CMD_GET    = "GET"
	CMD_DELETE = "DEL"
	CMD_KEYS   = "KEYS"
	CMD_MERGE  = "MERGE"
	CMD_SYNC   = "SYNC"
	CMD_BACKUP = "BACKUP"
	CMD_QUIT   = "QUIT"
	CMD_EXIT   = "EXIT"
)

func main() {
	logger.Setup(&logger.Settings{
		Path:           "logs",
		Name:           "penguin",
		Ext:            "log",
		TimeFORMAT:     "2006-01-02",
		OutputTerminal: false,
	})
	logger.SetDebugMode(true)

	opt := penguinDB.DefaultOptions
	opt.PersistentDir = "PersistentData"

	db, err := penguinDB.Open(opt)
	if err != nil {
		panic(err)
	}

	logger.Info("penguinDB started")

	//db.Set([]byte("test"), []byte("一个软件总是为解决某种特定的需求而产生，时代在发展，客户的业务也在发生变化。有的需求相对稳定一些，有的需求变化的比较剧烈，还有的需求已经消失了，或者转化成了别的需求。在这种情况下，软件必须相应的改变。\n考虑到成本和时间等因素，当然不是所有的需求变化都要在软件系统中实现。但是总的说来，软件要适应需求的变化，以保持自己的生命力。\n这就产生了一种糟糕的现象：软件产品最初制造出来，是经过精心的设计，具有良好架构的。但是随着时间的发展、需求的变化，必须不断的修改原有的功能、追加新的功能，还免不了有一些缺陷需要修改。为了实现变更，不可避免的要违反最初的设计构架。经过一段时间以后，软件的架构就千疮百孔了。bug越来越多，越来越难维护，新的需求越来越难实现，软件的架构对新的需求渐渐的失去支持能力，而是成为一种制约。最后新需求的开发成本会超过开发一个新的软件的成本，这就是这个软件系统的生命走到尽头的时候。\n重构就能够最大限度的避免这样一种现象。系统发展到一定阶段后，使用重构的方式，不改变系统的外部功能，只对内部的结构进行重新的整理。通过重构，不断的调整系统的结构，使系统对于需求的变更始终具有较强的适应能力。\n重构可以降低项目的耦合度，使项目更加模块化，有利于项目的开发效率和后期的维护。让项目主框架突出鲜明，给人一种思路清晰，一目了然的感觉，其实重构是对框架的一种维护。"))

	ch, err := db.NewWatch()
	if err != nil {
		panic(err)
	}

	go func() {
		for event := range ch {
			fmt.Println("watch event:", event.EventType, " key:", string(event.Key))
		}
	}()

	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Printf("pengiunDB> ")

		cmd, _ := reader.ReadString('\n')
		cmd = strings.ToUpper(strings.TrimSpace(cmd))
		logger.Info("cmd", cmd)
		switch cmd {
		case CMD_SET:
			key, _ := reader.ReadString('\n')
			value, _ := reader.ReadString('\n')

			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)

			_, err := db.Set([]byte(key), []byte(value))
			if err != nil {
				fmt.Printf("%s err:%s\n", CMD_SET, err)
				break
			}
			fmt.Printf("%s key:%s val:%s\n", CMD_SET, key, value)
		case CMD_GET:
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			val, err := db.Get([]byte(key))
			if err != nil {
				fmt.Printf("%s key:%s err:%s\n", CMD_GET, key, err)
				break
			}
			fmt.Printf("%s key:%s val:%s\n", CMD_GET, key, val)
		case CMD_DELETE:
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			err := db.Delete([]byte(key))
			if err != nil {
				fmt.Printf("%s key:%s err:%s\n", CMD_DELETE, key, err)
				break
			}
			fmt.Printf("%s key:%s \n", CMD_DELETE, key)
		case CMD_KEYS:
			keys := db.Keys()
			fmt.Println(CMD_KEYS)
			for _, key := range keys {
				fmt.Printf("key=%s \n", string(key))
			}
		case CMD_MERGE:
			err := db.Merge()
			if err != nil {
				fmt.Printf("%s  err:%s\n", CMD_MERGE, err)
				break
			}
			fmt.Printf("%s okay\n", CMD_MERGE)
		case CMD_SYNC:
			err := db.Sync()
			if err != nil {
				fmt.Printf("%s err:%s\n", CMD_SYNC, err)
				break
			}
			fmt.Printf("%s okay\n", CMD_SYNC)
		case CMD_BACKUP:
			dir, _ := reader.ReadString('\n')
			dir = strings.TrimSpace(dir)
			err := db.Backup(dir)
			if err != nil {
				fmt.Printf("%s err:%s\n", CMD_BACKUP, err)
				break
			}
			fmt.Printf("%s okay\n", CMD_BACKUP)
		case CMD_QUIT, CMD_EXIT:
			err := db.Close()
			if err != nil {
				fmt.Printf("%s err:%s\n", CMD_EXIT, err)
				break
			}
			fmt.Printf("%s okay\n", CMD_EXIT)
			os.Exit(0)
		default:
			fmt.Println("cmd not found")
		}
	}
}
