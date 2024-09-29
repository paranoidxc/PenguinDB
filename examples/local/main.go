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
	CMD_ALL    = "ALL"
	CMD_QUIT   = "QUIT"
	CMD_EXIT   = "EXIT"
	CMD_MERGE  = "MERGE"
	CMD_SYNC   = "SYNC"
)

func main() {
	logger.Setup(&logger.Settings{
		Path:           "logs",
		Name:           "penguin",
		Ext:            "log",
		TimeFORMAT:     "2006-01-02",
		OutputTerminal: true,
	})
	logger.SetDebugMode(true)

	opt := penguinDB.Options{
		PersistentDir: "PersistentData",
	}
	db, err := penguinDB.Open(opt)
	if err != nil {
		panic(err)
	}

	logger.Info("penguinDB started")

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
		case CMD_ALL:
			fmt.Println(CMD_ALL)
		case CMD_MERGE:
			fmt.Println(CMD_MERGE)
		case CMD_SYNC:
			err := db.Sync()
			if err != nil {
				fmt.Printf("%s err:%s\n", CMD_SYNC, err)
				break
			}
			fmt.Printf("%s okay\n", CMD_SYNC)
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
