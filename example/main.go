package main

import (
	"bufio"
	"fmt"
	penguinDB "github.com/paranoidxc/PenguinDB"
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
	opt := penguinDB.Options{
		StorePath: "storeData",
	}
	db, err := penguinDB.Open(opt)
	if err != nil {
		panic(err)
	}

	for {
		reader := bufio.NewReader(os.Stdin)
		fmt.Printf("pengiun> ")

		cmd, _ := reader.ReadString('\n')
		cmd = strings.ToUpper(strings.TrimSpace(cmd))
		switch cmd {
		case CMD_SET:
			key, _ := reader.ReadString('\n')
			value, _ := reader.ReadString('\n')

			key = strings.TrimSpace(key)
			value = strings.TrimSpace(value)

			_, err := db.Set([]byte(key), []byte(value))
			if err != nil {
				fmt.Println("err:", err)
			} else {
				fmt.Printf("%s %s=%s\n", CMD_SET, key, value)
			}
		case CMD_GET:
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			val, err := db.Get([]byte(key))
			if err != nil {
				fmt.Println("err:", err)
			} else {
				fmt.Printf("%s %s %s\n", CMD_GET, key, val)
			}
		case CMD_DELETE:
			key, _ := reader.ReadString('\n')
			key = strings.TrimSpace(key)
			err := db.Delete([]byte(key))
			fmt.Println(CMD_DELETE, " ", key)
			if err != nil {
				fmt.Println("err:", err)
			}
		case CMD_ALL:
			fmt.Println(CMD_ALL)
		case CMD_MERGE:
			fmt.Println(CMD_MERGE)
		case CMD_SYNC:
			fmt.Println(CMD_SYNC)
		case CMD_QUIT, CMD_EXIT:
			os.Exit(0)
		default:
			fmt.Println("cmd not found")
		}
	}
}
