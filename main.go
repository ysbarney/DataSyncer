/*
 *
 */
package main

import (
	"fmt"
	"os"

	"github.com/ysbarney/binlogdump/config"
	"github.com/ysbarney/binlogdump/dump"
)

func main() {
	cfg := config.NewConfig()
	if err := cfg.Load(os.Args[1]); nil != err {
		panic(err)
	}

	if cfg.Dump_type.DType == "mysql" {
		mydump := dump.NewMysqlDump()
		if err := mydump.Init(&cfg.Mysql_conf); nil != err {
			panic(err)
		}

		mydump.Run()
	} else {
		fmt.Printf("dump type: %s is not support now, please check!\n", cfg.Dump_type.DType)
	}
}
