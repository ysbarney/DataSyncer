/*
* read xml configuration
 */
package config

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"os"
)

type Config struct {
	XMLName    xml.Name    `xml:"config"`
	Dump_type  DumpType    `xml:"dump"`
	Mysql_conf MysqlConfig `xml:"mysql"`
	Zk_conf    ZkConfig    `xml:"zookeeper"`
	Log_conf   LogConfig   `xml:"log"`
}

type DumpType struct {
	DType string `xml:"type,attr"`
}

type MysqlConfig struct {
	Host       string `xml:"host,attr"`
	User       string `xml:"user,attr"`
	Passwd     string `xml:"passwd,attr"`
	EnableGTID bool   `xml:"enableGTID,attr"`
	Flavor     string `xml:"flavor,attr"`
	ServerID   int    `xml:"serverid,attr"`
	BinlogFile string `xml:"binglogfile,attr"`
	BinlogPos  int    `xml:"binlogpos,attr"`
	GtidStr    string `xml:"gtidstr,attr"`
}

type ZkConfig struct {
	Zkiplists string `xml:"iplist,attr"`
	Zkrootdir string `xml:"zkrootdir,attr"`
	Zktimeout int    `xml:"timeout,attr"`
}

type LogConfig struct {
	Logdir  string `xml:"dir,attr"`
	Logsize int    `xml:"size,attr"`
}

func NewConfig() *Config {
	conf := new(Config)
	return conf
}

func (c *Config) Load(ConfigFile string) error {
	file, err := os.Open(ConfigFile)
	if nil != err {
		return err
	}

	defer file.Close()

	data, err := ioutil.ReadAll(file)
	if nil != err {
		return err
	}

	if err := xml.Unmarshal(data, c); nil != err {
		return err
	}

	return nil
}

func (c *Config) Print() {
	fmt.Println(c)
}
