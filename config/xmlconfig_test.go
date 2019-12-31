package config

import (
	"testing"
)

func Test_configRead(t *testing.T) {
	c := NewConfig()
	c.Load("./dump.xml")
	c.Print()
}
