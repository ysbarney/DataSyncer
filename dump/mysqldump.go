/*
 * dump mysql binlog
 */
package dump

import (
	"context"
	"encoding/json"
	"fmt"

	//"os"
	"strconv"
	"strings"
	"time"

	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/ysbarney/binlogdump/config"
	//"github.com/sirupsen/logrus"
)

var curBinlogFilename string
var curEventGTID string

type msgput struct {
	Prefix          string        `json:"prefix"`
	Filename        string        `json:"filename"`
	Logtype         string        `json:"logtype"`
	Eventtype       int           `json:"eventtype"`
	Eventtypestr    string        `json:"eventtypestr"`
	Db              string        `json:"db"`
	Table           string        `json:"table"`
	Localip         string        `json:"localip"`
	Localport       int           `json:"localport"`
	Begintime       uint64        `json:"begintime"`
	Gtid            string        `json:"gtid"`
	Xid             string        `json:"xid"`
	Serverid        string        `json:"serverid"`
	Event_index     string        `json:"event_index"`
	Gtid_commitid   string        `json:"gtid_commitid"`
	Gtid_flag2      string        `json:"gtid_flags"`
	Field           []interface{} `json:"field"`
	Where           []interface{} `json:"where"`
	Sub_event_index string        `json:"sub_event_index"`
	Sequence_num    string        `json:"sequence_num"`
	Sql             string        `json:"sql"`
}

type MysqlDump struct {
	MysqlSyncer   *replication.BinlogSyncer
	MysqlStreamer *replication.BinlogStreamer
	binlogfile    string
	binlogpos     int
	gtidstr       string
	enableGTID    bool
	flavor        string
	localip       string
	localport     int
}

func NewMysqlDump() *MysqlDump {
	m := new(MysqlDump)
	return m
}

func (m *MysqlDump) Init(config *config.MysqlConfig) error {
	hosts := strings.Split(config.Host, "_")
	if len(hosts) != 2 {
		err := fmt.Errorf("input mysql host: %s is not valid", config.Host)
		return err
	}

	port, err := strconv.ParseInt(hosts[1], 10, 16)
	if nil != err {
		return err
	}

	m.binlogfile = config.BinlogFile
	m.binlogpos = config.BinlogPos
	m.gtidstr = config.GtidStr
	m.enableGTID = config.EnableGTID
	m.flavor = config.Flavor
	m.localip = hosts[0]
	m.localport = int(port)

	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(config.ServerID),
		Flavor:   config.Flavor,
		Host:     hosts[0],
		Port:     uint16(port),
		User:     config.User,
		Password: config.Passwd,
	}

	m.MysqlSyncer = replication.NewBinlogSyncer(cfg)
	return nil
}

func (m *MysqlDump) Run() error {
	if m.enableGTID {
		gset, err := mysql.ParseGTIDSet(m.flavor, m.gtidstr)
		if nil != err {
			return err
		}

		m.MysqlStreamer, err = m.MysqlSyncer.StartSyncGTID(gset)
		if nil != err {
			return err
		}
	} else {
		var err error
		m.MysqlStreamer, err = m.MysqlSyncer.StartSync(mysql.Position{m.binlogfile, uint32(m.binlogpos)})
		if nil != err {
			return err
		}
	}

	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		ev, err := m.MysqlStreamer.GetEvent(ctx)
		if context.DeadlineExceeded == err {
			continue
		}

		m.outputNewEvent(ev)
	}
}

func (m *MysqlDump) outputNewEvent(ev *replication.BinlogEvent) {
	switch ev.Header.EventType {
	case replication.WRITE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv1,
		replication.WRITE_ROWS_EVENTv2, replication.UPDATE_ROWS_EVENTv2, replication.DELETE_ROWS_EVENTv2:
		m.outputDmlEvent(ev)
	case replication.GTID_EVENT, replication.MARIADB_GTID_EVENT:
		m.outputGtidEvent(ev)
	case replication.XID_EVENT:
		m.outputXidEvent(ev)
	case replication.QUERY_EVENT:
		m.outputQueryLogEvent(ev)
	case replication.ROTATE_EVENT:
		p, ok := (ev.Event).(*replication.RotateEvent)
		if !ok {
			panic("ev is not rotate event")
		}
		curBinlogFilename = string(p.NextLogName)
	default:
		//fmt.Printf("eventtype:%s  %+v\n", ev.Header.EventType.String(), ev)
	}
}

func (m *MysqlDump) fillCommValue(ev *replication.BinlogEvent) msgput {
	msg := msgput{
		Filename:     curBinlogFilename,
		Logtype:      "mysqlbinlog",
		Eventtype:    int(ev.Header.EventType),
		Eventtypestr: ev.Header.EventType.String(),
		Localip:      m.localip,
		Localport:    m.localport,
		Begintime:    uint64(time.Unix(int64(ev.Header.Timestamp), 0).Unix()),
		Serverid:     strconv.FormatUint(uint64(ev.Header.ServerID), 10),
	}

	return msg
}

func (m *MysqlDump) outputDmlEvent(ev *replication.BinlogEvent) {
	//fmt.Println("=== dump dml event ===")
	p, ok := (ev.Event).(*replication.RowsEvent)
	if !ok {
		panic("ev is not row event")
	}
	msg := m.fillCommValue(ev)
	msg.Db = string(p.Table.Schema)
	msg.Table = string(p.Table.Table)
	msg.Gtid = curEventGTID

	if ev.Header.EventType == replication.UPDATE_ROWS_EVENTv1 ||
		ev.Header.EventType == replication.UPDATE_ROWS_EVENTv2 {
		msg.Where = p.Rows[1]
	}
	msg.Field = p.Rows[0]

	json_output, err := json.Marshal(msg)
	if nil != err {
		panic(err)
	}

	fmt.Printf("%s\n", json_output)
}

func (m *MysqlDump) outputGtidEvent(ev *replication.BinlogEvent) {
	//fmt.Println("=== dump gtid event ===")
	p, ok := (ev.Event).(*replication.GTIDEvent)
	if !ok {
		panic("ev is not gtid event")
	}

	msg := m.fillCommValue(ev)
	u, _ := uuid.FromBytes(p.SID)
	msg.Gtid = u.String() + ":" + strconv.FormatInt(p.GNO, 10)
	curEventGTID = msg.Gtid
	msg.Sub_event_index = "1"
	msg.Sequence_num = strconv.FormatInt(p.SequenceNumber, 10)

	json_output, err := json.Marshal(msg)
	if nil != err {
		panic(err)
	}

	fmt.Printf("%s\n", json_output)
}

func (m *MysqlDump) outputXidEvent(ev *replication.BinlogEvent) {
	//fmt.Println("=== dump xid event ===")
	p, ok := (ev.Event).(*replication.XIDEvent)
	if !ok {
		panic("ev is not query log event")
	}

	msg := m.fillCommValue(ev)
	msg.Gtid = curEventGTID
	msg.Sub_event_index = "1"
	msg.Xid = strconv.FormatUint(p.XID, 10)
	json_output, err := json.Marshal(msg)
	if nil != err {
		panic(err)
	}

	fmt.Printf("%s\n", json_output)
}

func (m *MysqlDump) outputQueryLogEvent(ev *replication.BinlogEvent) {
	//fmt.Println("=== dump query log event ===")
	p, ok := (ev.Event).(*replication.QueryEvent)
	if !ok {
		panic("ev is not query log event")
	}
	msg := m.fillCommValue(ev)
	msg.Db = string(p.Schema)
	msg.Sql = string(p.Query)

	msg.Gtid = curEventGTID
	msg.Sub_event_index = "1"

	json_output, err := json.Marshal(msg)
	if nil != err {
		panic(err)
	}

	fmt.Printf("%s\n", json_output)
}
