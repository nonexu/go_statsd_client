package statsd

import (
	"fmt"
	"strings"
	"sync"
	"time"
	"io"
	"os"
	"net"
)

var statsd *Statsd = nil

type Statsd struct {
	host     string
	service  string
	conn     io.Writer
	interv   time.Duration
	mutex    sync.Mutex
	buckets  map[string]int
	closeSig chan bool
	work     bool
}

func NewStatsd(srv string, addr string, interval time.Duration) error {
	conn, err := net.DialTimeout("udp", addr, 30*time.Second)
	if err != nil {
		return  err
	}

	host, err := os.Hostname()
	if err != nil {
		host = "UnknownHost"
	}
	host = strings.Replace(host, ".", "-", -1)

	statsd = &Statsd{
		host:     host,
		conn:     conn,
		interv:   interval,
		mutex:    sync.Mutex{},
		buckets:  make(map[string]int),
		closeSig: make(chan bool, 1),
		work:     false,
	}

	go statsd.serve()
	return nil
}

func (self *Statsd) SetWorkState(state bool) {
	self.work = state
}

func (self *Statsd) Working() bool {
	return self.work
}

func (self *Statsd) serve() {
	self.SetWorkState(true)
	for {
		select {
		case <-time.After(statsd.interv):
			go self.flush()
		case <-statsd.closeSig:
			self.SetWorkState(false)
			self.flush()
			return
		}
	}

}

func (self *Statsd) flush() {
	statBuckets := make(map[string]int)
	self.mutex.Lock()
	for bucket, count := range self.buckets {
		if count > 0 {
			statBuckets[bucket] = count
		}
		self.buckets[bucket] = 0
	}
	self.mutex.Unlock()

	for bucket, count := range statBuckets {
		if count > 0 {
			buf := self.formatSumStatsd(bucket, count)
			fmt.Println(buf)
			n, err := self.conn.Write([]byte(buf))
			if err != nil {
				fmt.Printf("err:%s\n", err.Error())
			} else if len(buf) != n {
				fmt.Printf("message missing")
			}
			//UDP发送太快时会出错，根据需求做短暂休眠，
			time.Sleep(time.Millisecond * 10)
		}
	}
}

func (self *Statsd) stop() {
	self.closeSig <- true
}

func (self *Statsd) StatNum(bucket string, num int) {
	if !self.Working() {
		return
	}
	self.mutex.Lock()
	self.buckets[bucket] += num
	self.mutex.Unlock()
}

func (self *Statsd) formatSumStatsd(bucket string, count int) string {
	return fmt.Sprintf("%s.%s.:%d|c\n", self.service, bucket, count)
}

func Stop() {
	if statsd == nil {
		return
	}
	statsd.stop()
}

func StatNum(bucket string, num int) {
	if statsd == nil {
		return
	}
	statsd.StatNum(bucket, num)
}
