package cluster

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/Monibuca/engine/v2/avformat"
	"github.com/Monibuca/engine/v2/pool"
	"golang.org/x/sync/errgroup"

	. "github.com/Monibuca/engine/v2"
)

const (
	_ byte = iota
	MSG_AUDIO
	MSG_VIDEO
	MSG_SUBSCRIBE
	MSG_AUTH
	MSG_SUMMARY
	MSG_LOG
	MSG_PUBLISH
)

var (
	config struct {
		OriginServer string
		ListenAddr   string
		Push         bool //推送模式
	}
	edges      sync.Map
	masterConn *net.TCPConn
)

func init() {
	InstallPlugin(&PluginConfig{
		Name:   "Cluster",
		Type:   PLUGIN_HOOK | PLUGIN_PUBLISHER | PLUGIN_SUBSCRIBER,
		Config: &config,
		Run:    run,
	})
}
func run() {
	var e errgroup.Group
	if config.ListenAddr != "" {
		Summary.Children = make(map[string]*ServerSummary)
		OnSummaryHooks.AddHook(onSummary)
		log.Printf("server bare start at %s", config.ListenAddr)
		e.Go(func() error {
			return ListenBare(config.ListenAddr)
		})
	}
	if config.OriginServer != "" {
		if config.Push {
			OnPublishHooks.AddHook(onPublish)
		} else {
			OnSubscribeHooks.AddHook(onSubscribe)
		}
		addr, err := net.ResolveTCPAddr("tcp", config.OriginServer)
		if MayBeError(err) {
			return
		}
		e.Go(func() error {
			return readMaster(addr)
		})
	}
	log.Fatal(e.Wait())
}

func readMaster(addr *net.TCPAddr) (err error) {
	var cmd byte
	for {
		if masterConn, err = net.DialTCP("tcp", nil, addr); !MayBeError(err) {
			reader := bufio.NewReader(masterConn)
			log.Printf("connect to master %s reporting", config.OriginServer)
			for report(); err == nil; {
				if cmd, err = reader.ReadByte(); !MayBeError(err) {
					switch cmd {
					case MSG_SUMMARY: //收到主服务器指令，进行采集和上报
						log.Println("receive summary request from OriginServer")
						if cmd, err = reader.ReadByte(); !MayBeError(err) {
							if cmd == 1 {
								Summary.Add()
								go onReport()
							} else {
								Summary.Done()
							}
						}
					}
				}
			}
		}
		t := 5 + rand.Int63n(5)
		log.Printf("reconnect to OriginServer %s after %d seconds", config.OriginServer, t)
		time.Sleep(time.Duration(t) * time.Second)
	}
	return
}
func report() {
	if b, err := json.Marshal(Summary); err == nil {
		data := make([]byte, len(b)+2)
		data[0] = MSG_SUMMARY
		copy(data[1:], b)
		data[len(data)-1] = 0
		_, err = masterConn.Write(data)
	}
}

//定时上报
func onReport() {
	for range time.NewTicker(time.Second).C {
		if Summary.Running() {
			report()
		} else {
			return
		}
	}
}
func orderReport(conn io.Writer, start bool) {
	b := []byte{MSG_SUMMARY, 0}
	if start {
		b[1] = 1
	}
	conn.Write(b)
}

//通知从服务器需要上报或者关闭上报
func onSummary(start bool) {
	edges.Range(func(k, v interface{}) bool {
		orderReport(v.(*net.TCPConn), start)
		return true
	})
}

func onSubscribe(s *Subscriber) {
	if s.Publisher == nil {
		go PullUpStream(s.StreamPath)
	}
}
func onPublish(s *Stream) {
	if masterConn != nil {
		w := bufio.NewWriter(masterConn)
		w.WriteByte(MSG_PUBLISH)
		w.WriteString(s.StreamPath)
		w.WriteByte(0)
		w.Flush()
		stream := Subscriber{
			OnData: func(p *avformat.SendPacket) error {
				head := pool.GetSlice(9)
				head[0] = p.Type - 7
				binary.BigEndian.PutUint32(head[1:5], p.Timestamp)
				binary.BigEndian.PutUint32(head[5:9], uint32(len(p.Payload)))
				if _, err := w.Write(head); err != nil {
					return err
				}
				pool.RecycleSlice(head)
				if _, err := w.Write(p.Payload); err != nil {
					return err
				}
				return nil
			}, SubscriberInfo: SubscriberInfo{
				ID:   config.OriginServer,
				Type: "Bare",
			},
		}
		go stream.Subscribe(s.StreamPath)
	}
}
