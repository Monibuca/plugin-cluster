package cluster

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"

	. "github.com/Monibuca/engine/v3"
	. "github.com/Monibuca/plugin-summary"
	. "github.com/Monibuca/utils/v3"
	"github.com/lucas-clemente/quic-go"
	"golang.org/x/sync/errgroup"
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
	MSG_VIDEOTRACK
	MSG_AUDIOTRACK
)

var (
	config struct {
		OriginServer string
		ListenAddr   string
		Push         bool //推送模式
	}
	edges      sync.Map
	masterConn quic.Stream
	tlsCfg     = generateTLSConfig()
	ctx        = context.Background()
)

func generateTLSConfig() *tls.Config {
	return &tls.Config{}
}
func init() {
	InstallPlugin(&PluginConfig{
		Name:   "Cluster",
		Config: &config,
		Run:    run,
	})
}
func run() {
	var e errgroup.Group
	if config.ListenAddr != "" {
		Summary.Children = make(map[string]*ServerSummary)
		go AddHook("Summary", onSummary)
		log.Printf("server bare start at %s", config.ListenAddr)
		e.Go(func() error {
			return ListenBare(config.ListenAddr)
		})
	}
	if config.OriginServer != "" {
		if config.Push {
			go AddHook(HOOK_PUBLISH, onPublish)
		} else {
			go AddHook(HOOK_SUBSCRIBE, onSubscribe)
		}
		e.Go(func() error {
			return readMaster()
		})
	}
	log.Fatal(e.Wait())
}

func readMaster() (err error) {
	var cmd byte
	for {
		if sess, err := quic.DialAddr(config.OriginServer, tlsCfg, nil); !MayBeError(err) {
			masterConn, err = sess.AcceptStream(ctx)
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
func onSummary(v interface{}) {
	start := v.(bool)
	edges.Range(func(k, v interface{}) bool {
		orderReport(v.(*net.TCPConn), start)
		return true
	})
}

func onSubscribe(v interface{}) {
	if s := v.(*Subscriber); s.Stream == nil {
		go PullUpStream(s.StreamPath)
	}
}
func onPublish(v interface{}) {
	if s := v.(*Stream); masterConn != nil {
		w := bufio.NewWriter(masterConn)
		w.WriteByte(MSG_PUBLISH)
		w.WriteString(s.StreamPath)
		w.WriteByte(0)
		w.Flush()
		sub := Subscriber{
			Type: "Bare",
			ID:   config.OriginServer,
		}
		// stream := Subscriber{
		// OnData: func(p *avformat.SendPacket) error {
		// 	head := pool.GetSlice(9)
		// 	head[0] = p.Type - 7
		// 	binary.BigEndian.PutUint32(head[1:5], p.Timestamp)
		// 	binary.BigEndian.PutUint32(head[5:9], uint32(len(p.Payload)))
		// 	if _, err := w.Write(head); err != nil {
		// 		return err
		// 	}
		// 	pool.RecycleSlice(head)
		// 	if _, err := w.Write(p.Payload); err != nil {
		// 		return err
		// 	}
		// 	return nil
		// }, SubscriberInfo: SubscriberInfo{
		// 	ID:   config.OriginServer,
		// 	Type: "Bare",
		// },
		// }
		sub.Subscribe(s.StreamPath)
	}
}
