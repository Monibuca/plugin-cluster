package cluster

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"log"
	"math/rand"
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
	MSG_UNSUBSCRIBE
	MSG_AUTH
	MSG_SUMMARY
	MSG_LOG
	MSG_PUBLISH
	MSG_VIDEOTRACK
	MSG_AUDIOTRACK

	STREAMTYPE_ORIGIN = "ClusterOrigin" //源流
	STREAMTYPE_SINK   = "ClusterSink"   //下级流
)

var (
	config struct {
		OriginServer string
		ListenAddr   string
	}
	edges      sync.Map
	masterConn quic.Stream
	masterWriter *bufio.Writer
	tlsCfg     = generateTLSConfig()
	ctx        = context.Background()
)

type OriginStream struct {
	hasSubscriber bool //是否含有订阅者
}

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
	hooks := map[string]func(interface{}){HOOK_PUBLISH: onPublish}
	if config.ListenAddr != "" {
		Summary.Children = make(map[string]*ServerSummary)
		hooks["Summary"] = onSummary
		e.Go(ListenBare)
	}
	if config.OriginServer != "" {
		hooks[HOOK_SUBSCRIBE] = onSubscribe
		hooks[HOOK_UNSUBSCRIBE] = onUnsubscribe
		e.Go(readMaster)
	}
	AddHooks(hooks)
	log.Fatal(e.Wait())
}

// 读取源服务器信息
func readMaster() (err error) {
	var cmd byte
	var sess quic.Session
	for {
		if sess, err = quic.DialAddr(config.OriginServer, tlsCfg, nil); !MayBeError(err) {
			masterConn, err = sess.AcceptStream(ctx)
			masterWriter = bufio.NewWriter(masterConn)
			reader := bufio.NewReader(masterConn)
			Printf("connect to master %s reporting", config.OriginServer)
			for report(); err == nil; {
				if cmd, err = reader.ReadByte(); !MayBeError(err) {
					switch cmd {
					case MSG_SUMMARY: //收到源服务器指令，进行采集和上报
						if cmd, err = reader.ReadByte(); !MayBeError(err) {
							if cmd == 1 {
								Printf("receive summary request from %s", config.OriginServer)
								Summary.Add()
								go onReport()
							} else {
								Printf("receive stop summary request from %s", config.OriginServer)
								Summary.Done()
							}
						}
					case MSG_PUBLISH: //收到源服务器的发布流信号，创建对应的流，准备接收数据
						if streamPath, err := reader.ReadString(0); err == nil {
							if s := FindStream(streamPath); s == nil {
								os := &Stream{
									StreamPath: streamPath,
									Type:       STREAMTYPE_ORIGIN,
									ExtraProp:  new(OriginStream),
								}
								os.Publish()
							} else if s.Type == STREAMTYPE_ORIGIN {
								s.Update()
							}
						}
					}
				}
			}
		}
		t := 5 + rand.Int63n(5)
		Printf("reconnect to OriginServer %s after %d seconds", config.OriginServer, t)
		time.Sleep(time.Duration(t) * time.Second)
	}
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
	for c := time.NewTicker(time.Second).C; Summary.Running(); <-c {
		report()
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
		orderReport(v.(io.Writer), start)
		return true
	})
}

func onSubscribe(v interface{}) {
	if s := v.(*Subscriber); s.Stream.Type == STREAMTYPE_ORIGIN {
		o := s.Stream.ExtraProp.(*OriginStream)
		if !o.hasSubscriber && masterConn != nil {
			w := masterWriter
			w.WriteByte(MSG_SUBSCRIBE)
			w.WriteString(s.Stream.StreamPath)
			w.WriteByte(0)
			w.Flush()
		}
		//go PullUpStream(s.StreamPath)
	}
}
func onUnsubscribe(v interface{}) {
	if s := v.(*Subscriber); s.Stream.Type == STREAMTYPE_ORIGIN {
		o := s.Stream.ExtraProp.(*OriginStream)
		if o.hasSubscriber && len(s.Stream.Subscribers) == 0 && masterConn != nil {
			w := masterWriter
			w.WriteByte(MSG_UNSUBSCRIBE)
			w.WriteString(s.Stream.StreamPath)
			w.WriteByte(0)
			w.Flush()
		}
	}
}
func onPublish(v interface{}) {
	if s := v.(*Stream); masterConn != nil {
		w := masterWriter
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
