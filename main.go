package cluster

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"log"
	"math/rand"
	"sync"
	"time"

	_ "embed"

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
	MSG_UNPUBLISH
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
	edges  sync.Map
	tlsCfg = generateTLSConfig()
	ctx    = context.Background()
	origin Cluster

	//go:embed keyPEM
	keyPEM []byte

	//go:embed certPEM
	certPEM []byte
)

func generateTLSConfig() *tls.Config {
	// key, err := rsa.GenerateKey(crand.Reader, 1024)
	// if err != nil {
	// 	panic(err)
	// }
	// template := x509.Certificate{SerialNumber: big.NewInt(1)}
	// certDER, err := x509.CreateCertificate(crand.Reader, &template, &template, &key.PublicKey, key)
	// if err != nil {
	// 	panic(err)
	// }
	// keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	// certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		panic(err)
	}
	return &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		NextProtos:   []string{"monibuca"},
	}
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
	hooks := map[string]interface{}{HOOK_PUBLISH: onPublish, HOOK_STREAMCLOSE: onStreamClose}
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
			var masterConn quic.Stream
			masterConn, err = sess.AcceptStream(ctx)
			origin.Reader = bufio.NewReader(masterConn)
			origin.Writer = bufio.NewWriter(masterConn)
			origin.tracks = make(map[string]map[string]Track)
			Printf("connect to master %s reporting", config.OriginServer)
			for report(); err == nil; {
				if cmd, err = origin.ReadByte(); !MayBeError(err) {
					switch cmd {
					case MSG_SUMMARY: //收到源服务器指令，进行采集和上报
						if cmd, err = origin.ReadByte(); !MayBeError(err) {
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
						if streamPath := origin.ReadString(); streamPath != "" {
							if s := FindStream(streamPath); s == nil {
								os := &Stream{
									StreamPath: streamPath,
									Type:       STREAMTYPE_ORIGIN,
									ExtraProp:  &origin,
								}
								os.Publish()
							} else if s.Type == STREAMTYPE_ORIGIN {
								s.Update()
							}
						}
					case MSG_UNPUBLISH: //收到源服务器的取消发布流信号，关闭对应的流
						if streamPath := origin.ReadString(); streamPath != "" {
							if s := FindStream(streamPath); s != nil && s.Type == STREAMTYPE_ORIGIN {
								s.Close()
							}
						}
					case MSG_VIDEOTRACK:
						origin.ReadVideoTrack()
					case MSG_AUDIOTRACK:
						origin.ReadAudioTrack()
					case MSG_VIDEO:
						origin.ReadVideoPack()
					case MSG_AUDIO:
						origin.ReadAudioPack()
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
		_, err = origin.Write(data)
	}
}

//定时上报
func onReport() {
	for c := time.NewTicker(time.Second).C; Summary.Running(); <-c {
		report()
	}
}

func broadcast(do func(*Cluster)) {
	edges.Range(func(k, v interface{}) bool {
		do(v.(*Cluster))
		return true
	})
}

//通知从服务器需要上报或者关闭上报
func onSummary(start bool) {
	broadcast(func(c *Cluster) {
		c.orderReport(start)
	})
}

func onSubscribe(s *Subscriber, count int) {
	if s.Stream.Type == STREAMTYPE_ORIGIN {
		if count == 1 {
			origin.WriteSubscribe(s.StreamPath)
		}
	}
}
func onUnsubscribe(s *Subscriber, count int) {
	if s.Stream.Type == STREAMTYPE_ORIGIN {
		if count == 0 {
			origin.WriteUnSubscribe(s.StreamPath)
		}
	}
}

// 流关闭向下级广播
func onStreamClose(s *Stream) {
	if s.Type != STREAMTYPE_ORIGIN {
		broadcast(func(w *Cluster) {
			w.WriteUnPublish(s.StreamPath)
		})
	}
}
func onPublish(s *Stream) {
	var edgePublisher *Cluster
	if s.Type == STREAMTYPE_SINK {
		edgePublisher = s.ExtraProp.(*Cluster)
	}
	// 作为源服务器，需要向所有下级服务器广播流信息
	if config.ListenAddr != "" {
		broadcast(func(e *Cluster) {
			if e != edgePublisher {
				e.WritePublish(s.StreamPath)
			}
		})
		go s.VideoTracks.OnTrack(func(name string, t Track) {
			vt := t.(*VideoTrack)
			broadcast(func(e *Cluster) {
				if e != edgePublisher {
					e.WriteVT(s.StreamPath, name, vt)
				}
			})
			// go vt.Play(nil, func(vp VideoPack) {
			// 	broadcast(func(e *Origin) {
			// 		if e != edgePublisher {
			// 			e.WriteVideoPack(name, vp)
			// 		}
			// 	})
			// })
		})
		go s.AudioTracks.OnTrack(func(name string, t Track) {
			at := t.(*AudioTrack)
			broadcast(func(e *Cluster) {
				if e != edgePublisher {
					e.WriteAT(s.StreamPath, name, at)
				}
			})
			// go at.Play(nil, func(ap AudioPack) {
			// 	broadcast(func(e *Origin) {
			// 		if e != edgePublisher {
			// 			e.WriteAudioPack(name, ap)
			// 		}
			// 	})
			// })
		})
	}
	// 作为从服务器，需要向源服务器推送
	if origin.Reader != nil {
		origin.WritePublish(s.StreamPath)
		go s.VideoTracks.OnTrack(func(name string, t Track) {
			vt := t.(*VideoTrack)
			origin.WriteVT(s.StreamPath, name, vt)
			go vt.Play(nil, func(vp VideoPack) {
				origin.WriteVideoPack(s.StreamPath, name, vp)
			})
		})
		go s.AudioTracks.OnTrack(func(name string, t Track) {
			at := t.(*AudioTrack)
			origin.WriteAT(s.StreamPath, name, at)
			go at.Play(nil, func(ap AudioPack) {
				origin.WriteAudioPack(s.StreamPath, name, ap)
			})
		})
	}
}
