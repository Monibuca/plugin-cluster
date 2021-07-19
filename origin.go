package cluster

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"time"

	. "github.com/Monibuca/engine/v3"
	. "github.com/Monibuca/plugin-summary"
	. "github.com/Monibuca/utils/v3"
	quic "github.com/lucas-clemente/quic-go"
)

func ListenBare() error {
	listener, err := quic.ListenAddr(config.ListenAddr, tlsCfg, nil)
	if MayBeError(err) {
		return err
	}
	Printf("server bare start at %s", config.ListenAddr)
	for {
		sess, err := listener.Accept(ctx)
		if MayBeError(err) {
			continue
		}
		stream, err := sess.AcceptStream(ctx)
		if MayBeError(err) {
			continue
		}
		go process(sess, stream)
	}
}

// 处理和下级服务器之间的通讯
func process(session quic.Session, stream quic.Stream) {
	defer stream.Close()
	reader := bufio.NewReader(stream)
	var o Cluster
	o.tracks = make(map[string]map[string]Track)
	o.Reader = reader
	o.Writer = bufio.NewWriter(stream)
	connAddr := session.RemoteAddr().String()
	subscribers := make(map[string]context.CancelFunc)
	o.WritePulse()
	for {
		cmd, err := reader.ReadByte()
		if err != nil {
			return
		}
		switch cmd {
		case MSG_PUBLISH:
			s := &Stream{
				StreamPath: o.ReadString(),
				Type:       STREAMTYPE_SINK,
				ExtraProp:  &o,
			}
			s.Publish()
		case MSG_VIDEOTRACK:
			o.ReadVideoTrack()
		case MSG_AUDIOTRACK:
			o.ReadAudioTrack()
		case MSG_SUBSCRIBE:
			if streamPath := origin.ReadString(); streamPath != "" {
				if s := FindStream(streamPath); s != nil {
					ctx, cancel := context.WithCancel(ctx)
					subscribers[streamPath] = cancel
					o.trackMutex.Lock()
					if m, ok := o.tracks[streamPath]; ok {
						for name, t := range m {
							switch track := t.(type) {
							case *VideoTrack:
								go track.Play(ctx, func(vp VideoPack) {
									o.WriteVideoPack(streamPath, name, vp)
								})
							case *AudioTrack:
								go track.Play(ctx, func(vp AudioPack) {
									o.WriteAudioPack(streamPath, name, vp)
								})
							}
						}
					}
					o.trackMutex.Unlock()
				}
			}
		case MSG_UNSUBSCRIBE:
			if streamPath := origin.ReadString(); streamPath != "" {
				if s := FindStream(streamPath); s != nil {
					subscribers[streamPath]()
				}
			}
		case MSG_VIDEO:
			o.ReadVideoPack()
		case MSG_AUDIO:
			o.ReadAudioPack()

		// case MSG_AUTH:
		// 	sign := strings.Split(string(bytes), ",")
		// 	head := []byte{MSG_AUTH, 2}
		// 	if len(sign) > 1 && AuthHooks.Trigger(sign[1]) == nil {
		// 		head[1] = 1
		// 	}
		// 	conn.Write(head)
		// 	conn.Write(bytes[0 : len(bytes)+1])
		case MSG_SUMMARY: //收到从服务器发来报告，加入摘要中
			summary := &ServerSummary{}
			if err = json.Unmarshal([]byte(o.ReadString()), summary); err == nil {
				summary.Address = connAddr
				Summary.Report(summary)
				if _, ok := edges.Load(connAddr); !ok {
					if edges.Store(connAddr, &o); Summary.Running() {
						o.WriteSummary(1)
					}
					defer edges.Delete(connAddr)
				}
			}
		case MSG_PULSE:
			time.AfterFunc(time.Second, o.WritePulse)
		default:
			fmt.Printf("bare receive unknown cmd:%d from %s", cmd, connAddr)
			return
		}
	}
}
