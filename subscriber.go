package cluster

import (
	"bufio"
	"encoding/json"
	"fmt"

	. "github.com/Monibuca/engine/v3"
	. "github.com/Monibuca/plugin-summary"
	. "github.com/Monibuca/utils/v3"
	"github.com/Monibuca/utils/v3/codec"
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
			return err
		}
		stream, err := sess.AcceptStream(ctx)
		if MayBeError(err) {
			return err
		}
		go process(sess, stream)
	}
}

func process(session quic.Session, stream quic.Stream) {
	defer stream.Close()
	reader := bufio.NewReader(stream)
	subscriber := Subscriber{
		ID:   fmt.Sprintf("%d", stream.StreamID()),
		Type: "Cluster",
		// OnAudio: func (pack AudioPack)  {

		// },
		// OnVideo: func(pack VideoPack) {
		// 	head := pool.GetSlice(9)
		// 	head[0] = p.Type - 7
		// 	binary.BigEndian.PutUint32(head[1:5], p.Timestamp)
		// 	binary.BigEndian.PutUint32(head[5:9], uint32(len(p.Payload)))
		// 	if _, err := conn.Write(head); err != nil {
		// 		return err
		// 	}
		// 	pool.RecycleSlice(head)
		// 	if _, err := conn.Write(p.Payload); err != nil {
		// 		return err
		// 	}
		// 	return nil
		// },
	}
	var p Receiver
	p.Reader = reader
	connAddr := session.RemoteAddr().String()
	defer p.Close()
	for {
		cmd, err := reader.ReadByte()
		if err != nil {
			return
		}
		// 通过推模式过来的数据
		if p.Stream != nil {
			switch cmd {
			case MSG_AUDIO:
				name, err := reader.ReadString(0)
				if err != nil {
					return
				}
				at := p.WaitAudioTrack(name)
				if t, payload, err := p.readAVPacket(codec.FLV_TAG_TYPE_AUDIO); err == nil {
					at.PushByteStream(t, payload)
				}
			case MSG_VIDEO:
				name, err := reader.ReadString(0)
				if err != nil {
					return
				}
				vt := p.WaitVideoTrack(name)
				if t, payload, err := p.readAVPacket(codec.FLV_TAG_TYPE_VIDEO); err == nil && len(payload) > 2 {
					vt.PushByteStream(t, payload)
				}
			}
			continue
		}
		bytes, err := reader.ReadBytes(0)
		if err != nil {
			return
		}
		bytes = bytes[0 : len(bytes)-1]
		switch cmd {
		// 从服务器通过推模式，向主服务器推送
		case MSG_PUBLISH:
			p.Stream = &Stream{
				StreamPath: string(bytes),
				Type:       "Cluster",
			}
			p.Publish()
		case MSG_VIDEOTRACK:
			name, err := reader.ReadString(0)
			if err != nil {
				Println(err)
			}
			vt := p.NewVideoTrack(0)
			vt.CodecID, err = reader.ReadByte()
			if MayBeError(err) {
				return
			}
			p.VideoTracks.AddTrack(name, vt)
		case MSG_AUDIOTRACK:
			name, err := reader.ReadString(0)
			if MayBeError(err) {
				return
			}
			at := p.NewAudioTrack(0)
			at.CodecID, err = reader.ReadByte()
			if MayBeError(err) {
				return
			}
			p.AudioTracks.AddTrack(name, at)
			// 拉模式，从服务器向主服务器订阅
		case MSG_SUBSCRIBE:
			if subscriber.Stream != nil {
				Printf("bare stream already exist from %s", session.RemoteAddr())
				return
			}
			if err = subscriber.Subscribe(string(bytes)); err == nil {

			}
			if MayBeError(err) {
				return
			}

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
			if err = json.Unmarshal(bytes, summary); err == nil {
				summary.Address = connAddr
				Summary.Report(summary)
				if _, ok := edges.Load(connAddr); !ok {
					if edges.Store(connAddr, stream); Summary.Running() {
						orderReport(stream, true)
					}
					defer edges.Delete(connAddr)
				}
			}
		default:
			fmt.Printf("bare receive unknown cmd:%d from %s", cmd, connAddr)
			return
		}
	}
}
