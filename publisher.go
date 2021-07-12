package cluster

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"

	. "github.com/Monibuca/engine/v3"
	. "github.com/Monibuca/utils/v3"
	. "github.com/Monibuca/utils/v3/codec"
	"github.com/lucas-clemente/quic-go"
)

type Receiver struct {
	*Stream
	io.Reader
	*bufio.Writer
}

// func (p *Receiver) Auth(authSub *Subscriber) {
// 	p.WriteByte(MSG_AUTH)
// 	p.WriteString(authSub.ID + "," + authSub.Sign)
// 	p.WriteByte(0)
// 	p.Flush()
// }

func (p *Receiver) readAVPacket(avType byte) (timestamp uint32, payload []byte, err error) {
	buf := GetSlice(4)
	defer RecycleSlice(buf)
	_, err = io.ReadFull(p, buf)
	if err != nil {
		println(err.Error())
		return
	}
	timestamp = binary.BigEndian.Uint32(buf)
	_, err = io.ReadFull(p, buf)
	if MayBeError(err) {
		return
	}
	payload = make([]byte, int(binary.BigEndian.Uint32(buf)))
	_, err = io.ReadFull(p, payload)
	MayBeError(err)
	return
}

func PullUpStream(streamPath string) {
	sess, err := quic.DialAddr(config.OriginServer, tlsCfg, nil)
	if MayBeError(err) {
		return
	}
	conn, err := sess.AcceptStream(ctx)
	brw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	p := &Receiver{
		&Stream{
			StreamPath: streamPath,
		},
		brw.Reader,
		brw.Writer,
	}
	if p.Publish() {
		p.Type = "Cluster"
		p.WriteByte(MSG_SUBSCRIBE)
		p.WriteString(streamPath)
		p.WriteByte(0)
		p.Flush()
		// for _, v := range p.Subscribers {
		// 	p.Auth(v)
		// }
	} else {
		return
	}
	defer p.Close()
	for cmd, err := brw.ReadByte(); !MayBeError(err); cmd, err = brw.ReadByte() {
		switch cmd {
		case MSG_AUDIO:
			name, err := brw.ReadString(0)
			if MayBeError(err) {
				return
			}
			at := p.WaitAudioTrack(name)
			if t, payload, err := p.readAVPacket(FLV_TAG_TYPE_AUDIO); err == nil {
				at.PushByteStream(t, payload)
			}
		case MSG_VIDEO:
			name, err := brw.ReadString(0)
			if MayBeError(err) {
				return
			}
			vt := p.WaitVideoTrack(name)
			if t, payload, err := p.readAVPacket(FLV_TAG_TYPE_VIDEO); err == nil && len(payload) > 2 {
				vt.PushByteStream(t, payload)
			}
		// case MSG_AUTH:
		// 	cmd, err = brw.ReadByte()
		// 	if MayBeError(err) {
		// 		return
		// 	}
		// 	bytes, err := brw.ReadBytes(0)
		// 	if MayBeError(err) {
		// 		return
		// 	}
		// 	subId := strings.Split(string(bytes[0:len(bytes)-1]), ",")[0]
		// 	if v, ok := p.Subscribers[subId]; ok {
		// 		if cmd != 1 {
		// 			v.Cancel()
		// 		}
		// 	}
		default:
			log.Printf("unknown cmd:%v", cmd)
		}
	}
}
