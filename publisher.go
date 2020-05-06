package cluster

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
	"net"
	"strings"

	. "github.com/Monibuca/engine/v2"
	"github.com/Monibuca/engine/v2/avformat"
	"github.com/Monibuca/engine/v2/pool"
)

type Receiver struct {
	Publisher
	io.Reader
	*bufio.Writer
}

func (p *Receiver) Auth(authSub *Subscriber) {
	p.WriteByte(MSG_AUTH)
	p.WriteString(authSub.ID + "," + authSub.Sign)
	p.WriteByte(0)
	p.Flush()
}

func (p *Receiver) readAVPacket(avType byte) (timestamp uint32, payload []byte, err error) {
	buf := pool.GetSlice(4)
	defer pool.RecycleSlice(buf)
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
	addr, err := net.ResolveTCPAddr("tcp", config.OriginServer)
	if MayBeError(err) {
		return
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if MayBeError(err) {
		return
	}
	brw := bufio.NewReadWriter(bufio.NewReader(conn), bufio.NewWriter(conn))
	p := &Receiver{
		Reader: brw.Reader,
		Writer: brw.Writer,
	}
	if p.Publish(streamPath) {
		p.Type = "Cluster"
		p.WriteByte(MSG_SUBSCRIBE)
		p.WriteString(streamPath)
		p.WriteByte(0)
		p.Flush()
		for _, v := range p.Subscribers {
			p.Auth(v)
		}
	} else {
		return
	}
	defer p.Cancel()
	for cmd, err := brw.ReadByte(); !MayBeError(err); cmd, err = brw.ReadByte() {
		switch cmd {
		case MSG_AUDIO:
			if t, payload, err := p.readAVPacket(avformat.FLV_TAG_TYPE_AUDIO); err == nil {
				p.PushAudio(t, payload)
			}
		case MSG_VIDEO:
			if t, payload, err := p.readAVPacket(avformat.FLV_TAG_TYPE_VIDEO); err == nil && len(payload) > 2 {
				p.PushVideo(t, payload)
			}
		case MSG_AUTH:
			cmd, err = brw.ReadByte()
			if MayBeError(err) {
				return
			}
			bytes, err := brw.ReadBytes(0)
			if MayBeError(err) {
				return
			}
			subId := strings.Split(string(bytes[0:len(bytes)-1]), ",")[0]
			if v, ok := p.Subscribers[subId]; ok {
				if cmd != 1 {
					v.Cancel()
				}
			}
		default:
			log.Printf("unknown cmd:%v", cmd)
		}
	}
}
