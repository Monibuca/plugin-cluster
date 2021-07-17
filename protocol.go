package cluster

import (
	"bufio"
	"io"
	"sync"

	. "github.com/Monibuca/engine/v3"
	. "github.com/Monibuca/utils/v3"
)

type Cluster struct {
	*bufio.Writer
	*bufio.Reader
	sync.Mutex
	tracks     map[string]map[string]Track
	trackMutex sync.RWMutex
}

func (c *Cluster) ReadString() string {
	if c.Reader == nil {
		return ""
	}
	bytes, err := c.ReadBytes(0)
	if err != nil {
		return ""
	}
	bytes = bytes[0 : len(bytes)-1]
	return string(bytes)
}

func (c *Cluster) WriteSubscribe(streamPath string) {
	c.Lock()
	defer c.Unlock()
	c.WriteByte(MSG_SUBSCRIBE)
	c.WriteString(streamPath)
	c.Flush()
}

func (c *Cluster) WriteUnSubscribe(streamPath string) {
	c.Lock()
	defer c.Unlock()
	c.WriteByte(MSG_UNSUBSCRIBE)
	c.WriteString(streamPath)
	c.Flush()
}
func (c *Cluster) ReadVideoTrack() {
	if streamPath := c.ReadString(); streamPath != "" {
		if s := FindStream(streamPath); s != nil && s.Type == STREAMTYPE_ORIGIN {
			if name := c.ReadString(); name != "" {
				if codec, err := c.ReadByte(); err == nil {
					vt := s.NewVideoTrack(codec)
					c.trackMutex.Lock()
					if m, ok := c.tracks[streamPath]; ok {
						m[name] = vt
					} else {
						c.tracks[streamPath] = map[string]Track{name: vt}
					}
					c.trackMutex.Unlock()
					if len := c.ReadUint32(); len > 0 {
						payload := make([]byte, len)
						_, err = io.ReadFull(c, payload)
						vt.PushByteStream(0, payload)
					} else {
						s.VideoTracks.AddTrack(name, vt)
					}
				}
			}
		}
	}
}
func (c *Cluster) ReadAudioTrack() {
	if streamPath := c.ReadString(); streamPath != "" {
		if s := FindStream(streamPath); s != nil && s.Type == STREAMTYPE_ORIGIN {
			if name := c.ReadString(); name != "" {
				if codec, err := c.ReadByte(); err == nil {
					at := s.NewAudioTrack(codec)
					c.trackMutex.Lock()
					if m, ok := c.tracks[streamPath]; ok {
						m[name] = at
					} else {
						c.tracks[streamPath] = map[string]Track{name: at}
					}
					c.trackMutex.Unlock()
					if len := c.ReadUint32(); len > 0 {
						payload := make([]byte, len)
						_, err = io.ReadFull(c, payload)
						at.PushByteStream(0, payload)
					} else {
						s.AudioTracks.AddTrack(name, at)
					}
				}
			}
		}
	}
}
func (c *Cluster) ReadVideoPack() {
	c.trackMutex.RLock()
	defer c.trackMutex.RUnlock()
	if streamPath := c.ReadString(); streamPath != "" {
		m, ok := c.tracks[streamPath]
		if name := c.ReadString(); name != "" && ok {
			ts := c.ReadUint32()
			payload := make([]byte, c.ReadUint32())
			io.ReadFull(c, payload)
			m[name].(*VideoTrack).PushByteStream(ts, payload)
		}
	}
}
func (c *Cluster) ReadAudioPack() {
	c.trackMutex.RLock()
	defer c.trackMutex.RUnlock()
	if streamPath := c.ReadString(); streamPath != "" {
		m, ok := c.tracks[streamPath]
		if name := c.ReadString(); name != "" && ok {
			ts := c.ReadUint32()
			payload := make([]byte, c.ReadUint32())
			io.ReadFull(c, payload)
			m[name].(*AudioTrack).PushByteStream(ts, payload)
		}
	}
}
func (c *Cluster) orderReport(start bool) {
	c.Lock()
	defer c.Unlock()
	b := []byte{MSG_SUMMARY, 0}
	if start {
		b[1] = 1
	}
	c.Write(b)
}
func (c *Cluster) WriteUint32(num uint32) {
	c.Write(BigEndian.ToUint32(num))
}
func (c *Cluster) ReadUint32() uint32 {
	tmp := make([]byte, 4)
	c.Read(tmp)
	return BigEndian.Uint32(tmp)
}
func (c *Cluster) WriteString(s string) {
	c.Writer.WriteString(s)
	c.WriteByte(0)
}
func (c *Cluster) WriteVT(streamPath string, name string, vt *VideoTrack) {
	c.Lock()
	defer c.Unlock()
	c.WriteByte(MSG_VIDEOTRACK)
	c.WriteString(streamPath)
	c.WriteString(name)
	c.WriteByte(vt.CodecID)
	c.WriteUint32(uint32(len(vt.ExtraData.Payload)))
	c.Write(vt.ExtraData.Payload)
	c.Flush()
}
func (c *Cluster) WriteAT(streamPath string, name string, at *AudioTrack) {
	c.Lock()
	defer c.Unlock()
	c.WriteByte(MSG_AUDIOTRACK)
	c.WriteString(streamPath)
	c.WriteString(name)
	c.WriteByte(at.CodecID)
	c.WriteUint32(uint32(len(at.ExtraData)))
	c.Write(at.ExtraData)
	c.Flush()
}

func (c *Cluster) WritePublish(streamPath string) {
	c.Lock()
	defer c.Unlock()
	c.WriteByte(MSG_PUBLISH)
	c.WriteString(streamPath)
	c.Flush()
}
func (c *Cluster) WriteUnPublish(streamPath string) {
	c.Lock()
	defer c.Unlock()
	c.WriteByte(MSG_UNPUBLISH)
	c.WriteString(streamPath)
	c.Flush()
}

func (c *Cluster) WriteVideoPack(streamPath string, name string, pack VideoPack) {
	c.Lock()
	defer c.Unlock()
	c.WriteByte(MSG_VIDEO)
	c.WriteString(streamPath)
	c.WriteString(name)
	c.WriteUint32(pack.Timestamp)
	c.WriteUint32(uint32(len(pack.Payload)))
	c.Write(pack.Payload)
	c.Flush()
}

func (c *Cluster) WriteAudioPack(streamPath string, name string, pack AudioPack) {
	c.Lock()
	defer c.Unlock()
	c.WriteByte(MSG_AUDIO)
	c.WriteString(streamPath)
	c.WriteString(name)
	c.WriteUint32(pack.Timestamp)
	c.WriteUint32(uint32(len(pack.Payload)))
	c.Write(pack.Payload)
	c.Flush()
}
