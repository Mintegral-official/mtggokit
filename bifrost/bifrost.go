package bifrost

import (
	"errors"
	"github.com/lzexin/mtggokit/bifrost/container"
	"github.com/lzexin/mtggokit/bifrost/log"
	"github.com/lzexin/mtggokit/bifrost/streamer"
)

type Bifrost struct {
	DataStreamers map[string]streamer.Streamer
	logger        *log.BiLogger
}

func NewBifrost() *Bifrost {
	return &Bifrost{
		DataStreamers: make(map[string]streamer.Streamer),
	}
}

func (l *Bifrost) Get(name string, key container.MapKey) (interface{}, error) {
	s, ok := l.DataStreamers[name]
	if !ok {
		return nil, errors.New("not found streamer[" + name + "]")
	}
	c := s.GetContainer()
	if c == nil {
		return nil, errors.New("contain is nil, streamer[" + name + "]")
	}
	return c.Get(key)
}

func (l *Bifrost) Register(name string, streamer streamer.Streamer) error {
	if _, ok := l.DataStreamers[name]; ok {
		return errors.New("streamer[" + name + "] has already exist")
	}
	l.DataStreamers[name] = streamer
	return nil
}

func (l *Bifrost) GetStreamer(name string) (streamer.Streamer, error) {
	s, ok := l.DataStreamers[name]
	if !ok {
		return nil, errors.New("not found streamer[" + name + "]")
	}
	return s, nil
}
