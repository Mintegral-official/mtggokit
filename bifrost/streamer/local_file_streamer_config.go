package streamer

import "github.com/lzexin/mtggokit/bifrost/log"

type LocalFileStreamerCfg struct {
	Name         string
	Path         string
	UpdatMode    UpdatMode
	Interval     int
	IsSync       bool
	DataParser   DataParser
	UserData     interface{}
	Logger       log.BiLogger
	OnBeforeBase func(streamer Streamer) error
	OnFinishBase func(streamer Streamer)
}
