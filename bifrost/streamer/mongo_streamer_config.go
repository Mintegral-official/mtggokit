package streamer

import (
	"github.com/lzexin/mtggokit/bifrost/log"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoStreamerCfg struct {
	Name           string
	UpdateMode     UpdatMode
	IncInterval    int
	BaseInterval   int
	IsSync         bool
	TryTimes       int
	URI            string
	DB             string
	Collection     string
	ConnectTimeout int
	ReadTimeout    int
	BaseParser     DataParser
	IncParser      DataParser
	BaseQuery      interface{}
	IncQuery       interface{}
	UserData       interface{}
	FindOpt        *options.FindOptions
	OnBeforeBase   func(interface{}) interface{}
	OnBeforeInc    func(interface{}) interface{}
	OnFinishBase   func(streamer Streamer)
	OnFinishInc    func(streamer Streamer)
	Logger         log.BiLogger
}
