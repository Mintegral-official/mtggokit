package streamer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lzexin/mtggokit/bifrost/container"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"time"
)

type MongoStreamer struct {
	container    container.Container
	cfg          *MongoStreamerCfg
	hasInit      bool
	totalNum     int
	errorNum     int
	curParser    DataParser
	client       *mongo.Client
	collection   *mongo.Collection
	cursor       *mongo.Cursor
	result       []ParserResult
	curLen       int
	findOpt      *options.FindOptions
	lastBaseTime time.Time
	lastIncTime  time.Time
	baseTimeUsed time.Duration
	incTimeUsed  time.Duration
}

func NewMongoStreamer(mongoConfig *MongoStreamerCfg) (*MongoStreamer, error) {
	streamer := &MongoStreamer{
		cfg: mongoConfig,
	}

	ctx, _ := context.WithTimeout(context.TODO(), time.Duration(mongoConfig.ConnectTimeout)*time.Microsecond)
	opt := options.Client().ApplyURI(mongoConfig.URI)
	opt.SetReadPreference(readpref.SecondaryPreferred())
	direct := true
	opt.Direct = &direct
	client, err := mongo.Connect(ctx, opt)
	if err != nil {
		if mongoConfig.Logger != nil {
			mongoConfig.Logger.Warnf("mongo connect error, err=[%s]", err.Error())
		}
		return nil, err
	}
	streamer.client = client

	streamer.findOpt = options.MergeFindOptions(mongoConfig.FindOpt)
	d := time.Duration(mongoConfig.ReadTimeout) * time.Microsecond
	streamer.findOpt.MaxTime = &d

	if err = client.Ping(ctx, readpref.Primary()); err != nil {
		if mongoConfig.Logger != nil {
			mongoConfig.Logger.Warnf("mongo ping error, err=[%s]", err.Error())
		}
		return nil, err
	}

	streamer.collection = client.Database(mongoConfig.DB).Collection(mongoConfig.Collection)
	if streamer.collection == nil {
		if mongoConfig.Logger != nil {
			mongoConfig.Logger.Warnf("[%s.%s] Not found", mongoConfig.DB, mongoConfig.Collection)
		}
		return nil, errors.New(fmt.Sprintf("[%s.%s] Not found", mongoConfig.DB, mongoConfig.Collection))
	}

	return streamer, nil
}

func (ms *MongoStreamer) SetContainer(container container.Container) {
	ms.container = container
}

func (ms *MongoStreamer) GetContainer() container.Container {
	return ms.container
}

func (ms *MongoStreamer) GetSchedInfo() *SchedInfo {
	return &SchedInfo{
		TimeInterval: ms.cfg.IncInterval,
	}
}

func (ms *MongoStreamer) HasNext() (bool, error) {
	if ms.curLen < len(ms.result) {
		return true, nil
	}
	if ms.cursor.Next(context.Background()) {
		return true, nil
	} else {
		return false, ms.cursor.Err()
	}
}

func (ms *MongoStreamer) Next() (container.DataMode, container.MapKey, interface{}, error) {
	ms.totalNum++
	if ms.curLen < len(ms.result) {
		r := ms.result[ms.curLen]
		ms.curLen++
		if r.Err != nil {
			ms.errorNum++
		}
		return r.DataMode, r.Key, r.Value, r.Err
	}
	if ms.cursor == nil {
		ms.errorNum++
		return container.DataModeAdd, nil, nil, errors.New("cursor is nil")
	}
	if ms.cursor.Err() != nil {
		ms.WarnStatus(fmt.Sprintf("cursor is error[%s]", ms.cursor.Err().Error()))
		ms.errorNum++
		return container.DataModeAdd, nil, nil, errors.New(fmt.Sprintf("cursor is error[%s]", ms.cursor.Err().Error()))
	}
	result := ms.curParser.Parse(ms.cursor.Current, ms.cfg.UserData)
	if result == nil {
		ms.errorNum++
		return container.DataModeAdd, nil, nil, errors.New("Parse error")
	}
	ms.curLen = 0
	ms.result = result
	if ms.curLen < len(ms.result) {
		r := ms.result[ms.curLen]
		ms.curLen++
		if r.Err != nil {
			ms.errorNum++
		}
		return r.DataMode, r.Key, r.Value, r.Err
	}
	ms.errorNum++
	return container.DataModeAdd, nil, nil, errors.New(fmt.Sprintf("Index[%d] error, len[%d]", ms.curLen, len(ms.result)))
}

func (ms *MongoStreamer) UpdateData(ctx context.Context) error {
	ms.lastBaseTime = time.Now()
	if !ms.hasInit && ms.cfg.IsSync {
		err := ms.loadBase(ctx)
		if err != nil {
			ms.WarnStatus("LoadBase error:" + err.Error())
		} else {
			ms.InfoStatus("LoadBase Succ")
			ms.hasInit = true
		}
	}
	go func() {
		ms.lastBaseTime = time.Now()
		if !ms.hasInit {
			err := ms.loadBase(ctx)
			if err != nil {
				ms.WarnStatus("LoadBase error:" + err.Error())
			} else {
				ms.InfoStatus("LoadBase succ")
			}
		}
		inc := time.After(time.Duration(ms.cfg.IncInterval) * time.Second)
		base := time.After(time.Duration(ms.cfg.BaseInterval) * time.Second)
		if ms.cfg.BaseInterval == 0 {
			base = nil
		}
		for {
			select {
			case <-ctx.Done():
				ms.InfoStatus("LoadInc Finish:")
				return
			case <-inc:
				ms.lastIncTime = time.Now()
				err := ms.loadInc(ctx)
				if err != nil {
					ms.WarnStatus("LoadInc Error:" + err.Error())
				} else {
					ms.InfoStatus("LoadInc Succ:")
				}
				inc = time.After(time.Duration(ms.cfg.IncInterval) * time.Second)
			case <-base:
				ms.lastBaseTime = time.Now()
				err := ms.loadBase(ctx)
				if err != nil {
					ms.WarnStatus("LoadBase Error:" + err.Error())
				} else {
					ms.InfoStatus("LoadBase Succ:")
				}
				base = time.After(time.Duration(ms.cfg.BaseInterval) * time.Second)
			}
		}
	}()
	return nil
}

func (ms *MongoStreamer) loadBase(ctx context.Context) (err error) {
	for i := -1; i < ms.cfg.TryTimes; i++ {
		err = ms.loadBase2(ctx)
		if err == nil {
			return nil
		} else {
			ms.cfg.Logger.Warnf("LoadBase error[%s], tryTimes[%d]", err, i+1)
		}
	}
	return
}

func (ms *MongoStreamer) loadBase2(context.Context) error {

	if ms.cfg.OnBeforeBase != nil {
		ms.cfg.BaseQuery = ms.cfg.OnBeforeBase(ms.cfg.UserData)
		if ms.cfg.BaseQuery == nil {
			return nil
		}
	}
	ms.totalNum = 0
	ms.errorNum = 0
	cur, err := ms.collection.Find(nil, ms.cfg.BaseQuery, ms.findOpt)
	if err != nil {
		return errors.New("FindError, " + err.Error())
	}

	if ms.cursor != nil {
		_ = ms.cursor.Close(nil)
	}
	ms.cursor = cur
	ms.curParser = ms.cfg.BaseParser
	err = ms.container.LoadBase(ms)
	ms.baseTimeUsed = time.Now().Sub(ms.lastBaseTime)
	if ms.cfg.OnFinishBase != nil {
		ms.cfg.OnFinishBase(ms)
	}
	return err
}

func (ms *MongoStreamer) loadInc(ctx context.Context) error {
	if ms.cfg.OnBeforeInc != nil {
		ms.cfg.IncQuery = ms.cfg.OnBeforeInc(ms.cfg.UserData)
		if ms.cfg.IncQuery == nil {
			return nil
		}
	}
	c, _ := context.WithTimeout(ctx, time.Duration(ms.cfg.ReadTimeout)*time.Microsecond)
	cur, err := ms.collection.Find(nil, ms.cfg.IncQuery, ms.cfg.FindOpt)
	if err != nil {
		return errors.New("FindError: " + err.Error())
	}
	if ms.cursor != nil {
		_ = ms.cursor.Close(c)
	}
	ms.cursor = cur
	ms.curParser = ms.cfg.IncParser
	err = ms.container.LoadInc(ms)
	ms.incTimeUsed = time.Now().Sub(ms.lastIncTime)
	if ms.cfg.OnFinishInc != nil {
		ms.cfg.OnFinishInc(ms)
	}
	return err
}

func (ms *MongoStreamer) GetInfo() *Info {
	return &Info{
		Name:         ms.cfg.Name,
		TotalNum:     ms.container.Len(),
		AddNum:       ms.totalNum,
		ErrorNum:     ms.errorNum,
		LastBaseTime: ms.lastBaseTime,
		LastIncTime:  ms.lastIncTime,
		BaseTimeUsed: ms.baseTimeUsed,
		IncTimeUsed:  ms.incTimeUsed,
	}
}

func (ms *MongoStreamer) InfoStatus(s string) {
	if ms.cfg.Logger != nil {
		ms.cfg.Logger.Infof("%s, streamInfo[%d]", s, ms.getInfoStr())
	}
}

func (ms *MongoStreamer) WarnStatus(s string) {
	if ms.cfg.Logger != nil {
		ms.cfg.Logger.Warnf("%s, streamInfo[%d]", s, ms.getInfoStr())
	}
}

func (ms *MongoStreamer) getInfoStr() string {
	data, _ := json.Marshal(ms.GetInfo())
	return string(data)
}
