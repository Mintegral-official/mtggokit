package streamer

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Mintegral-official/mtggokit/bifrost/container"
	"os"
	"time"
)

type LocalFileStreamer struct {
	container    container.Container
	cfg          *LocalFileStreamerCfg
	scan         *bufio.Scanner
	result       []ParserResult
	curLen       int
	hasInit      bool
	modTime      time.Time
	addNum       int
	errorNum     int
	lastBaseTime time.Time
	baseTimeUsed time.Duration
}

func NewFileStreamer(cfg *LocalFileStreamerCfg) *LocalFileStreamer {
	fs := &LocalFileStreamer{
		cfg: cfg,
	}
	return fs
}

func (fs *LocalFileStreamer) SetContainer(container container.Container) {
	fs.container = container
}

func (fs *LocalFileStreamer) GetContainer() container.Container {
	return fs.container
}

func (fs *LocalFileStreamer) GetSchedInfo() *SchedInfo {
	return &SchedInfo{
		TimeInterval: fs.cfg.Interval,
	}
}

func (fs *LocalFileStreamer) HasNext() (bool, error) {
	return fs.curLen < len(fs.result) || fs.scan != nil && fs.scan.Scan(), nil
}

func (fs *LocalFileStreamer) Next() (container.DataMode, container.MapKey, interface{}, error) {
	fs.addNum++
	if fs.curLen < len(fs.result) {
		r := fs.result[fs.curLen]
		fs.curLen++
		if r.Err != nil {
			fs.errorNum++
		}
		return r.DataMode, r.Key, r.Value, r.Err
	}
	result := fs.cfg.DataParser.Parse([]byte(fs.scan.Text()), nil)
	if result == nil {
		fs.errorNum++
		return container.DataModeAdd, nil, nil, errors.New(fmt.Sprintf("Parser error"))
	}
	fs.curLen = 0
	fs.result = result
	if fs.curLen < len(fs.result) {
		r := fs.result[fs.curLen]
		fs.curLen++
		if r.Err != nil {
			fs.errorNum++
		}
		return r.DataMode, r.Key, r.Value, r.Err
	}
	fs.errorNum++
	return container.DataModeAdd, nil, nil, errors.New(fmt.Sprintf("Index[%d] error, len[%d]", fs.curLen, len(fs.result)))
}

func (fs *LocalFileStreamer) UpdateData(ctx context.Context) error {
	if fs.cfg.IsSync {
		fs.lastBaseTime = time.Now()
		err := fs.updateData(ctx)
		fs.baseTimeUsed = time.Now().Sub(fs.lastBaseTime)
		if err != nil {
			fs.WarnStatus("LoadBase error: " + err.Error())
			return err
		}
		fs.InfoStatus("LoadBase succ")
	}
	go func() {
		for {
			inc := time.After(time.Duration(fs.cfg.Interval) * time.Second)
			select {
			case <-ctx.Done():
				return
			case <-inc:
				fs.lastBaseTime = time.Now()
				err := fs.updateData(ctx)
				fs.baseTimeUsed = time.Now().Sub(fs.lastBaseTime)
				if err != nil {
					fs.WarnStatus("LoadBase error: " + err.Error())
				} else {
					fs.InfoStatus("LoadBase succ")
				}
			}
		}
	}()
	return nil
}

func (fs *LocalFileStreamer) updateData(ctx context.Context) error {
	switch fs.cfg.UpdatMode {
	case Static, Dynamic:
		fs.addNum = 0
		fs.errorNum = 0
		if fs.hasInit && fs.cfg.UpdatMode == Static {
			return nil
		}
		f, err := os.Open(fs.cfg.Path)
		defer func() { _ = f.Close() }()
		if err != nil {
			return err
		}
		stat, _ := f.Stat()
		modTime := stat.ModTime()
		if modTime.After(fs.modTime) {
			fs.modTime = modTime
			fs.scan = bufio.NewScanner(f)
			err = fs.container.LoadBase(fs)
			if fs.cfg.OnFinishBase != nil {
				fs.cfg.OnFinishBase(fs)
			}
			return err
		}
	case Increment:
	case DynInc:
	default:
		return errors.New("not support mode[" + fs.cfg.UpdatMode.toString() + "]")
	}
	return nil
}

func (fs *LocalFileStreamer) InfoStatus(s string) {
	if fs.cfg.Logger != nil {
		fs.cfg.Logger.Infof("%s, streamerInfo[%s]", s, fs.getInfoStr())
	}
}

func (fs *LocalFileStreamer) WarnStatus(s string) {
	if fs.cfg.Logger != nil {
		fs.cfg.Logger.Warnf("%s, streamerInfo[%s]", s, fs.getInfoStr())
	}
}

func (fs *LocalFileStreamer) GetInfo() *Info {
	return &Info{
		Name:         fs.cfg.Name,
		TotalNum:     fs.container.Len(),
		AddNum:       fs.addNum,
		ErrorNum:     fs.errorNum,
		LastBaseTime: fs.lastBaseTime,
		BaseTimeUsed: fs.baseTimeUsed,
	}
}

func (fs *LocalFileStreamer) getInfoStr() string {
	data, _ := json.Marshal(fs.GetInfo())
	return string(data)
}
