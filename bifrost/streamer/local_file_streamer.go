package streamer

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/lzexin/mtggokit/bifrost/container"
	"os"
	"time"
)

type LocalFileStreamer struct {
	container    container.Container
	cfg          *LocalFileStreamerCfg
	fileReader   *bufio.Reader
	line         []byte
	eof          bool
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
	if fs.curLen < len(fs.result) {
		return true, nil
	}
	line, err := fs.readLn(fs.fileReader)
	for err == nil && len(line) == 0 {
		line, err = fs.readLn(fs.fileReader)
	}

	if err == nil {
		fs.line = line
		return true, nil
	}

	if fs.isEof(err) {
		fs.eof = true
		return false, nil
	}

	return false, err
}

func (fs *LocalFileStreamer) readLn(r *bufio.Reader) ([]byte, error) {
	var (
		isPrefix       = true
		err      error = nil
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
	}
	return ln, err
}

func (fs *LocalFileStreamer) isEof(err error) bool {
	if err != nil {
		return err.Error() == "EOF"
	}
	return false
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
	result := fs.cfg.DataParser.Parse(fs.line, nil)
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
			fs.fileReader = bufio.NewReader(f)
			if fs.cfg.OnBeforeBase != nil {
				err := fs.cfg.OnBeforeBase(fs)
				if err != nil {
					return fmt.Errorf("OnBeforeBase Error: " + err.Error())
				}
			}
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
