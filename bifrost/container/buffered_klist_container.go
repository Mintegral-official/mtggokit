package container

import (
	"errors"
	"fmt"
)

// 双bufMap, 仅提供Get/LoadBase接口
type BufferedKListContainer struct {
	innerData *map[interface{}][]interface{}
	ErrorNum  int
}

func (bm *BufferedKListContainer) Get(key MapKey) (interface{}, error) {
	data, in := (*bm.innerData)[key.Value()]
	if !in {
		return nil, NotExistErr
	}
	return data, nil
}

func (bm *BufferedKListContainer) LoadBase(iterator DataIterator) error {
	bm.ErrorNum = 0
	tmpM := make(map[interface{}][]interface{})
	b, e := iterator.HasNext()
	if e != nil {
		return fmt.Errorf("LoadBase Error, err[%s]", e.Error())
	}
	for b {
		_, k, v, e := iterator.Next()
		if e != nil {
			bm.ErrorNum++
			b, e = iterator.HasNext()
			if e != nil {
				return fmt.Errorf("LoadBase Error, err[%s]", e.Error())
			}
			continue
		}
		res, in := tmpM[k.Value()]
		if in {
			res = append(res, v)
			tmpM[k.Value()] = res
		} else {
			tmpM[k.Value()] = []interface{}{v}
		}
		b, e = iterator.HasNext()
		if e != nil {
			return fmt.Errorf("LoadBase Error, err[%s]", e.Error())
		}
	}
	bm.innerData = &tmpM
	return nil
}

func (bm *BufferedKListContainer) Set(key MapKey, value interface{}) error {
	return errors.New("not implement")
}

func (bm *BufferedKListContainer) Del(key MapKey, value interface{}) {
}

func (bm *BufferedKListContainer) LoadInc(iterator DataIterator) error {
	return errors.New("not implement")
}
