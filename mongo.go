package mongo

import (
	"context"
	"sync"
	"time"

	"github.com/globalsign/mgo"
	"github.com/globalsign/mgo/bson"
	"github.com/go-session/session"
	"github.com/json-iterator/go"
)

var (
	_             session.ManagerStore = &managerStore{}
	_             session.Store        = &store{}
	jsonMarshal                        = jsoniter.Marshal
	jsonUnmarshal                      = jsoniter.Unmarshal
)

// NewStore Create an instance of a mongo store
func NewStore(url, dbName, cName string) session.ManagerStore {
	session, err := mgo.Dial(url)
	if err != nil {
		panic(err)
	}
	return newManagerStore(session, dbName, cName)
}

// NewStoreWithSession Create an instance of a mongo store
func NewStoreWithSession(session *mgo.Session, dbName, cName string) session.ManagerStore {
	return newManagerStore(session, dbName, cName)
}

func newManagerStore(session *mgo.Session, dbName, cName string) *managerStore {
	err := session.DB(dbName).C(cName).EnsureIndex(mgo.Index{
		Key:         []string{"expired_at"},
		ExpireAfter: time.Second,
	})
	if err != nil {
		panic(err)
	}

	return &managerStore{
		session: session,
		dbName:  dbName,
		cName:   cName,
		pool: sync.Pool{
			New: func() interface{} {
				return newStore(session, dbName, cName)
			},
		},
	}
}

type managerStore struct {
	pool    sync.Pool
	session *mgo.Session
	dbName  string
	cName   string
}

func (s *managerStore) getValue(sid string) (string, error) {
	session := s.session.Clone()
	defer session.Close()

	var item sessionItem
	err := session.DB(s.dbName).C(s.cName).FindId(sid).One(&item)
	if err != nil {
		if err == mgo.ErrNotFound {
			return "", nil
		}
		return "", err
	} else if item.ExpiredAt.Before(time.Now()) {
		return "", nil
	}
	return item.Value, nil
}

func (s *managerStore) parseValue(value string) (map[string]interface{}, error) {
	var values map[string]interface{}
	if len(value) > 0 {
		err := jsonUnmarshal([]byte(value), &values)
		if err != nil {
			return nil, err
		}
	}

	return values, nil
}

func (s *managerStore) Check(_ context.Context, sid string) (bool, error) {
	val, err := s.getValue(sid)
	if err != nil {
		return false, err
	}
	return val != "", nil
}

func (s *managerStore) Create(ctx context.Context, sid string, expired int64) (session.Store, error) {
	store := s.pool.Get().(*store)
	store.reset(ctx, sid, expired, nil)
	return store, nil
}

func (s *managerStore) Update(ctx context.Context, sid string, expired int64) (session.Store, error) {
	store := s.pool.Get().(*store)

	value, err := s.getValue(sid)
	if err != nil {
		return nil, err
	} else if value == "" {
		store.reset(ctx, sid, expired, nil)
		return store, nil
	}

	session := s.session.Clone()
	defer session.Close()
	err = session.DB(s.dbName).C(s.cName).UpdateId(sid, bson.M{
		"$set": bson.M{
			"expired_at": time.Now().Add(time.Duration(expired) * time.Second),
		},
	})
	if err != nil {
		return nil, err
	}

	values, err := s.parseValue(value)
	if err != nil {
		return nil, err
	}

	store.reset(ctx, sid, expired, values)
	return store, nil
}

func (s *managerStore) Delete(_ context.Context, sid string) error {
	session := s.session.Clone()
	defer session.Close()
	return session.DB(s.dbName).C(s.cName).RemoveId(sid)
}

func (s *managerStore) Refresh(ctx context.Context, oldsid, sid string, expired int64) (session.Store, error) {
	store := s.pool.Get().(*store)

	value, err := s.getValue(oldsid)
	if err != nil {
		return nil, err
	} else if value == "" {
		store.reset(ctx, sid, expired, nil)
		return store, nil
	}

	session := s.session.Clone()
	defer session.Close()
	c := session.DB(s.dbName).C(s.cName)
	_, err = c.UpsertId(sid, &sessionItem{
		ID:        sid,
		Value:     value,
		ExpiredAt: time.Now().Add(time.Duration(expired) * time.Second),
	})
	if err != nil {
		return nil, err
	}
	err = c.RemoveId(oldsid)
	if err != nil {
		return nil, err
	}

	values, err := s.parseValue(value)
	if err != nil {
		return nil, err
	}

	store.reset(ctx, sid, expired, values)
	return store, nil
}

func (s *managerStore) Close() error {
	s.session.Close()
	return nil
}

func newStore(session *mgo.Session, dbName, cName string) *store {
	return &store{
		session: session,
		dbName:  dbName,
		cName:   cName,
	}
}

type store struct {
	sync.RWMutex
	ctx     context.Context
	session *mgo.Session
	dbName  string
	cName   string
	sid     string
	expired int64
	values  map[string]interface{}
}

func (s *store) reset(ctx context.Context, sid string, expired int64, values map[string]interface{}) {
	if values == nil {
		values = make(map[string]interface{})
	}
	s.ctx = ctx
	s.sid = sid
	s.expired = expired
	s.values = values
}

func (s *store) Context() context.Context {
	return s.ctx
}

func (s *store) SessionID() string {
	return s.sid
}

func (s *store) Set(key string, value interface{}) {
	s.Lock()
	s.values[key] = value
	s.Unlock()
}

func (s *store) Get(key string) (interface{}, bool) {
	s.RLock()
	val, ok := s.values[key]
	s.RUnlock()
	return val, ok
}

func (s *store) Delete(key string) interface{} {
	s.RLock()
	v, ok := s.values[key]
	s.RUnlock()
	if ok {
		s.Lock()
		delete(s.values, key)
		s.Unlock()
	}
	return v
}

func (s *store) Flush() error {
	s.Lock()
	s.values = make(map[string]interface{})
	s.Unlock()
	return s.Save()
}

func (s *store) Save() error {
	var value string

	s.RLock()
	if len(s.values) > 0 {
		buf, err := jsonMarshal(s.values)
		if err != nil {
			s.RUnlock()
			return err
		}
		value = string(buf)
	}
	s.RUnlock()

	session := s.session.Clone()
	defer session.Close()
	_, err := session.DB(s.dbName).C(s.cName).UpsertId(s.sid, &sessionItem{
		ID:        s.sid,
		Value:     value,
		ExpiredAt: time.Now().Add(time.Duration(s.expired) * time.Second),
	})

	return err
}

// Data items stored in mongo
type sessionItem struct {
	ID        string    `bson:"_id"`
	Value     string    `bson:"value"`
	ExpiredAt time.Time `bson:"expired_at"`
}
