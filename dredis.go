package go_delay_driver_redis

import (
	"github.com/go-redis/redis"
	"time"
)

type Dredis struct {
	conn *redis.Client
}

func NewDredis(opt *redis.Options) (dr *Dredis, err error)  {
	dr = &Dredis{
		conn: redis.NewClient(opt),
	}
	if _, err := dr.conn.Ping().Result();err != nil {
		return nil, err
	}
	return
}

func (dr *Dredis) SubscribeMsg(topic string, dealFn func([]byte)(err error)) (err error){
	go func() {
		var tk = time.NewTicker(1 * time.Second)
		for ;; {
			if msg,err := dr.conn.RPop(topic).Bytes(); err == nil {
				err = dealFn(msg)
				if err != nil {
					dr.conn.RPush(topic, msg)
				}
			} else {
				<-tk.C
			}
		}
	}()
	return 
}

func  (dr *Dredis) PublishMsg(topic string, msg []byte) (err error){
	dr.conn.LPush(topic, msg)
	return 
}