package go_delay_driver_redis

import (
	"github.com/go-redis/redis"
	"log"
	"time"
)

type Dredis struct {
	conn *redis.Client
}

type OptFn func(cfg *redis.Options)

func NewDredis(addr, pwd string, opt ...OptFn) (dr *Dredis, err error)  {
	cfg := &redis.Options{
		Addr: addr,
		Password: pwd,
		DB: 0,
		DialTimeout:  1 * time.Second,
		ReadTimeout:  1 * time.Second,
		WriteTimeout: 1 * time.Second,
		PoolSize:     15,
		PoolTimeout:  3 * time.Minute,
	}
	if len(opt) > 0 {
		for _,fn := range opt {
			fn(cfg)
		}
	}
	dr = &Dredis{
		conn: redis.NewClient(cfg),
	}
	if _, err := dr.conn.Ping().Result();err != nil {
		return nil, err
	}
	return
}

func (dr *Dredis) SubscribeMsg(topic string, dealFn func([]byte)(err error)) (err error){
	go func() {
		var tk = time.NewTicker(5 * time.Second)
		for ;; {
			// 获取队列消息
			if msg,err := dr.conn.RPop(topic).Bytes(); err == nil {
				err = dealFn(msg)
				if err != nil {
					// 程序退出，消息重新放回
					err = dr.conn.RPush(topic, msg).Err()
					if err != nil {
						// log
						log.Print("消息重新放回失败：", err)
					}
				}
			} else {
				if err != redis.Nil {
					// log
					log.Print("消息出队失败：", err)
				}
				<-tk.C
			}
		}
	}()
	return 
}

func  (dr *Dredis) PublishMsg(topic string, msg []byte) (err error){
	err = dr.conn.LPush(topic, msg).Err()
	if err != nil {
		// log
		log.Print("消息入队失败：", err)
	}
	return 
}