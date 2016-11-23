/* Description
 * traffic controller 用于对无router/proxy的分布式访问traffic的控制。
 * 实现了qps实时统计，限流。
 *
 */


package trafficcontroller


import (
	"fmt"
	"errors"
	"strings"
	_ "strconv"
	"time"
	"sync"
	"sync/atomic"
	"poiutils/redis"
)


type TrafficCounter struct {
	AppliedQps   int64    // 申请的配额
	Bucket       int64    // 限流周期内的令牌桶
	Toplimit     int64    // 业务限流启动周期内的流量上限，防止出现瞬时流量过大而没有限流，设置为AppliedQps * 1.5
	LastTotalCnt int64    // 保存上一次从codis中读到的总数。如果为0，则跳过本轮check
	CurrentCnt   *int64   // 当前周期内的qps
	TrafficLimit bool     // 是否开启流量限制
}

type TrafficController struct {
	TrafficCtrl  map[string] *TrafficCounter   // the key is $service_$productid_$acckey
	Period       int64                         // the period of write counter to redis and to dicide traffic limit, ms
	Redis        *redis.RedisPool              // redis key is POI_$service_$productid_$acckey
	lock         *sync.RWMutex
}

type RedisCfg struct {
	Hosts     []string
	MaxActive int
	MaxIdle   int
	IdleTmout int
}

const (
	DEBUG = false
	EXPIRETIME = 3600 * 48
)

func Debug(v ...interface{}) {
	if DEBUG {
		str := fmt.Sprint(v...)
		fmt.Println(str)
	}
}

// 创建TrafficController对象
// 内部调用run函数做定时写redis并检查是否要对某个调用者限流
// input:
//		redisCfg *RedisCfg  -redis相关的配置
//      period   int64      -限流启动及关闭的检测周期, ms
//
// return:
//		*TrafficController
func CreateTrafficController(redisCfg *RedisCfg, period int64) *TrafficController {
	trafficCtrl := new(TrafficController)
	trafficCtrl.Redis = new(redis.RedisPool)
	trafficCtrl.Redis.AddPools(redisCfg.Hosts, int(redisCfg.MaxActive), int(redisCfg.MaxIdle), int(redisCfg.IdleTmout))
	trafficCtrl.TrafficCtrl = make(map[string] *TrafficCounter)
	trafficCtrl.lock = new(sync.RWMutex)
	if period > 0 {
		trafficCtrl.Period = period
	} else {
		trafficCtrl.Period = 1000
	}

	go run(trafficCtrl)
	return trafficCtrl
}

// 注册访问者(service+productid+acckey)
// input:
//		service    string
// 		productid  string
// 		acckey     string
//		appliedqps int64	-申请的qps值
//		bucket     int64	-启动限流状态下，单机支持的qps值，计算方法是: appliedqps / 集群机器数
// return:
//		error -- nil means OK
func (trafficCtrl *TrafficController) RegisterCaller(service, productid, acckey string, appliedqps, bucket int) error {
	var err error
	if service == "" || productid == "" || acckey == "" || appliedqps < 0 || bucket < 0 {
		err = errors.New(fmt.Sprint("there is input error"))
		return err
	}

	aqps := int64(appliedqps) * trafficCtrl.Period / 1000
	if appliedqps > 0 {
		if aqps < 1 {
			aqps = 1
		}
	} else {
		aqps = 0
	}

	bt := int64(bucket) * trafficCtrl.Period / 1000
	if bucket > 0 {
		if bt < 1 {
			bt = 1
		}
	} else {
		bt = 0
	}

	key := GenerateKey(service, productid, acckey)
	fc := new(TrafficCounter)
	fc.CurrentCnt = new(int64)
	fc.AppliedQps = aqps
	fc.Bucket = bt
	fc.Toplimit = int64(float64(aqps) * 1.5)
	trafficCtrl.TrafficCtrl[key] = fc
	return err
}

// 判断来访(service+productid+acckey)的请求是否需要traffic controll
// input:
//		service    string
// 		productid  string
// 		acckey     string
// return:
//		true  -traffic controll，该请求直接丢弃
//		false -为请求提供服务
func (trafficCtrl *TrafficController) IsTrafficCtrl(service, productid, acckey string) (bool, int) {
	Debug("IsTrafficCtrl, servicename = ", service, ", productid = ", productid, ", acckey = ", acckey)
	var limitFlag bool = false
	var ctrlCode int = 0
	key := GenerateKey(service, productid, acckey)

	trafficCtrl.lock.RLock()
	defer trafficCtrl.lock.RUnlock()
	if ctrl, ok := trafficCtrl.TrafficCtrl[key]; ok {
		if (ctrl.TrafficLimit && *ctrl.CurrentCnt > ctrl.Bucket) || (*ctrl.CurrentCnt > ctrl.Toplimit) {
			ctrlCode = 1
			limitFlag = true
		}
	} else {
		// if "key" is not registered, the request may be illegal, just drop it return
		ctrlCode = 2
		return true, ctrlCode
	}

	trafficCtrl.AddCount(key)

	return limitFlag, ctrlCode
}

func (trafficCtrl *TrafficController) AddCount(key string) (err error) {
	if ctrl, ok := trafficCtrl.TrafficCtrl[key]; ok {
		atomic.AddInt64(ctrl.CurrentCnt, 1)
		return
	}
	err = errors.New(fmt.Sprintf("No value found for key %s", key))
	return
}

// 自循环函数，执行周期性的写redis，检查是否需要限流
func run(trafficCtrl *TrafficController) {
	//timerCheck := time.NewTicker(time.Second * 5)
	timerSet   := time.NewTicker(time.Millisecond * time.Duration(trafficCtrl.Period))

	go trafficCtrl.writeCount()

	for {
		select {
		case <-timerSet.C:
			go trafficCtrl.writeCount()
		}
	}
}

func (trafficCtrl *TrafficController) writeCount() {
	respone := make(map[string]int64)

	day := time.Now().Day()
	hour := time.Now().Hour()

	var expire int64 = int64(EXPIRETIME)

	if DEBUG {
		hour = time.Now().Minute()
		expire = 900
	}

	// copy the value quickly before the long time job
	for k, v := range trafficCtrl.TrafficCtrl {
		count := atomic.LoadInt64(v.CurrentCnt)
		atomic.StoreInt64(v.CurrentCnt, 0)
		rediskey := fmt.Sprintf("POI_%s_%d_%d", k, day, hour)
		Debug("redis key： ", rediskey)
		if resp, err := trafficCtrl.Redis.IncrBy(rediskey, count); err != nil {
			continue
		} else {
			respone[k] = resp
			trafficCtrl.Redis.Expire(rediskey, expire)
		}
	}

	trafficCtrl.setTrafficLimit(respone)
}

func (trafficCtrl *TrafficController) setTrafficLimit(resp map[string]int64) {
	limit := make(map[string]bool)
	for k, v := range resp {
		if ctrl, ok := trafficCtrl.TrafficCtrl[k]; ok {
			increment := resp[k] - ctrl.LastTotalCnt;
			if increment > ctrl.AppliedQps {
				limit[k] = true
			} else {
				limit[k] = false
			}
			ctrl.LastTotalCnt = v
		}
	}
	// 加读写锁来保护数据
	trafficCtrl.lock.Lock()
	defer trafficCtrl.lock.Unlock()
	for k, v := range limit {
		trafficCtrl.TrafficCtrl[k].TrafficLimit = v
	}
}

func GenerateKey(service, productid, acckey string) string {
	key := fmt.Sprintf("%s_%s_%s", strings.ToUpper(service), productid, acckey)
	return key
}

