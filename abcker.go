package rdsworker

import (
    "errors"
    "github.com/go-redis/redis/v7"
    "github.com/zhaojunlike/logger"
    "strconv"
    "sync"
    "time"
    "zhaojunlike/cacher"
    "zhaojunlike/common"
)

var (
    errorGetCookies = errors.New("获取可用cookies失败")
    CkNike          = "cn_nike"
    CkS3Nike        = "s3_nike"
    CkWebFootLocker = "footlocker"
)

type AbCkCache struct {
    cache          *cacher.GtCache
    Channel        chan Task
    LogFun         LogInfo
    Name           string
    queueKey       string
    dataMapKey     string
    isStart        bool
    clearSpeed     int64
    clearTimer     time.Duration
    getLocker      *sync.Mutex
    expPreTime     int64
    cookieLifeTime int64 //cookie的生存时间
}

func init() {

}

func NewAbCkCache(name string, ch *cacher.GtCache) *AbCkCache {
    var worker = &AbCkCache{}
    worker.Channel = make(chan Task, 100)
    worker.cache = ch
    worker.Name = name
    worker.dataMapKey = name + ":abck_mapper"
    worker.queueKey = name + ":abck_queue"
    worker.clearSpeed = 1000 //每次清理1000条过期数据
    worker.clearTimer = time.Second * 10
    worker.getLocker = new(sync.Mutex)
    worker.expPreTime = int64(20000)
    worker.cookieLifeTime = 1000 * 60 * 60 * 24 * 7 //默认有效时间7个小时
    worker.start()                                  //默認自動開啓
    return worker
}

//启动
func (tk *AbCkCache) start() {
    if tk.isStart {
        return
    }
    tk.isStart = true
    go tk.monitor()
}

func (tk *AbCkCache) Clear() {

}

//监控和清理过期的Cookies
func (tk *AbCkCache) monitor() {
    for {
        time.Sleep(tk.clearTimer) //5ms 检测一次库
        var opt = &redis.ZRangeBy{}
        var expTime = common.CreateTimestamp() + tk.expPreTime //系統也要檢測提前20s就給cookies標記成expire
        opt.Count = tk.clearSpeed
        opt.Min = "0"
        opt.Max = strconv.FormatInt(expTime, 10)
        ls, err := tk.cache.Client.ZRangeByScoreWithScores(tk.queueKey, opt).Result()
        if err != nil {
            logger.Error("redis获取数据失败:ZRangeByScoreWithScores:%v", err)
            continue
        }
        if len(ls) <= 0 {
            continue
        }
        //包含了分数和key的val
        var keys = make([]interface{}, len(ls)/2)
        for _, v := range ls {
            keys = append(keys, v.Member)
        }
        le, err := tk.cache.Client.ZRem(tk.queueKey, keys...).Result()
        if tk.LogFun != nil {
            tk.LogFun(le, err)
        }
    }
}

//添加一个新的Cookies
func (tk *AbCkCache) AddCookie() {

}

//获取Cookies
func (tk *AbCkCache) GetCookie() (*AbCkCookie, error) {
    return tk.getOneByKey(tk.queueKey)
}

func (tk *AbCkCache) GetCookieByKey(name string) (*AbCkCookie, error) {
    var queueKey = name + ":abck_queue"
    return tk.getOneByKey(queueKey)
}

//获取某个
func (tk *AbCkCache) getOneByKey(queueKey string) (*AbCkCookie, error) {
    tk.getLocker.Lock()
    defer tk.getLocker.Unlock()

    var opt = &redis.ZRangeBy{}
    var minTime = common.CreateTimestamp() + 5000 //提前5秒过期

    opt.Count = 1 //获取一个
    opt.Min = strconv.FormatInt(minTime, 10)
    opt.Max = "+inf"

    ls, _ := tk.cache.Client.ZRangeByScoreWithScores(queueKey, opt).Result()
    if len(ls) <= 0 {
        return nil, errorGetCookies
    }
    //包含了分数和key的val
    var keys = make([]interface{}, len(ls)/2)
    for _, v := range ls {
        keys = append(keys, v.Member)
    }
    le, err := tk.cache.Client.ZRem(queueKey, keys...).Result()
    if tk.LogFun != nil {
        tk.LogFun(le, err)
    }

    for _, v := range ls {
        //task
        var cook = &AbCkCookie{}
        var json = v.Member.(string)
        err = common.JSONParse(json, cook)
        if err != nil {
            return nil, err
        }
        return cook, nil
    }
    return nil, errorGetCookies
}

//添加cookies
func (tk *AbCkCache) Publish(cook AbCkCookie) error {
    tk.getLocker.Lock()
    defer tk.getLocker.Unlock()
    if cook.AkaCookie.Abck == "" {
        return errors.New("abck 值为空")
    }
    if cook.ExpireTime <= 0 {
        cook.ExpireTime = common.CreateTimestamp() + tk.cookieLifeTime
    }
    var str, _ = common.JSONStringify(cook)
    var z = &redis.Z{
        Score:  float64(cook.ExpireTime),
        Member: str,
    }
    _, err := tk.cache.Client.ZAdd(tk.queueKey, z).Result()
    return err
}

func (tk *AbCkCache) GetQueyeKey() string {
    return tk.queueKey
}
func (tk *AbCkCache) GetCookiesCount() int64 {
    var c, err = tk.cache.Client.ZCount(tk.GetQueyeKey(), "-inf", "+inf").Result()
    if err != nil {
        return 0
    }
    return c
}
