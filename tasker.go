package rdsworker

import (
    "fmt"
    "github.com/go-redis/redis/v7"
    "github.com/zhaojunlike/logger"
    "strconv"
    "sync"
    "time"
    "zhaojunlike/cacher"
    "zhaojunlike/common"
)

type TaskWorker struct {
    cache       *cacher.GtCache
    Channel     chan Task
    LogFun      LogInfo
    Name        string
    queueKey    string
    dataMapKey  string
    isStart     bool
    Speed       int64
    freeCount   int
    maxCount    int
    createCount int
    locker      *sync.Mutex
    mChan       chan int //通道
    workerChan  []chan int
}
type Task struct {
    Id         string      `json:"id"`
    Data       interface{} `json:"data"`
    Time       int64       `json:"time"`
    Retry      int         `json:"retry"`
    Attempts   int         `json:"attempts"`
    Type       string      `json:"type"`
    Next       string      `json:"next"`
    TimeStart  int64       `json:"time_start"`
    TimeFinish int64       `json:"time_finish"`
}

func init() {

}

type LogInfo func(msg interface{}, err error)

func NewDefaultTask(action string) Task {
    var task = Task{}
    task.Id = common.CreateUUIDV4()
    task.Time = common.CreateTimestamp()
    task.TimeStart = common.CreateTimestamp()
    task.Retry = 1
    task.Type = action
    return task
}

func NewTaskWorker(name string, ch *cacher.GtCache) *TaskWorker {
    var worker = &TaskWorker{}
    worker.Channel = make(chan Task, 1000) //数据通道最多100个，如果每有取完写入会被阻塞
    worker.cache = ch
    worker.Name = name
    worker.dataMapKey = name + ":task_mapper"
    worker.queueKey = name + ":task_queue"
    worker.Speed = 100 //每次最多获取50个数据
    worker.freeCount = 0
    worker.createCount = 0 //当前创建了多少了
    worker.maxCount = 5000
    worker.locker = new(sync.Mutex)
    worker.mChan = make(chan int)
    return worker
}

//启动
func (tk *TaskWorker) Start() {
    tk.locker.Lock()
    defer tk.locker.Unlock()
    if !tk.isStart {
        tk.isStart = true
        go tk.monitor(tk.mChan)
    }
}
func (tk *TaskWorker) Stop() {
    tk.locker.Lock()
    defer tk.locker.Unlock()
    if !tk.isStart {
        return
    }
    //1.关闭查询redis的通道
    tk.mChan <- 1 //写入退出
    <-tk.mChan    //等待退出
    tk.isStart = false
}

//关闭一些通道
func (tk *TaskWorker) Close() {
    tk.Stop() //1.先退出
    close(tk.mChan)
    close(tk.Channel)
}

//开启一个工作线程,创建后不需要跳出循环
func (tk *TaskWorker) NewWorker(do func(task Task)) {
    tk.locker.Lock()
    defer tk.locker.Unlock()
    if tk.createCount > tk.maxCount {
        return //最多放入多少个工作线程
    }
    tk.createCount++
    tk.freeCount++
    go func() {
        for {
            //如果通道关闭，第二个值会变成空值
            task, ok := <-tk.Channel
            if ok == false {
                break
            }
            tk.busy(1)
            do(task) //调用函数
            tk.free(1)
        }
        tk.done(1) //协程退出标记
    }()

}

//创建工作协程执行
func (tk *TaskWorker) NewWorkers(count int, do func(task Task)) {
    for i := 0; i < count; i++ {
        tk.NewWorker(do)
    }
}

//
func (tk *TaskWorker) busy(c int) {
    tk.locker.Lock()
    defer tk.locker.Unlock()
    tk.freeCount -= c
}

//空闲的协程
func (tk *TaskWorker) free(c int) {
    tk.locker.Lock()
    defer tk.locker.Unlock()
    tk.freeCount += c
}

//协程退出
func (tk *TaskWorker) done(c int) {
    tk.locker.Lock()
    defer tk.locker.Unlock()
    tk.createCount -= c
    tk.freeCount -= c //free后也要释放free
}

//发布任务
func (tk *TaskWorker) Publish(task Task) error {
    var str, err = common.JSONStringify(task)
    if err != nil {
        return err
    }
    _, err = tk.cache.Client.HSet(tk.dataMapKey, task.Id, str).Result()
    if err != nil {
        return err
    }
    //设置排序
    var z = redis.Z{Score: float64(task.Time), Member: task.Id}
    _, err = tk.cache.Client.ZAdd(tk.queueKey, &z).Result()
    if err != nil {
        return err
    }
    return nil
}

func (tk *TaskWorker) Clear() {

}

//TODO 考虑是否增加协程
func (tk *TaskWorker) monitor(stop chan int) {
    for {
        select {
        case <-stop:
            stop <- 1
            break
        default:
        }
        time.Sleep(time.Millisecond * 20) //20ms检测一次哭
        tk.doFetcher()
    }
}

func (tk *TaskWorker) doFetcher() {
    //如果工作协程执行太慢会导致Channel超最大值被阻塞,所以我们可以判断一下freeCount，如果没有了那我就停止获取数据
    if tk.freeCount <= 0 {
        return
    }
    //分数删除成功后就需要进行解锁
    var scores = tk.pullScores(tk.Speed) //这里面会自动加锁
    for _, v := range scores {
        if v.Member == nil {
            logger.Error("v.member=nil", v)
            continue //异常的TODO 做日志
        }
        //task
        var id = v.Member.(string)
        //获取
        task, err := tk.GetTask(id)
        if err != nil {
            continue
        }
        //删除集合中的数据
        _ = tk.delTask(id)
        tk.Channel <- task
    }
}

//获取分数
func (tk *TaskWorker) pullScores(count int64) []redis.Z {
    //加分布式lock
    var lockKey = tk.Name + ":task_timeout_lock"
    var freeTime = 5 * time.Second
    ok, err := tk.cache.Client.SetNX(lockKey, "OK", freeTime).Result()
    if err != nil || !ok {
        return nil
    }
    //移除KEY
    defer tk.cache.Client.Del(lockKey)

    var opt = &redis.ZRangeBy{}
    opt.Count = count
    opt.Min = "0"
    opt.Max = strconv.FormatInt(common.CreateTimestamp(), 10)

    //不用担心score相同
    ls, _ := tk.cache.Client.ZRangeByScoreWithScores(tk.queueKey, opt).Result()
    var keys = make([]interface{}, len(ls)/2)
    for _, v := range ls {
        keys = append(keys, v.Member)
    }
    le, err := tk.cache.Client.ZRem(tk.queueKey, keys...).Result()
    if tk.LogFun != nil {
        tk.LogFun(le, err)
    }
    return ls
}
func (tk *TaskWorker) GetTask(id string) (Task, error) {
    var task Task
    str, err := tk.cache.Client.HGet(tk.dataMapKey, id).Result()
    if err != nil {
        return task, err
    }
    //判断str
    err = common.JSONParse(str, &task)
    if err != nil {
        return task, err
    }
    return task, nil
}

func (tk *TaskWorker) delTask(id string) error {
    _, err := tk.cache.Client.HDel(tk.dataMapKey, id).Result()
    return err
}

//free
func (tk *TaskWorker) GetStatus() int {
    return tk.freeCount
}
func (tk *TaskWorker) GetStatusString() string {
    return fmt.Sprintf("协程总数:%v,空闲数:%v,忙碌数:%v", tk.createCount, tk.freeCount, tk.createCount-tk.freeCount)
}
