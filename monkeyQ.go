//author 逆雪寒
//version 0.7.1
//消息队列
package main

import(
	"fmt"
	"time"
	"net/http"
	"encoding/json"
	"flag"
	"log"
	"strconv"
	"runtime"
	"github.com/garyburd/redigo/redis"
	"github.com/hjr265/redsync.go/redsync"
)

var (
	Host string
	Port string
	Redis string
	Auth string
)

var (
	Pool *redis.Pool
	Qlock *redsync.Mutex
)

func must(e error) {
	defer func() {
		if err := recover();err != nil{
			log.Fatalf("Fatal error:%v",err)
		}
	}()
    if e != nil {
        panic(e)
    }
}

func toInt(parameters string) int64{
	 if parameter,err := strconv.ParseInt(parameters,10,32);err != nil{
	 	return 0
	 }else{
	 	return parameter
	 }
}

func init(){

	log.SetFlags(log.LstdFlags)
	flag.StringVar(&Host,"host","localhost","Bound IP. default:localhost")
	flag.StringVar(&Port,"port","9394","port. default:9394")
	flag.StringVar(&Redis,"redis","127.0.0.1:6379","redis server. default:127.0.0.1:6379")
	flag.StringVar(&Auth,"auth","","redis server auth password")
	//sdwoslDq23

	flag.Parse()
	Pool = newPool(Redis)
	log.Printf("Success:redis have connected")

	var err error
	redisPool := []*redis.Pool{Pool}
	Qlock,err = redsync.NewMutexWithPool("redsync",redisPool)
	must(err)

	MonkeyQ = NewMonkey()
	ReadyQ = NewReadyQueue()
    DelayQ = NewDelayQueue()
    Queue = NewQueues()

    if err := Queue.init();err != nil{
        log.Fatalf("queue init error: %s",err.Error())
    }

    DelayQ.Trigger()
}

func newPool(server string) *redis.Pool {
    return &redis.Pool{
        MaxIdle: 8,
        MaxActive:0,
        IdleTimeout: 240 * time.Second,
        Dial: func () (redis.Conn, error) {
            conn, err := redis.Dial("tcp", server)
            if err != nil {
            	log.Printf("Fail:redis connection failed")
                return nil, err
            }

			if Auth != "" {
                if _, err := conn.Do("AUTH", Auth); err != nil {
                    conn.Close()
            		log.Printf("Fail:redis auth failed")
                    return nil, err
                }
            }
            return conn, err
        },
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            _, err := c.Do("PING")
            return err
        },
    }
}

var (
	MonkeyQ *Monkey
	ReadyQ *ReadyQueue
	DelayQ *DelayQueue
	Queue *Queues
)

const(
	OptQueueNames = "SysInfo_queue_names"
)

type OptionQueue struct{
	QueueName string
	VisibilityTimeout string
	MessageRetentionPeriod string
	DelaySeconds string	
}

//队列配置
type Queues struct{
	Option map[string]OptionQueue
	UpdateQueue chan map[string]string
	QueueNameCache map[string]string
}

func NewQueues() *Queues{
	return &Queues{make(map[string]OptionQueue),make(chan map[string]string),make(map[string]string)}
}

func(this *Queues) init() (err error){

    //动态更新队列配置
    go func(){
    	for{
    		select{
    			case uo := <-this.UpdateQueue:
    				this.AddQueueInOpt(uo["queueName"]) //记录新添加的queue
    				this.SaveOptCache(uo["queueName"],uo)
    				go DelayQ.Monitor(uo["queueName"])
    		}
    	}
    }()

    items,_ := this.GetAllQueueInOpt()

    if len(items) == 0 {
    	return 
    }

    var optMap map[string]string
    for _,qname := range items{
	    if optMap,err = this.GetOptions(qname); err != nil{
	    	return
	    }
	    //更新到内存
		this.SaveOptCache(qname,optMap)
    }
	return
}

func(this *Queues) SaveOptCache(qname string,opt map[string]string) {
	this.Option[qname] = OptionQueue{opt["queueName"],opt["visibilityTimeout"],opt["messageRetentionPeriod"],opt["delaySeconds"]}
}

func(this *Queues) Get(queueName string) (qn OptionQueue,ok bool){
	qn,ok = this.Option[queueName]
	return
}

//系统配置
func(this *Queues) AddQueueInOpt(qname string) (err error){
    rdg := Pool.Get()
    defer rdg.Close()
    //记录所有的队列名
    _,err = rdg.Do("SADD",OptQueueNames,qname)

    this.QueueNameCache[qname] = ""

    return 
}

func(this *Queues) DelQueueInOpt(k string)(err error){
    rdg := Pool.Get()
    defer rdg.Close()

	_,err = rdg.Do("SREM",OptQueueNames,k)

	if _,ok := this.QueueNameCache[k];ok{
		delete(this.QueueNameCache,k)
	}
	return
}

func(this *Queues) ExistsQueueInOpt(qname string) (ok bool,err error){
    rdg := Pool.Get()
    defer rdg.Close()

	if _,ok = this.QueueNameCache[qname];!ok {
		ok,err = redis.Bool(rdg.Do("SISMEMBER",OptQueueNames,qname))
	}

	return
}

//获取系统配置
func(this *Queues) GetAllQueueInOpt()([]string,error){
    rdg := Pool.Get()
    defer rdg.Close()

	queues,err := redis.Strings(rdg.Do("SMEMBERS",OptQueueNames))
	for _,q := range queues{
		this.QueueNameCache[q] = ""
	}    
	return queues,err
}

func(this *Queues)DelQueue(queueName string) (err error){
    rdg := Pool.Get()
    defer rdg.Close()

	if _,err = rdg.Do("DEL",this.Table(queueName));err == nil{
		err = this.DelQueueInOpt(queueName)
	}

	return
}

func(this *Queues) Table(queueName string) string{
    return "configureQueue_" + queueName
}

func(this *Queues) GetOptions(queueName string) (opt map[string]string,err error){
    rdg := Pool.Get()
    defer rdg.Close()

    queueName = this.Table(queueName)

	if opt,err = redis.StringMap(rdg.Do("HGETALL",queueName)); err != nil{
		return
	}

	return
}

func(this *Queues) queueExists(queueName string) (bool,error){
    rdg := Pool.Get()
    defer rdg.Close()

	if result,_ := redis.Bool(rdg.Do("EXISTS",this.Table(queueName)));result {
		return true,fmt.Errorf("queue exists:%s",queueName)
	}

	return false,nil
}

func(this *Queues) build(opt OptionQueue) (err error){
    rdg := Pool.Get()
    defer rdg.Close()

    queueName := this.Table(opt.QueueName)

    visibilityTimeout,messageRetentionPeriod,delaySeconds := toInt(opt.VisibilityTimeout),toInt(opt.MessageRetentionPeriod),toInt(opt.DelaySeconds)

    if toInt(opt.VisibilityTimeout) == 0{
    	return fmt.Errorf("VisibilityTimeout must be greater than zero!")
    }

    //隐藏时间
	if _,err = rdg.Do("HSET",queueName,"visibilityTimeout",visibilityTimeout); err != nil{
		return
	}
	//信息最大保存时间
	if _,err = rdg.Do("HSET",queueName,"messageRetentionPeriod",messageRetentionPeriod); err != nil{
		return
	}   
	//延迟队列
	if _,err = rdg.Do("HSET",queueName,"delaySeconds",delaySeconds); err != nil{
		return
	}
	return 
}

func(this *Queues) Create(opt OptionQueue) (err error){
    if result,_ := this.queueExists(opt.QueueName);result{
    	return fmt.Errorf("Queue %s exist",opt.QueueName)
    }
	return this.build(opt)
}

func(this *Queues) Update(opt OptionQueue) (err error){
    if result,_ := this.queueExists(opt.QueueName);!result{
    	return fmt.Errorf("Queue %s doesn't exist",opt.QueueName)
    }
	return this.build(opt)
}

type Monkey struct{

}

func NewMonkey() *Monkey{
	return &Monkey{}
}

//创建队列
func(this *Monkey) Create(optionQueue OptionQueue) (err error){

	if err = Queue.Create(optionQueue); err == nil{
		opt := make(map[string]string)
		opt["queueName"] = optionQueue.QueueName
		opt["visibilityTimeout"] = optionQueue.VisibilityTimeout
		opt["messageRetentionPeriod"] = optionQueue.MessageRetentionPeriod
		opt["delaySeconds"] = optionQueue.DelaySeconds

		Queue.UpdateQueue <- opt //创建
	}
	return
}

func(this *Monkey) Update(optionQueue OptionQueue) (err error){

	if err = Queue.Update(optionQueue); err == nil{
		opt := make(map[string]string)
		opt["queueName"] = optionQueue.QueueName
		opt["visibilityTimeout"] = optionQueue.VisibilityTimeout
		opt["messageRetentionPeriod"] = optionQueue.MessageRetentionPeriod
		opt["delaySeconds"] = optionQueue.DelaySeconds

		Queue.UpdateQueue <- opt //更新
	}
	return
}

//插入队列
func(this *Monkey) Push(queueName string,body string,delaySeconds string) (err error){
	optionQueue,ok := Queue.Get(queueName);

	if !ok{
		return fmt.Errorf("Queue %s exception",queueName)
	}

	var delaySecondsInt,queueDelaySeconds int64

	if delaySeconds != ""{
		delaySecondsInt = toInt(delaySeconds)
	}

	if optionQueue.DelaySeconds != "" {
		queueDelaySeconds = toInt(optionQueue.DelaySeconds)	
	}

	if delaySecondsInt != 0 {
		err = this.delayPush(queueName,body,delaySecondsInt)
	}else if queueDelaySeconds != 0 {
		err = this.delayPush(queueName,body,queueDelaySeconds)
	}else{
		err = this.readyPush(queueName,body)
	}	
	return 
}

//插入延迟队列
func(this *Monkey) delayPush(queueName string,body string,delaySeconds int64) (err error){
	if err = DelayQ.Add(queueName,body,delaySeconds);err != nil{
		return
	}
	return
}

//插入准备队列
func(this *Monkey) readyPush(queueName string,body string) (err error){
	if err = ReadyQ.Push(queueName,body);err != nil{
		return
	}
	return
}

//弹出队列
func(this *Monkey) Pop(queueName string,waitSeconds int) (string,error){
    return ReadyQ.Pop(queueName,waitSeconds)
}

//删除
func(this *Monkey) Del(queueName string,body string) (err error){
	return DelayQ.Del(queueName,body)
	
}

//
func(this *Monkey) SetVisibilityTime(queueName string,body string,visibilityTime int64) (err error){
	err = DelayQ.SetVisibilityTime(queueName,body,visibilityTime)
	return
}


//删除队列
func(this *Monkey) DelQueue(queueName string) (err error){
	if err = ReadyQ.DelQueue(queueName);err != nil{
		return
	}

    if err = DelayQ.DelQueue(queueName); err != nil{
    	return
    }
    if err = Queue.DelQueue(queueName); err != nil{
    	return
    }
    return
}


//准备队列
type ReadyQueue struct{

}

func NewReadyQueue() *ReadyQueue{
	return &ReadyQueue{}
}

func(this *ReadyQueue) Table(queueName string) string{
    return "readyQueue_" + queueName
}

//添加到准备队列
func(this *ReadyQueue) Push(queueName string,id string) (err error){
    rdg := Pool.Get()
    defer rdg.Close()

	_,err = rdg.Do("LPUSH",this.Table(queueName),id)
	return
}

//出队列
func(this *ReadyQueue) Pop(queueName string,waitSeconds int) (string,error) {
    rdg := Pool.Get()
    defer rdg.Close()

    qn := this.Table(queueName)
    values,err := redis.Values(rdg.Do("BRPOP",qn,waitSeconds))

    var nul,body string
    redis.Scan(values,&nul, &body)
    
    return body,err
}

func(this *ReadyQueue) DelQueue(queueName string) (err error){
    rdg := Pool.Get()
    defer rdg.Close()

    _,err = rdg.Do("DEL",this.Table(queueName))
    return err
}

func(this *ReadyQueue) MultiPush(queueName string,items []string){
    rdg := Pool.Get()
    defer rdg.Close()

    queueName = this.Table(queueName)
    for _,v := range items{
		rdg.Send("LPUSH",queueName,v)    	
    }
	rdg.Flush()
}

type DelayQueue struct{

}

func NewDelayQueue() *DelayQueue{
	return &DelayQueue{}
}

func(this *DelayQueue) Table(queueName string) string{
    return "delayQueue_" + queueName
}

//添加
func(this *DelayQueue) Add(queueName string,id string,delaySeconds int64) (err error){
    redis := Pool.Get()
    defer redis.Close()

    delaySeconds = time.Now().Unix() + delaySeconds
	_,err = redis.Do("ZADD",this.Table(queueName),delaySeconds,id)
	return
}

//删除
func(this *DelayQueue) Del(queueName string,id string) (err error){
    redis := Pool.Get()
    defer redis.Close()

	_,err = redis.Do("ZREM",this.Table(queueName),id)
	return
}

func(this *DelayQueue) SetVisibilityTime(queueName string,body string,visibilityTime int64) (err error){
    redis := Pool.Get()
    defer redis.Close()

    optionQueue,_ := Queue.Get(queueName)

    if visibilityTime == 0 {
    	visibilityTime = toInt(optionQueue.VisibilityTimeout)
    }

    visibilityTime = time.Now().Unix() + visibilityTime
	_,err = redis.Do("ZADD",this.Table(queueName),visibilityTime,body)

	return
}

func(this *DelayQueue) DelQueue(queueName string) (err error){
    rdg := Pool.Get()
    defer rdg.Close()

    _,err = rdg.Do("DEL",this.Table(queueName))
    return err
}

//移去 准备队列
func(this *DelayQueue) ToReadyQueue(queueName string,closeQueue chan bool) {
    rdg := Pool.Get()
    defer rdg.Close()

    if ok,_ := Queue.ExistsQueueInOpt(queueName);!ok{
		closeQueue <- true
	    log.Printf("%s queue exit",queueName)
    }

    nowBySecond := time.Now().Unix()
	delayQueueName := this.Table(queueName)

    items,err := redis.Strings(rdg.Do("ZRANGEBYSCORE",delayQueueName,0,nowBySecond))
    must(err)

	_,err = redis.Int(rdg.Do("ZREMRANGEBYSCORE",delayQueueName,0,nowBySecond))
	must(err)

	ReadyQ.MultiPush(queueName,items)

}

//监控队列
func(this *DelayQueue) Monitor(queueName string) {
    redis := Pool.Get()
	ticker := time.NewTicker(1 * time.Second)

    defer func(){
    	redis.Close()
    	ticker.Stop()
    	Qlock.Unlock()
    }()

    closeQueue := make(chan bool,1)

	for{
		select{
			case <-ticker.C:
				if err := Qlock.Lock();err == nil{
					this.ToReadyQueue(queueName,closeQueue)
				}
			case <-closeQueue:
				return
		}
	}
}

//延迟队列定时
func(this *DelayQueue) Trigger() {
    rdg := Pool.Get()
    defer rdg.Close()

    items,_ := Queue.GetAllQueueInOpt()
    for _,qname := range items{
    	go this.Monitor(qname)
    }
}

type CreateResult struct{
	Success bool `json:"success"`
	QueueName string `json:"queueName"`
	VisibilityTimeout string `json:"visibilityTimeout"`
	MessageRetentionPeriod string `json:"messageRetentionPeriod"`
	DelaySeconds string `json:"delaySeconds"`
	Error string `json:"error"`
}

type UpdateResult struct{
	Success bool `json:"success"`
	QueueName string `json:"queueName"`
	VisibilityTimeout string `json:"visibilityTimeout"`
	MessageRetentionPeriod string `json:"messageRetentionPeriod"`
	DelaySeconds string `json:"delaySeconds"`
	Error string `json:"error"`
}

type PushResult struct{
	Success bool `json:"success"`
	QueueName string `json:"queueName"`
	Body string `json:"body"`
	DelaySeconds string `json:"delaySeconds"`
	Error string `json:"error"`
}

type PopResult struct{
	Success bool `json:"success"`
	Body string `json:"body"`
	Error string `json:"error"`
}

type DelResult struct{
	Success bool `json:"success"`
	Body string `json:"body"`
	Error string `json:"error"`
}

type DelQueueResult struct{
	Success bool `json:"success"`
	QueueName string `json:"queueName"`
	Error string `json:"error"`
}


type SetVisibilityTimeResult struct{
	Success bool `json:"success"`
	QueueName string `json:"queueName"`
	VisibilityTimeout string `json:"visibilityTimeout"`
	Error string `json:"error"`
}

func CreateQueue(res http.ResponseWriter,req *http.Request) {
	req.ParseForm()
	queueName := req.PostFormValue("QueueName")
	visibilityTimeout := req.PostFormValue("VisibilityTimeout")
	messageRetentionPeriod := req.PostFormValue("MessageRetentionPeriod")
	delaySeconds := req.PostFormValue("DelaySeconds")

	var optionQueue OptionQueue
	optionQueue.QueueName = queueName
	optionQueue.VisibilityTimeout = visibilityTimeout
	optionQueue.MessageRetentionPeriod = messageRetentionPeriod
	optionQueue.DelaySeconds = delaySeconds

	var(
		err error
		result []byte
	)

	if err = MonkeyQ.Create(optionQueue);err != nil{
		result,err = json.Marshal(CreateResult{false,queueName,visibilityTimeout,messageRetentionPeriod,delaySeconds,err.Error()})
	}else{
		result,err = json.Marshal(CreateResult{true,queueName,visibilityTimeout,messageRetentionPeriod,delaySeconds,""})
	}
	must(err)
	res.Write(result)
}

func UpdateQueue(res http.ResponseWriter,req *http.Request) {
	req.ParseForm()
	queueName := req.PostFormValue("QueueName")
	visibilityTimeout := req.PostFormValue("VisibilityTimeout") //变成活跃时间
	messageRetentionPeriod := req.PostFormValue("MessageRetentionPeriod")//信息最大保存时间
	delaySeconds := req.PostFormValue("DelaySeconds") //延迟时间

	var optionQueue OptionQueue
	optionQueue.QueueName = queueName
	optionQueue.VisibilityTimeout = visibilityTimeout
	optionQueue.MessageRetentionPeriod = messageRetentionPeriod
	optionQueue.DelaySeconds = delaySeconds

	var(
		err error
		result []byte
	)

	if err = MonkeyQ.Update(optionQueue);err != nil{
		result,err = json.Marshal(UpdateResult{false,queueName,visibilityTimeout,messageRetentionPeriod,delaySeconds,err.Error()})
	}else{
		result,err = json.Marshal(UpdateResult{true,queueName,visibilityTimeout,messageRetentionPeriod,delaySeconds,""})
	}
	must(err)
	res.Write(result)
}


func Push(res http.ResponseWriter,req *http.Request) {
	req.ParseForm()
	queueName := req.PostFormValue("queueName")
	body := req.PostFormValue("body")
	delaySeconds := req.PostFormValue("delaySeconds") //延迟时间

	var(
		err error
		result []byte
	)
	if err = MonkeyQ.Push(queueName,body,delaySeconds);err != nil{
		result,err = json.Marshal(PushResult{false,queueName,body,delaySeconds,err.Error()})
	}else{
		result,err = json.Marshal(PushResult{true,queueName,body,delaySeconds,""})
	}
	must(err)
	res.Write(result)

}

func Pop(res http.ResponseWriter,req *http.Request) {
    req.ParseForm()
    queueName := req.Form["queueName"]
    waitSeconds := req.Form["waitSeconds"]

	second := toInt(waitSeconds[0])

    body,err := MonkeyQ.Pop(queueName[0],int(second))

	var(
		result []byte
	)

    if err != nil{
		result,err = json.Marshal(PopResult{false,"",err.Error()})
    }else{
		result,err = json.Marshal(PopResult{true,body,""})
    }
	must(err)
	res.Write(result)
}

func DelMessage(res http.ResponseWriter,req *http.Request){
	req.ParseForm()
	queueName := req.PostFormValue("queueName")
	body := req.PostFormValue("body")

	var(
		result []byte
	)

	err := MonkeyQ.Del(queueName,body)
    if err != nil{
		result,err = json.Marshal(DelResult{false,"",err.Error()})
    }else{
		result,err = json.Marshal(DelResult{true,body,""})
    }
	must(err)
	res.Write(result)
}

func SetVisibilityTime(res http.ResponseWriter,req *http.Request){
	req.ParseForm()

	queueName := req.PostFormValue("queueName")
	body := req.PostFormValue("body")
	visibilityTime := req.PostFormValue("visibilityTime")

	var(
		result []byte
	)

	visibilitySecond := toInt(visibilityTime)
	err := MonkeyQ.SetVisibilityTime(queueName,body,visibilitySecond)
    if err != nil{
		result,err = json.Marshal(SetVisibilityTimeResult{false,queueName,visibilityTime,err.Error()})
    }else{
		result,err = json.Marshal(SetVisibilityTimeResult{true,queueName,visibilityTime,""})
    }
	must(err)
	res.Write(result)
}

func DelQueue(res http.ResponseWriter,req *http.Request) {
	req.ParseForm()
	queueName := req.PostFormValue("queueName")

	var(
		result []byte
	)

	err := MonkeyQ.DelQueue(queueName)
    if err != nil{
		result,err = json.Marshal(DelQueueResult{false,queueName,err.Error()})
    }else{
		result,err = json.Marshal(DelQueueResult{true,queueName,""})
    }
	must(err)
	res.Write(result)
}

func main(){

    runtime.GOMAXPROCS(runtime.NumCPU())

	http.HandleFunc("/createQueue",CreateQueue)
	http.HandleFunc("/updateQueue",UpdateQueue)
	http.HandleFunc("/push",Push)
	http.HandleFunc("/pop",Pop)
	http.HandleFunc("/setVisibilityTime",SetVisibilityTime)
	http.HandleFunc("/delMessage",DelMessage)
	http.HandleFunc("/delQueue",DelQueue)
	log.Printf("Success:HTTP has been started")
	log.Fatal(http.ListenAndServe(Host + ":" + Port,nil))
}


