package cleaner

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/shevacn/rdb-cleaner/internal/connector"
	"github.com/shevacn/rdb-cleaner/internal/log"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

type Cleaner struct {
	Config *Config
}

type Task struct {
	NodeId         string      `json:"nodeId"`
	Pattern        string      `json:"pattern"`
	Action         string      `json:"action"`
	DelMaxLen      int64       `json:"delMaxLen"`
	DelLimitByNum  map[int]int `json:"delLimitByNum"`
	ForceStrategy  []string    `json:"forceStrategy"`
	compiledRegexp *regexp.Regexp
	patternCnt     int
	delKeyCnt      int32
}

type Config struct {
	Nodes []connector.NodeConfig `json:"nodes"`

	Tasks []Task `json:"tasks"`
}

type SubDelTask struct {
	task      *Task
	taskIndex int
	conn      *connector.RdbClient
	key       []string
}

var usageStr = `Usage: rdb-cleaner [options]

Broker Options:
    -c,  --config <file path>              Configuration file path
`
var ctx = context.Background()

func (c *Cleaner) Start() {

	workerNum := 500
	taskChan := make(chan *SubDelTask)

	for workerNum > 0 {
		workerNum--
		go startWorker(taskChan, workerNum)
	}

	for index, task := range c.Config.Tasks {

		patternRegex := strings.ReplaceAll(task.Pattern, "*", "(.+)")
		task.compiledRegexp = regexp.MustCompile(patternRegex)
		task.patternCnt = strings.Count(task.Pattern, "*")

		go doTask(index, task, taskChan)
	}
}

func startWorker(taskChan chan *SubDelTask, workerId int) {
	var delTask *SubDelTask
	var ok bool

	for {
		delTask, ok = <-taskChan
		if !ok {
			break
		}
		if delTask.task.DelLimitByNum != nil {
			delKeys := make([]string, 0, len(delTask.key))
			for _, k := range delTask.key {
				patternMatchs := delTask.task.compiledRegexp.FindAllStringSubmatch(k, delTask.task.patternCnt)
				//fmt.Printf("task pattern=%s, key=%s, matchs=%s\n", task.Pattern, key, patternMatchs)
				if len(patternMatchs) == 0 {
					panic(nil)
				}
				for index, maxNum := range delTask.task.DelLimitByNum {
					//if len(patternMatchs[0]) < index-1 {
					//	panic(nil)
					//}
					num, err := strconv.Atoi(patternMatchs[0][index+1])
					if err != nil {
						log.Logger.Error("parse key by pattern error", zap.String("key", k), zap.String("mached", patternMatchs[0][index+1]), zap.Int("taskIndex", delTask.taskIndex), zap.String("taskPattern", delTask.task.Pattern))
						continue
						//panic(err)
					}
					if num <= maxNum {
						delKeys = append(delKeys, k)
					} else {
						//fmt.Printf("skip task pattern=%s, key=%s, because %d > %d\n", delTask.task.Pattern, k, num, maxNum)
					}
				}
			}
			delTask.key = delKeys
		}
		if len(delTask.key) == 0 {
			continue
		}
		delTask.conn.RateLimit.Take()
		//log.Logger.Info(fmt.Sprintf("[task-%d][worker-%d]del key=%s", delTask.taskIndex, workerId, delTask.key))
		delTask.conn.Conn.Del(ctx, delTask.key...)

		atomic.AddInt32(&delTask.task.delKeyCnt, int32(len(delTask.key)))
	}
}

//func StrPadLeft(input string, padLength int, padString string) string {
//	output := padString
//
//	for padLength > len(output) {
//		output += output
//	}
//
//	if len(input) >= padLength {
//		return input
//	}
//
//	return output[:padLength-len(input)] + input
//}
//
//func Strrev(s string) string {
//	n := len(s)
//	runes := make([]rune, n)
//	for _, rune := range s {
//		n--
//		runes[n] = rune
//	}
//	return string(runes[n:])
//}
//
//func Bindec(str string) string {
//	i, err := strconv.ParseInt(str, 2, 0)
//	if err != nil {
//		return ""
//	}
//	return strconv.FormatInt(i, 10)
//}
//
//func reverseBits(cursor uint64, pow float64) (int, error) {
//
//	bin := strconv.FormatUint(cursor, 2)
//
//	bin = StrPadLeft(bin, int(pow), "0")
//
//	return strconv.Atoi(Bindec(Strrev(bin)))
//}
//
//func scanProgress(cursor uint64, dbSize int64) int {
//	nextPowerOfTwo := math.Ceil(math.Log(float64(dbSize)) / math.Log(2))
//	hashTableSize := math.Pow(2, nextPowerOfTwo)
//	hashTableIndex, _ := reverseBits(cursor, nextPowerOfTwo)
//
//	return hashTableIndex * 100 / int(hashTableSize)
//}

func doTask(taskIndex int, task Task, taskChan chan *SubDelTask) {
	fmt.Printf("Start task, taskIndex=%d, task=%s\n", taskIndex, task)
	time.Sleep(5 * time.Second)

	rdb := connector.GetNodeConn(task.NodeId)

	var cursor uint64
	var i int
	batchDelKeyCnt := 20
	var delKeys = make([]string, 0, batchDelKeyCnt)
	for {
		var keys []string
		var err error
		keys, cursor, err = rdb.Conn.Scan(ctx, cursor, task.Pattern, 300).Result()
		if err != nil {
			panic(err)
		}

		for _, key := range keys {
			i++
			if task.Action == "del" && task.DelMaxLen > 0 {
				t, _ := rdb.Conn.Type(ctx, key).Result()
				var size int64
				switch t {
				case "zset":
					size, _ = rdb.Conn.ZCard(ctx, key).Result()
				case "hash":
					size, _ = rdb.Conn.HLen(ctx, key).Result()
				case "set":
					size, _ = rdb.Conn.SCard(ctx, key).Result()
				}
				if size == 0 || size > task.DelMaxLen {
					fmt.Printf("task index=%d, task pattern=%s, skip del key=%s, type=%s, size=%d\n", taskIndex, task.Pattern, key, t, size)
					continue
				}
			}

			if task.Action == "show" {
				fmt.Printf("task index=%d, task pattern=%s, show key=%s\n", taskIndex, task.Pattern, key)
			} else if task.Action == "del" {
				delKeys = append(delKeys, key)

				if len(delKeys) == batchDelKeyCnt {
					var ks []string
					ks = append(delKeys[:0:0], delKeys...)
					delTask := &SubDelTask{
						task:      &task,
						taskIndex: taskIndex,
						conn:      rdb,
						key:       ks,
					}
					taskChan <- delTask

					delKeys = delKeys[0:0]
				}
			}
		}

		if cursor == 0 { // no more keys
			log.Logger.Info(fmt.Sprintf("task index=%d, nodeId=%s, task pattern=%s, del key cnt=%d, completed!", taskIndex, task.NodeId, task.Pattern, task.delKeyCnt))
			if len(delKeys) > 0 {
				delTask := &SubDelTask{
					task:      &task,
					taskIndex: taskIndex,
					conn:      rdb,
					key:       delKeys,
				}
				taskChan <- delTask
			}
			break
		}
	}
}

func showHelp() {
	fmt.Printf("%s\n", usageStr)
	os.Exit(0)
}

func LoadConfig(args []string) (*Config, error) {
	config := &Config{}
	var (
		help       bool
		configFile string
	)
	fs := flag.NewFlagSet("hmq-broker", flag.ExitOnError)
	fs.Usage = showHelp

	fs.BoolVar(&help, "h", false, "Show this message.")
	fs.BoolVar(&help, "help", false, "Show this message.")
	fs.StringVar(&configFile, "config", "", "config file for hmq")
	fs.StringVar(&configFile, "c", "", "config file for hmq")

	if err := fs.Parse(args); err != nil {
		return nil, err
	}

	if help {
		showHelp()
		return nil, nil
	}

	if configFile != "" {
		tmpConfig, e := loadConfigFile(configFile)
		if e != nil {
			panic(e)
		} else {
			config = tmpConfig
		}
	} else {
		showHelp()
		return nil, nil
	}
	return config, nil
}

func loadConfigFile(filename string) (*Config, error) {

	content, err := ioutil.ReadFile(filename)
	if err != nil {
		// log.Error("Read config file error: ", zap.Error(err))
		return nil, err
	}
	// log.Info(string(content))

	var config Config
	err = json.Unmarshal(content, &config)
	if err != nil {
		// log.Error("Unmarshal config file error: ", zap.Error(err))
		return nil, err
	}

	return &config, nil
}
