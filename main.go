package main

import (
	"github.com/fsp1yjl/hbase-monitor/dd/test"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"time"

	badger "github.com/dgraph-io/badger"
)

type Monitor struct {
	span time.Duration //探测间隔周期
	Db   string        // 监控数据存放位置
}

// type DB struct {
// 	path string
// 	DB   *badger.DB
// }

type Metric struct {
	Hostname                        string `json:"tag.Hostname"`
	TimeString                      string
	ProcessCallTime_75th_percentile int
	ProcessCallTime_90th_percentile int
	ProcessCallTime_95th_percentile int
	TotalCallTime_99th_percentile   int
}

var queue *MetricQueue

type MetricQueue struct {
	metrics []Metric
}

func (q *MetricQueue) Push(list []Metric) {
	q.metrics = append(q.metrics, list...)

}

func (q *MetricQueue) Pop(size int) []Metric {
	l := len(q.metrics)
	if l < size {
		size = l
	}
	ret := q.metrics[:size]
	q.metrics = q.metrics[size:]
	return ret
}

// func (m *Monitor) getMetric(Id string) Metric {

// 	return nil
// }

func init() {
	queue = new(MetricQueue)
}

func main() {
	aa := new(test.DD)
	fmt.Println(aa)
	//写入测试
	// m := getMetricTest()
	// saveMetric(m)

	// m := getMetric("uhadoop-sjstgcu3-core3")
	// saveMetric(m)
	//前缀读取测试
	// kvGetWithPrefix("uhadoop-sjstgcu3")
	// go intervalFetch()
	// go intervalSave()
	go intervalPrint()
	select {}
	// kvRewind()

}

// 循环获取监控数据
func intervalFetch() {
	d := time.Duration(time.Second * 10)

	t := time.NewTimer(d)
	defer t.Stop()

	for {
		<-t.C

		fmt.Println("do something")
		metrics := hbaseMetricGet()
		queue.Push(metrics)

		// need reset
		t.Reset(time.Second * 10)
	}
}

// 循环获取监控数据保存
func intervalSave() {
	for {

		metrics := queue.Pop(10)
		for _, m := range metrics {
			saveMetric(m)
		}

		time.Sleep(time.Second * 30)

	}
}

func intervalPrint() {
	for {

		kvRewind()

		time.Sleep(time.Second * 10)

	}
}

// 获取此刻全部节点 jmx数据
func hbaseMetricGet() []Metric {
	nodes := []string{"uhadoop-sjstgcu3-core1", "uhadoop-sjstgcu3-core2", "uhadoop-sjstgcu3-core3", "uhadoop-sjstgcu3-core4"}
	var metrics []Metric
	for _, node := range nodes {
		metrics = append(metrics, getMetric(node))
	}

	return metrics
}

func saveMetric(metric Metric) {
	k_prefix := metric.Hostname
	k_end := metric.TimeString

	k_75 := k_prefix + "_ProcessCallTime_75th_percentile_" + k_end
	k_90 := k_prefix + "_ProcessCallTime_90th_percentile_" + k_end
	// k_95 := k_prefix + "_ProcessCallTime_95th_percentile_" + k_end
	// k_99 := k_prefix + "_ProcessCallTime_99th_percentile_" + k_end

	kvSet(k_75, strconv.Itoa(metric.ProcessCallTime_75th_percentile))
	kvSet(k_90, strconv.Itoa(metric.ProcessCallTime_90th_percentile))

}

func kvSet(k string, v string) {
	opts := badger.DefaultOptions("/tmp/badger")
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	err = db.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(k), []byte(v))
		return err
	})
	if err != nil {
		log.Fatal((err))
	}
}

func kvGet(k string) string {
	opts := badger.DefaultOptions("/tmp/badger")
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	var valCopy []byte

	err = db.View(func(txn *badger.Txn) error {
		item, err := txn.Get([]byte(k))
		if err != nil {
			return err
		}
		err = item.Value(func(val []byte) error {
			valCopy = append([]byte{}, val...)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
	return string(valCopy)

}

func kvRewind() {
	opts := badger.DefaultOptions("/tmp/badger")
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			var valCopy []byte
			err = item.Value(func(val []byte) error {
				valCopy = append([]byte{}, val...)
				return nil
			})
			if err != nil {
				return err
			}
			fmt.Printf("%s  key=%s, value=%s\n", time.Now().Format("2006-01-02 15:04:05"), k, string(valCopy))
		}
		return nil
	})

}

func kvGetWithPrefix(pre string) {
	opts := badger.DefaultOptions("/tmp/badger")
	db, err := badger.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		db.View(func(txn *badger.Txn) error {
			it := txn.NewIterator(badger.DefaultIteratorOptions)
			defer it.Close()
			prefix := []byte(pre)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				item := it.Item()
				k := item.Key()
				err := item.Value(func(v []byte) error {
					fmt.Printf("key=%s, value=%s\n", k, v)
					return nil
				})
				if err != nil {
					return err
				}
			}
			return nil
		})
		return nil
	})
}

func getMetric(node string) Metric {
	url := "http://" + node + ":60030/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=IPC"

	resp, err := http.Get(url)
	if err != nil {
		// handle error
	}

	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		// handle error
	}

	type Res struct {
		Beans []Metric `json:"beans"`
	}
	metric_res := new(Res)
	json.Unmarshal(body, metric_res)

	t := time.Now()

	day := t.Day()
	hour := t.Hour()
	minute := t.Minute()
	second := t.Second()
	timeString := fmt.Sprintf("%d_%d_%d_%d", day, hour, minute, second)
	metric_res.Beans[0].TimeString = timeString

	fmt.Println("ddddd------", metric_res)
	return metric_res.Beans[0]
}

func getMetricTest() Metric {

	t := time.Now()

	day := t.Day()
	hour := t.Hour()
	minute := t.Minute()
	second := t.Second()

	timeString := fmt.Sprintf("%d_%d_%d_%d", day, hour, minute, second)

	fmt.Println(timeString)
	body := []byte(`{
                    "beans" : [ {
                      "name" : "Hadoop:service=HBase,name=RegionServer,sub=IPC",
                      "modelerType" : "RegionServer,sub=IPC",
                      "tag.Context" : "regionserver",
                      "tag.Hostname" : "uhadoop-erictest-core3",
                      "queueSize" : 0,
                      "numCallsInGeneralQueue" : 0,
                      "numCallsInReplicationQueue" : 0,
                      "numCallsInPriorityQueue" : 0,
                      "numOpenConnections" : 12,
                      "numActiveHandler" : 0,
                      "numCallsInWriteQueue" : 0,
                      "numCallsInReadQueue" : 0,
                      "numCallsInScanQueue" : 0,
                      "numActiveWriteHandler" : 0,
                      "numActiveReadHandler" : 0,
                      "numActiveScanHandler" : 0,
                      "receivedBytes" : 1270905410103,
                      "exceptions.RegionMovedException" : 56,
                      "exceptions.multiResponseTooLarge" : 0,
                      "authenticationSuccesses" : 0,
                      "authorizationFailures" : 0,
                      "TotalCallTime_num_ops" : 80002757,
                      "TotalCallTime_min" : 0,
                      "TotalCallTime_max" : 92,
                      "TotalCallTime_mean" : 1,
                      "TotalCallTime_25th_percentile" : 0,
                      "TotalCallTime_median" : 0,
                      "TotalCallTime_75th_percentile" : 1,
                      "TotalCallTime_90th_percentile" : 1,
                      "TotalCallTime_95th_percentile" : 1,
                      "TotalCallTime_98th_percentile" : 2,
                      "TotalCallTime_99th_percentile" : 73,
                      "TotalCallTime_99.9th_percentile" : 92,
                      "TotalCallTime_TimeRangeCount_0-1" : 291,
                      "TotalCallTime_TimeRangeCount_1-3" : 3,
                      "TotalCallTime_TimeRangeCount_30-100" : 4,
                      "exceptions.RegionTooBusyException" : 0,
                      "exceptions.FailedSanityCheckException" : 0,
                      "ResponseSize_num_ops" : 80002757,
                      "ResponseSize_min" : 8,
                      "ResponseSize_max" : 25697,
                      "ResponseSize_mean" : 1435,
                      "ResponseSize_25th_percentile" : 8,
                      "ResponseSize_median" : 8,
                      "ResponseSize_75th_percentile" : 8,
                      "ResponseSize_90th_percentile" : 8,
                      "ResponseSize_95th_percentile" : 11121,
                      "ResponseSize_98th_percentile" : 19866,
                      "ResponseSize_99th_percentile" : 22781,
                      "ResponseSize_99.9th_percentile" : 25405,
                      "ResponseSize_SizeRangeCount_0-10" : 272,
                      "ResponseSize_SizeRangeCount_100-1000" : 26,
                      "exceptions.UnknownScannerException" : 0,
                      "exceptions.OutOfOrderScannerNextException" : 0,
                      "exceptions" : 444,
                      "ProcessCallTime_num_ops" : 80002757,
                      "ProcessCallTime_min" : 0,
                      "ProcessCallTime_max" : 92,
                      "ProcessCallTime_mean" : 1,
                      "ProcessCallTime_25th_percentile" : 0,
                      "ProcessCallTime_median" : 0,
                      "ProcessCallTime_75th_percentile" : 1,
                      "ProcessCallTime_90th_percentile" : 1,
                      "ProcessCallTime_95th_percentile" : 1,
                      "ProcessCallTime_98th_percentile" : 2,
                      "ProcessCallTime_99th_percentile" : 73,
                      "ProcessCallTime_99.9th_percentile" : 92,
                      "ProcessCallTime_TimeRangeCount_0-1" : 291,
                      "ProcessCallTime_TimeRangeCount_1-3" : 3,
                      "ProcessCallTime_TimeRangeCount_30-100" : 4,
                      "authenticationFallbacks" : 0,
                      "exceptions.NotServingRegionException" : 388,
                      "exceptions.callQueueTooBig" : 0,
                      "authorizationSuccesses" : 16458,
                      "exceptions.ScannerResetException" : 0,
                      "RequestSize_num_ops" : 80002757,
                      "RequestSize_min" : 144,
                      "RequestSize_max" : 231,
                      "RequestSize_mean" : 201,
                      "RequestSize_25th_percentile" : 163,
                      "RequestSize_median" : 231,
                      "RequestSize_75th_percentile" : 231,
                      "RequestSize_90th_percentile" : 231,
                      "RequestSize_95th_percentile" : 231,
                      "RequestSize_98th_percentile" : 231,
                      "RequestSize_99th_percentile" : 231,
                      "RequestSize_99.9th_percentile" : 231,
                      "RequestSize_SizeRangeCount_0-10" : 12,
                      "RequestSize_SizeRangeCount_100-1000" : 286,
                      "sentBytes" : 10716632615,
                      "QueueCallTime_num_ops" : 80002757,
                      "QueueCallTime_min" : 0,
                      "QueueCallTime_max" : 1,
                      "QueueCallTime_mean" : 0,
                      "QueueCallTime_25th_percentile" : 0,
                      "QueueCallTime_median" : 0,
                      "QueueCallTime_75th_percentile" : 0,
                      "QueueCallTime_90th_percentile" : 0,
                      "QueueCallTime_95th_percentile" : 0,
                      "QueueCallTime_98th_percentile" : 0,
                      "QueueCallTime_99th_percentile" : 0,
                      "QueueCallTime_99.9th_percentile" : 1,
                      "QueueCallTime_TimeRangeCount_0-1" : 298,
                      "authenticationFailures" : 0
                    } ]
                  }`)

	type Res struct {
		Beans []Metric `json:"beans"`
	}
	metric_res := new(Res)
	json.Unmarshal(body, metric_res)

	fmt.Println(metric_res)
	metric_res.Beans[0].TimeString = timeString
	return metric_res.Beans[0]
}

/*
// query test url:
http://uhadoop-sjstgcu3-core3:60030/jmx?qry=Hadoop:service=HBase,name=RegionServer,sub=IPC

{
  "beans" : [ {
    "name" : "Hadoop:service=HBase,name=RegionServer,sub=IPC",
    "modelerType" : "RegionServer,sub=IPC",
    "tag.Context" : "regionserver",
    "tag.Hostname" : "uhadoop-sjstgcu3-core3",
    "queueSize" : 0,
    "numCallsInGeneralQueue" : 0,
    "numCallsInReplicationQueue" : 0,
    "numCallsInPriorityQueue" : 0,
    "numOpenConnections" : 12,
    "numActiveHandler" : 0,
    "numCallsInWriteQueue" : 0,
    "numCallsInReadQueue" : 0,
    "numCallsInScanQueue" : 0,
    "numActiveWriteHandler" : 0,
    "numActiveReadHandler" : 0,
    "numActiveScanHandler" : 0,
    "receivedBytes" : 1270905410103,
    "exceptions.RegionMovedException" : 56,
    "exceptions.multiResponseTooLarge" : 0,
    "authenticationSuccesses" : 0,
    "authorizationFailures" : 0,
    "TotalCallTime_num_ops" : 80002757,
    "TotalCallTime_min" : 0,
    "TotalCallTime_max" : 92,
    "TotalCallTime_mean" : 1,
    "TotalCallTime_25th_percentile" : 0,
    "TotalCallTime_median" : 0,
    "TotalCallTime_75th_percentile" : 1,
    "TotalCallTime_90th_percentile" : 1,
    "TotalCallTime_95th_percentile" : 1,
    "TotalCallTime_98th_percentile" : 2,
    "TotalCallTime_99th_percentile" : 73,
    "TotalCallTime_99.9th_percentile" : 92,
    "TotalCallTime_TimeRangeCount_0-1" : 291,
    "TotalCallTime_TimeRangeCount_1-3" : 3,
    "TotalCallTime_TimeRangeCount_30-100" : 4,
    "exceptions.RegionTooBusyException" : 0,
    "exceptions.FailedSanityCheckException" : 0,
    "ResponseSize_num_ops" : 80002757,
    "ResponseSize_min" : 8,
    "ResponseSize_max" : 25697,
    "ResponseSize_mean" : 1435,
    "ResponseSize_25th_percentile" : 8,
    "ResponseSize_median" : 8,
    "ResponseSize_75th_percentile" : 8,
    "ResponseSize_90th_percentile" : 8,
    "ResponseSize_95th_percentile" : 11121,
    "ResponseSize_98th_percentile" : 19866,
    "ResponseSize_99th_percentile" : 22781,
    "ResponseSize_99.9th_percentile" : 25405,
    "ResponseSize_SizeRangeCount_0-10" : 272,
    "ResponseSize_SizeRangeCount_100-1000" : 26,
    "exceptions.UnknownScannerException" : 0,
    "exceptions.OutOfOrderScannerNextException" : 0,
    "exceptions" : 444,
    "ProcessCallTime_num_ops" : 80002757,
    "ProcessCallTime_min" : 0,
    "ProcessCallTime_max" : 92,
    "ProcessCallTime_mean" : 1,
    "ProcessCallTime_25th_percentile" : 0,
    "ProcessCallTime_median" : 0,
    "ProcessCallTime_75th_percentile" : 1,
    "ProcessCallTime_90th_percentile" : 1,
    "ProcessCallTime_95th_percentile" : 1,
    "ProcessCallTime_98th_percentile" : 2,
    "ProcessCallTime_99th_percentile" : 73,
    "ProcessCallTime_99.9th_percentile" : 92,
    "ProcessCallTime_TimeRangeCount_0-1" : 291,
    "ProcessCallTime_TimeRangeCount_1-3" : 3,
    "ProcessCallTime_TimeRangeCount_30-100" : 4,
    "authenticationFallbacks" : 0,
    "exceptions.NotServingRegionException" : 388,
    "exceptions.callQueueTooBig" : 0,
    "authorizationSuccesses" : 16458,
    "exceptions.ScannerResetException" : 0,
    "RequestSize_num_ops" : 80002757,
    "RequestSize_min" : 144,
    "RequestSize_max" : 231,
    "RequestSize_mean" : 201,
    "RequestSize_25th_percentile" : 163,
    "RequestSize_median" : 231,
    "RequestSize_75th_percentile" : 231,
    "RequestSize_90th_percentile" : 231,
    "RequestSize_95th_percentile" : 231,
    "RequestSize_98th_percentile" : 231,
    "RequestSize_99th_percentile" : 231,
    "RequestSize_99.9th_percentile" : 231,
    "RequestSize_SizeRangeCount_0-10" : 12,
    "RequestSize_SizeRangeCount_100-1000" : 286,
    "sentBytes" : 10716632615,
    "QueueCallTime_num_ops" : 80002757,
    "QueueCallTime_min" : 0,
    "QueueCallTime_max" : 1,
    "QueueCallTime_mean" : 0,
    "QueueCallTime_25th_percentile" : 0,
    "QueueCallTime_median" : 0,
    "QueueCallTime_75th_percentile" : 0,
    "QueueCallTime_90th_percentile" : 0,
    "QueueCallTime_95th_percentile" : 0,
    "QueueCallTime_98th_percentile" : 0,
    "QueueCallTime_99th_percentile" : 0,
    "QueueCallTime_99.9th_percentile" : 1,
    "QueueCallTime_TimeRangeCount_0-1" : 298,
    "authenticationFailures" : 0
  } ]
}

*/
