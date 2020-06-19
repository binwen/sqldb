package sqldb

import (
	"math/rand"
	"strings"
	"sync"
	"time"
)

/*
从库负载策略
*/

var policyHandlerMap = map[string]interface{}{
	"random":           RandomPolicy,
	"weightrandom":     WeightRandomPolicy,
	"roundrobin":       RoundRobinPolicy,
	"weightroundrobin": WeightRoundRobinPolicy,
	"leastconn":        LeastConnPolicy,
}

func RegisterPolicyHandler(name string, handler interface{}) {
	policyHandlerMap[strings.ToLower(name)] = handler
}

func GetPolicyHandler(name string) (handlerFunc interface{}, ok bool) {
	handlerFunc, ok = policyHandlerMap[strings.ToLower(name)]
	return
}

type IPolicy interface {
	Slave(engine *ConnectionEngine) *Connection
}

type PolicyHandler func(engine *ConnectionEngine) *Connection

func (handler PolicyHandler) Slave(engine *ConnectionEngine) *Connection {
	return handler(engine)
}

type PolicyParams struct {
	Weights []int
}

// 随机访问负载策略
func RandomPolicy() PolicyHandler {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	return func(engine *ConnectionEngine) *Connection {
		return engine.slaves[r.Intn(len(engine.slaves))]
	}
}

// 权重随机访问负载策略
func WeightRandomPolicy(params PolicyParams) PolicyHandler {
	weightsLen := len(params.Weights)
	rands := make([]int, 0, weightsLen)

	for i := 0; i < weightsLen; i++ {
		for n := 0; n < params.Weights[i]; n++ {
			rands = append(rands, i)
		}
	}

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	return func(engine *ConnectionEngine) *Connection {
		index := rands[r.Intn(len(rands))]
		count := len(engine.slaves)
		if index >= count {
			index = count - 1
		}

		return engine.slaves[index]
	}
}

// 轮询访问负载策略
func RoundRobinPolicy() PolicyHandler {
	pos := -1
	var lock sync.Mutex

	return func(engine *ConnectionEngine) *Connection {
		lock.Lock()
		defer lock.Unlock()
		pos++

		if pos >= len(engine.slaves) {
			pos = 0
		}

		return engine.slaves[pos]
	}
}

// 权重轮询访问负载策略
func WeightRoundRobinPolicy(params PolicyParams) PolicyHandler {
	weightsLen := len(params.Weights)
	rands := make([]int, 0, weightsLen)
	for i := 0; i < weightsLen; i++ {
		for n := 0; n < params.Weights[i]; n++ {
			rands = append(rands, i)
		}
	}

	pos := -1
	var lock sync.Mutex

	return func(engine *ConnectionEngine) *Connection {
		lock.Lock()
		defer lock.Unlock()
		pos++
		if pos >= len(rands) {
			pos = 0
		}

		index := rands[pos]
		count := len(engine.slaves)
		if index > count {
			index = count - 1
		}

		return engine.slaves[index]
	}
}

// 最小连接数访问负载策略
func LeastConnPolicy() PolicyHandler {
	return func(engine *ConnectionEngine) *Connection {
		connections, index := 0, 0
		for i, count := 0, len(engine.slaves); i < count; i++ {
			openConnections := engine.slaves[i].Stats().OpenConnections
			if i == 0 {
				connections = openConnections
				index = i
			} else if openConnections <= connections {
				connections = openConnections
				index = i
			}
		}

		return engine.slaves[index]
	}
}
