package main

import (
	"errors"
	"github.com/Ezekail/kaydb"
	"github.com/tidwall/redcon"
	"strings"
)

var errClientIsNil = errors.New("ERR client conn is nil")

type cmdHandler func(cli *Client, arg [][]byte) (interface{}, error)

// 支持的客户端命令
var supportedCommands = map[string]cmdHandler{
	// string commands
	"set":      set,
	"setnx":    setNX,
	"setex":    setEX,
	"mset":     mSet,
	"msetnx":   mSetNX,
	"get":      get,
	"mget":     mGet,
	"getdel":   getDel,
	"append":   appendStr,
	"decr":     decr,
	"decrby":   decrBy,
	"incr":     incr,
	"incrby":   incrBy,
	"strlen":   strLen,
	"strscan":  strScan,
	"getrange": getRange,
	"scan":     scan,

	// list
	"lpush":  lPush,
	"lpushx": lPushX,
	"rpush":  rPush,
	"rpushx": rPushX,
	"lpop":   lPop,
	"rpop":   rPop,
	"lmove":  lMove,
	"llen":   lLen,
	"lindex": lIndex,
	"lset":   lSet,
	"lrange": lRange,

	// hash
	"hset":    hSet,
	"hsetnx":  hSetNX,
	"hget":    hGet,
	"hmget":   hMGet,
	"hdel":    hDel,
	"hexists": hExists,
	"hlen":    hLen,
	"hkeys":   hKeys,
	"hvals":   hVals,
	"hgetall": hGetAll,
	"hscan":   hScan,
	"hincrby": hIncrBy,

	// set
	"sadd":      sAdd,
	"spop":      sPop,
	"srem":      sRem,
	"sismember": sIsMember,
	"smembers":  sMembers,
	"scard":     sCard,
	"sdiff":     sDiff,
	"sunion":    sUnion,

	// zset
	"zadd":      zAdd,
	"zscore":    zScore,
	"zrem":      zRem,
	"zcard":     zCard,
	"zrange":    zRange,
	"zrevrange": zRevRange,
	"zrank":     zRank,
	"zrevrank":  zRevRank,

	// generic commands
	"del": del,

	// connection management commands
	"select": selectDB,
	"ping":   ping,
	"info":   info,
	"quit":   nil,
}

type Client struct {
	db  *kaydb.KayDB // 数据库
	svr *Server      // 服务
}

// 处理客户端命令
func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	// 从map取出命令，判断是否存在
	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("ERR unsupported command '" + string(cmd.Args[0]) + "'")
		return
	}
	// 取出设置的上下文，判断是否存在
	client, _ := conn.Context().(*Client)
	if client == nil {
		conn.WriteError(errClientIsNil.Error())
		return
	}
	switch command {
	case "quit":
		_ = conn.Close()
	default:
		// 遍历的值，写入
		if res, err := cmdFunc(client, cmd.Args[1:]); err != nil {
			if err == kaydb.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
		} else {
			conn.WriteAny(res)
		}
	}
}
