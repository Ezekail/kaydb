package main

import (
	"flag"
	"fmt"
	"github.com/Ezekail/kaydb"
	"github.com/Ezekail/kaydb/logger"
	"github.com/tidwall/redcon"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

var (
	defaultDBPath            = filepath.Join("/tmp", "kaydb")
	defaultHost              = "127.0.0.1"
	defaultPort              = "5200"
	defaultDatabasesNum uint = 16
)

const dbName = "kaydb-%04d"

func init() {
	// 打印banner
	path, _ := filepath.Abs("resource/banner.txt")
	banner, _ := ioutil.ReadFile(string(path))
	fmt.Println(string(banner))
}

type Server struct {
	dbs    map[int]*kaydb.KayDB // 数据库
	ser    *redcon.Server       // 支持redis
	signal chan os.Signal       // 信号量
	opts   ServerOptions
	mu     *sync.RWMutex
}
type ServerOptions struct {
	dbPath    string
	host      string
	port      string
	databases uint // 数据库编号
}

func main() {
	// 初始化服务器选项，未输入选项flag则默认配置
	serverOpts := new(ServerOptions)
	flag.StringVar(&serverOpts.dbPath, "dbPath", defaultDBPath, "db path")
	flag.StringVar(&serverOpts.host, "host", defaultHost, "server host")
	flag.StringVar(&serverOpts.port, "port", defaultPort, "server port")
	flag.UintVar(&serverOpts.databases, "databases", defaultDatabasesNum, "the number of database")
	flag.Parse()

	// 打开一个默认的数据库
	// 拼接路径+数据库名
	path := filepath.Join(serverOpts.dbPath, fmt.Sprintf(dbName, 0))
	opts := kaydb.DefaultOptions(path)
	now := time.Now()
	db, err := kaydb.Open(opts)
	if err != nil {
		logger.Errorf("open kaydb err,fail to start. %v", err)
		return
	}
	logger.Infof("open db from [%s] successfully,time cost:%v", serverOpts.dbPath, time.Since(now))

	// 发送的信号
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, os.Kill, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	// 初始化dbs
	dbs := make(map[int]*kaydb.KayDB)
	dbs[0] = db

	// 初始化并启动服务
	server := &Server{dbs: dbs, signal: sig, opts: *serverOpts, mu: new(sync.RWMutex)}
	addr := server.opts.host + ":" + server.opts.port

	//NewServerNetwork返回一个新的Redcon服务器。网络网络必须是面向流的网络：“tcp”、“tcp4”、“tcp6”、“unix”或“unixpacket”
	redServer := redcon.NewServerNetwork("tcp", addr, execClientCommand, server.redconAccept,
		func(conn redcon.Conn, err error) {
		})
	server.ser = redServer
	// 启动服务
	go server.listen()
	// 监听到信号，则停止服务
	<-server.signal
	server.stop()
}

// 启动服务器
func (s *Server) listen() {
	logger.Infof("kaydb server is running,ready to accept connections")
	if err := s.ser.ListenAndServe(); err != nil {
		logger.Fatalf("listen and server err,fail to start. %v", err)
		return
	}
}

// 关闭数据库与服务
func (s *Server) stop() {
	for _, db := range s.dbs {
		if err := db.Close(); err != nil {
			logger.Errorf("close db err : %v", err)
		}
	}
	if err := s.ser.Close(); err != nil {
		logger.Errorf("close server err: %v", err)
	}
	logger.Infof("kaydb is ready to exit,bye bye...")
}

func (s *Server) redconAccept(conn redcon.Conn) bool {
	cli := new(Client)
	cli.svr = s
	s.mu.RLock()
	cli.db = s.dbs[0]
	s.mu.RUnlock()
	// 设置用户定义的上下文
	conn.SetContext(cli)
	return true
}
