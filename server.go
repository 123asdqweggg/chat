package main

import (
    "context"
    "crypto/sha256"
    "encoding/hex"
    "encoding/json"
    "log"
    "net"
    "net/http"
    "os"
    "os/signal"
    "sync"
    "syscall"
    "time"

    "github.com/gorilla/websocket"
    bolt "go.etcd.io/bbolt"
)

const (
    MaxHistory  = 100
    MinInterval = 2 * time.Second
)

var (
    // 聊天历史
    history     []string
    historyLock sync.Mutex

    // 限流
    lastAccess sync.Map

    // WebSocket 客户端
    clients     = make(map[*websocket.Conn]bool)
    clientsLock sync.Mutex
    broadcast   = make(chan string, 100)

    // BoltDB
    db *bolt.DB

    upgrader = websocket.Upgrader{
        CheckOrigin: func(r *http.Request) bool { return true },
    }
)

func main() {
    // 初始化 DB
    var err error
    db, err = bolt.Open("user.db", 0600, nil)
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    db.Update(func(tx *bolt.Tx) error {
        _, err := tx.CreateBucketIfNotExists([]byte("user_ip_hash"))
        return err
    })

    // 启动 WebSocket 广播协程
    go wsBroadcaster()

    mux := http.NewServeMux()
    mux.Handle("/chat", recoverMiddleware(loggingMiddleware(http.HandlerFunc(handleChat))))
    mux.Handle("/history", recoverMiddleware(loggingMiddleware(http.HandlerFunc(handleHistory))))
    mux.Handle("/ws", recoverMiddleware(loggingMiddleware(http.HandlerFunc(handleWS))))

    srv := &http.Server{
        Addr:    ":80",
        Handler: mux,
    }

    go func() {
        log.Println("Server started on :80")
        if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Fatal(err)
        }
    }()

    // 优雅关闭
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit

    log.Println("Shutting down...")

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    srv.Shutdown(ctx)
    close(broadcast)
}

// -------------------- API --------------------

func handleChat(w http.ResponseWriter, r *http.Request) {
    if r.Method != http.MethodPost {
        http.Error(w, "POST only", 405)
        return
    }

    ip := clientIP(r)
    if !allow(ip) {
        http.Error(w, "Too many requests", 429)
        return
    }

    text := r.FormValue("text")
    if len(text) == 0 || len(text) > 2048 {
        http.Error(w, "invalid text", 400)
        return
    }

    // 写入历史
    historyLock.Lock()
    if len(history) >= MaxHistory {
        history = history[1:]
    }
    history = append(history, text)
    historyLock.Unlock()

    // 持久化 IP hash
    saveIPHash(ip)

    // 广播
    select {
    case broadcast <- text:
    default:
    }

    w.Write([]byte("ok"))
}

func handleHistory(w http.ResponseWriter, r *http.Request) {
    historyLock.Lock()
    data := make([]string, len(history))
    copy(data, history)
    historyLock.Unlock()

    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(data)
}

func handleWS(w http.ResponseWriter, r *http.Request) {
    conn, err := upgrader.Upgrade(w, r, nil)
    if err != nil {
        return
    }

    clientsLock.Lock()
    clients[conn] = true
    clientsLock.Unlock()

    // 推送历史
    go func(c *websocket.Conn) {
        historyLock.Lock()
        hist := append([]string(nil), history...)
        historyLock.Unlock()

        for _, msg := range hist {
            if err := c.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
                break
            }
        }
    }(conn)

    // 读协程（丢弃消息）
    go func(c *websocket.Conn) {
        defer func() {
            clientsLock.Lock()
            delete(clients, c)
            clientsLock.Unlock()
            c.Close()
        }()
        for {
            if _, _, err := c.ReadMessage(); err != nil {
                break
            }
        }
    }(conn)
}

// -------------------- WebSocket 广播 --------------------

func wsBroadcaster() {
    for msg := range broadcast {
        clientsLock.Lock()
        for c := range clients {
            if err := c.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
                c.Close()
                delete(clients, c)
            }
        }
        clientsLock.Unlock()
    }
}

// -------------------- 工具函数 --------------------

func allow(ip string) bool {
    now := time.Now()
    if last, ok := lastAccess.Load(ip); ok {
        if now.Sub(last.(time.Time)) < MinInterval {
            return false
        }
    }
    lastAccess.Store(ip, now)
    return true
}

func saveIPHash(ip string) {
    h := sha256.Sum256([]byte(ip))
    hashStr := hex.EncodeToString(h[:])

    db.Update(func(tx *bolt.Tx) error {
        b := tx.Bucket([]byte("user_ip_hash"))
        return b.Put([]byte(ip), []byte(hashStr))
    })
}

func clientIP(r *http.Request) string {
    host, _, err := net.SplitHostPort(r.RemoteAddr)
    if err != nil {
        return r.RemoteAddr
    }
    return host
}

// -------------------- 中间件 --------------------

func loggingMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL.Path)
        next.ServeHTTP(w, r)
    })
}

func recoverMiddleware(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        defer func() {
            if err := recover(); err != nil {
                log.Println("panic:", err)
                http.Error(w, "internal error", 500)
            }
        }()
        next.ServeHTTP(w, r)
    })
}
