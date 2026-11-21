package tcp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	log "github.com/schollz/croc/v11/src/logger"
)

const (
	defaultWSBridgeDialTimeout = 10 * time.Second
	maxWebSocketBridgeMessage  = 65 * 1024 * 1024
	wsBridgePingInterval       = 30 * time.Second
	wsBridgePingTimeout        = 10 * time.Second
)

// WebSocketBridge exposes an HTTP/WebSocket listener that bridges binary
// WebSocket streams to a local croc TCP relay's control and data ports.
// Bridging through loopback TCP avoids invoking non-fatal short read deadlines
// (such as the 5ms earlyRoom probe) directly on websocket.NetConn, which would
// otherwise permanently close the WebSocket connection.
type WebSocketBridge struct {
	listener  net.Listener
	server    *http.Server
	done      chan error
	closeOnce sync.Once
}

// Addr returns the bound listener address of the WebSocket bridge.
func (b *WebSocketBridge) Addr() net.Addr {
	if b == nil || b.listener == nil {
		return nil
	}
	return b.listener.Addr()
}

// URL returns the local ws:// URL for connecting to this bridge.
func (b *WebSocketBridge) URL() string {
	if b == nil || b.listener == nil {
		return ""
	}
	return "ws://" + b.listener.Addr().String() + "/ws"
}

// Close shuts down the WebSocket bridge listener.
func (b *WebSocketBridge) Close() error {
	if b == nil {
		return nil
	}
	var err error
	b.closeOnce.Do(func() {
		if b.server != nil {
			err = b.server.Close()
		} else if b.listener != nil {
			err = b.listener.Close()
		}
	})
	return err
}

// Wait blocks until the HTTP server exits.
func (b *WebSocketBridge) Wait() error {
	if b == nil || b.done == nil {
		return nil
	}
	return <-b.done
}

// StartWebSocketBridge binds listenAddr and starts serving the WebSocket-to-TCP
// bridge in the background until ctx is canceled or Close is called.
func StartWebSocketBridge(ctx context.Context, listenAddr, targetHost string, allowedPorts []string) (*WebSocketBridge, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if strings.TrimSpace(listenAddr) == "" {
		return nil, errors.New("websocket bridge listen address is required")
	}
	var cleanPorts []string
	allowedSet := make(map[string]struct{}, len(allowedPorts))
	for _, p := range allowedPorts {
		trimmed := strings.TrimSpace(p)
		if trimmed == "" {
			continue
		}
		if _, exists := allowedSet[trimmed]; !exists {
			allowedSet[trimmed] = struct{}{}
			cleanPorts = append(cleanPorts, trimmed)
		}
	}
	if len(cleanPorts) == 0 {
		return nil, errors.New("websocket bridge requires at least one allowed relay port")
	}

	dialHost := strings.TrimSpace(targetHost)
	if dialHost == "" || dialHost == "0.0.0.0" || dialHost == "::" || dialHost == "[::]" {
		dialHost = "127.0.0.1"
	}

	lc := net.ListenConfig{}
	listener, err := lc.Listen(ctx, "tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("listen websocket bridge on %s: %w", listenAddr, err)
	}

	controlPort := cleanPorts[0]
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})

	wsHandler := func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		port := strings.TrimSpace(r.URL.Query().Get("port"))
		if port == "" || port == "control" {
			port = controlPort
		}
		if _, allowed := allowedSet[port]; !allowed {
			http.Error(w, "relay port is not allowed", http.StatusForbidden)
			return
		}

		dialCtx, cancelDial := context.WithTimeout(r.Context(), defaultWSBridgeDialTimeout)
		defer cancelDial()
		upstream, dialErr := (&net.Dialer{}).DialContext(
			dialCtx,
			"tcp",
			net.JoinHostPort(dialHost, port),
		)
		if dialErr != nil {
			http.Error(w, "relay port is unavailable", http.StatusBadGateway)
			return
		}

		socket, acceptErr := websocket.Accept(w, r, &websocket.AcceptOptions{
			InsecureSkipVerify: true,
		})
		if acceptErr != nil {
			_ = upstream.Close()
			return
		}
		socket.SetReadLimit(maxWebSocketBridgeMessage)

		streamCtx, cancelStream := context.WithCancel(ctx)
		go func() {
			ticker := time.NewTicker(wsBridgePingInterval)
			defer ticker.Stop()
			for {
				select {
				case <-streamCtx.Done():
					return
				case <-ticker.C:
					pingCtx, cancelPing := context.WithTimeout(streamCtx, wsBridgePingTimeout)
					pingErr := socket.Ping(pingCtx)
					cancelPing()
					if pingErr != nil {
						return
					}
				}
			}
		}()

		web := websocket.NetConn(streamCtx, socket, websocket.MessageBinary)
		var closeOnce sync.Once
		closeBoth := func() {
			closeOnce.Do(func() {
				cancelStream()
				_ = web.Close()
				_ = upstream.Close()
			})
		}
		defer closeBoth()

		errc := make(chan error, 2)
		copyStream := func(dst io.Writer, src io.Reader) {
			buffer := make([]byte, 64*1024)
			_, copyErr := io.CopyBuffer(dst, src, buffer)
			errc <- copyErr
			closeBoth()
		}
		go copyStream(upstream, web)
		go copyStream(web, upstream)
		<-errc
		<-errc
	}

	mux.HandleFunc("/ws", wsHandler)
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			wsHandler(w, r)
			return
		}
		http.NotFound(w, r)
	})

	httpServer := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 15 * time.Second,
	}
	bridge := &WebSocketBridge{
		listener: listener,
		server:   httpServer,
		done:     make(chan error, 1),
	}
	stopClose := context.AfterFunc(ctx, func() {
		_ = bridge.Close()
	})
	go func() {
		defer stopClose()
		log.Infof("starting WebSocket relay bridge on %s (target %s, ports %s)", listener.Addr().String(), dialHost, strings.Join(cleanPorts, ","))
		serveErr := httpServer.Serve(listener)
		if errors.Is(serveErr, http.ErrServerClosed) || errors.Is(serveErr, net.ErrClosed) {
			serveErr = nil
		}
		bridge.done <- serveErr
		close(bridge.done)
	}()
	return bridge, nil
}

// RunWebSocketBridge starts the bridge and blocks until it stops.
func RunWebSocketBridge(ctx context.Context, listenAddr, targetHost string, allowedPorts []string) error {
	bridge, err := StartWebSocketBridge(ctx, listenAddr, targetHost, allowedPorts)
	if err != nil {
		return err
	}
	return bridge.Wait()
}
