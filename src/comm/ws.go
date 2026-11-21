package comm

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	log "github.com/schollz/croc/v11/src/logger"
	"github.com/schollz/croc/v11/src/models"
	"github.com/schollz/croc/v11/src/utils"
	"golang.org/x/net/proxy"
)

const (
	// defaultWSPingInterval keeps WebSocket connections active well below
	// Cloudflare's ~100-second proxied idle timeout.
	defaultWSPingInterval = 30 * time.Second
	defaultWSPingTimeout  = 10 * time.Second
	maxWSFrameLimit       = 65 * 1024 * 1024
)

// IsWebSocketAddress reports whether address uses a ws:// or wss:// scheme.
func IsWebSocketAddress(address string) bool {
	trimmed := strings.ToLower(strings.TrimSpace(address))
	return strings.HasPrefix(trimmed, "ws://") || strings.HasPrefix(trimmed, "wss://")
}

// NormalizeWebSocketURL validates and normalizes a WebSocket relay address.
// When no path (or only "/") is present, it defaults the path to "/ws" so
// callers can specify bare tunnel origins such as "wss://relay.trycloudflare.com".
func NormalizeWebSocketURL(rawAddress string) (string, error) {
	trimmed := strings.TrimSpace(rawAddress)
	if !IsWebSocketAddress(trimmed) {
		return "", fmt.Errorf("not a websocket address: %q", rawAddress)
	}
	u, err := url.Parse(trimmed)
	if err != nil {
		return "", fmt.Errorf("parse websocket url: %w", err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "ws" && scheme != "wss" {
		return "", fmt.Errorf("unsupported websocket scheme %q", u.Scheme)
	}
	u.Scheme = scheme
	if u.Host == "" {
		return "", fmt.Errorf("websocket url is missing host: %q", rawAddress)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/ws"
	}
	return u.String(), nil
}

// BuildWebSocketPortURL returns a WebSocket URL targeting a specific relay port
// via the "?port=<port>" query parameter.
func BuildWebSocketPortURL(rawAddress, port string) (string, error) {
	normalized, err := NormalizeWebSocketURL(rawAddress)
	if err != nil {
		return "", err
	}
	u, err := url.Parse(normalized)
	if err != nil {
		return "", err
	}
	q := u.Query()
	trimmedPort := strings.TrimSpace(port)
	if trimmedPort != "" {
		q.Set("port", trimmedPort)
	} else {
		q.Del("port")
	}
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// JoinRelayAddressPort combines a relay control address (either host:port or
// ws(s)://...) with a target relay data port.
func JoinRelayAddressPort(baseAddress, port string) (string, error) {
	trimmedPort := strings.TrimSpace(port)
	if trimmedPort == "" {
		return "", fmt.Errorf("relay port is required")
	}
	if IsWebSocketAddress(baseAddress) {
		return BuildWebSocketPortURL(baseAddress, trimmedPort)
	}
	host := strings.TrimSpace(baseAddress)
	if splitHost, _, err := net.SplitHostPort(host); err == nil {
		host = splitHost
	}
	if host == "" {
		return "", fmt.Errorf("bad relay address %q", baseAddress)
	}
	return net.JoinHostPort(host, trimmedPort), nil
}

type wsAddr struct {
	network string
	addr    string
}

func (a wsAddr) Network() string { return a.network }
func (a wsAddr) String() string  { return a.addr }

type wsNetConn struct {
	net.Conn
	remote       net.Addr
	cancelStream context.CancelFunc
	transport    *http.Transport
	closeOnce    sync.Once
	closeErr     error
}

func (c *wsNetConn) RemoteAddr() net.Addr {
	if c.remote != nil {
		return c.remote
	}
	return c.Conn.RemoteAddr()
}

func (c *wsNetConn) Close() error {
	c.closeOnce.Do(func() {
		if c.cancelStream != nil {
			c.cancelStream()
		}
		c.closeErr = c.Conn.Close()
		if c.transport != nil {
			c.transport.CloseIdleConnections()
		}
	})
	return c.closeErr
}

func dialWebSocketContext(dialCtx context.Context, address string) (net.Conn, error) {
	wsURL, err := NormalizeWebSocketURL(address)
	if err != nil {
		return nil, err
	}
	parsedURL, err := url.Parse(wsURL)
	if err != nil {
		return nil, err
	}

	transport := &http.Transport{
		ForceAttemptHTTP2: false,
	}

	if Socks5Proxy != "" && !utils.IsLocalIP(parsedURL.Host) {
		proxyAddress := Socks5Proxy
		if !strings.Contains(proxyAddress, "://") {
			proxyAddress = "socks5://" + proxyAddress
		}
		socksURL, parseErr := url.Parse(proxyAddress)
		if parseErr != nil {
			return nil, fmt.Errorf("unable to parse socks proxy url: %w", parseErr)
		}
		dialer, proxyErr := proxy.FromURL(socksURL, proxy.Direct)
		if proxyErr != nil {
			return nil, fmt.Errorf("proxy failed: %w", proxyErr)
		}
		transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			return dialProxyContext(ctx, dialer, network, addr)
		}
	} else if HttpProxy != "" && !utils.IsLocalIP(parsedURL.Host) {
		proxyAddress := HttpProxy
		if !strings.Contains(proxyAddress, "://") {
			proxyAddress = "http://" + proxyAddress
		}
		httpProxyURL, parseErr := url.Parse(proxyAddress)
		if parseErr != nil {
			return nil, fmt.Errorf("unable to parse http proxy url: %w", parseErr)
		}
		transport.Proxy = http.ProxyURL(httpProxyURL)
	} else {
		transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
			resolved, resolveErr := models.ResolveRelayAddress(ctx, addr)
			if resolveErr != nil {
				return nil, resolveErr
			}
			return (&net.Dialer{}).DialContext(ctx, network, resolved)
		}
	}

	httpClient := &http.Client{Transport: transport}
	wsConn, _, err := websocket.Dial(dialCtx, wsURL, &websocket.DialOptions{
		HTTPClient: httpClient,
	})
	if err != nil {
		transport.CloseIdleConnections()
		return nil, fmt.Errorf("websocket dial %s failed: %w", wsURL, err)
	}
	wsConn.SetReadLimit(maxWSFrameLimit)

	// Keep the stream context separate from dialCtx so that the connection
	// outlives the initial dial timeout.
	streamCtx, cancelStream := context.WithCancel(context.Background())
	netConnection := websocket.NetConn(streamCtx, wsConn, websocket.MessageBinary)

	go func() {
		ticker := time.NewTicker(defaultWSPingInterval)
		defer ticker.Stop()
		for {
			select {
			case <-streamCtx.Done():
				return
			case <-ticker.C:
				pingCtx, cancelPing := context.WithTimeout(streamCtx, defaultWSPingTimeout)
				pingErr := wsConn.Ping(pingCtx)
				cancelPing()
				if pingErr != nil {
					log.Tracef("websocket ping failed for %s: %v", wsURL, pingErr)
					return
				}
			}
		}
	}()

	return &wsNetConn{
		Conn:         netConnection,
		remote:       wsAddr{network: "websocket", addr: parsedURL.Host},
		cancelStream: cancelStream,
		transport:    transport,
	}, nil
}
