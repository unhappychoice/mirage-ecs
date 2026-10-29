package mirageecs_test

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	mirageecs "github.com/acidlemon/mirage-ecs/v2"
)

func TestReverseProxyFlushInterval(t *testing.T) {
	tests := []struct {
		name          string
		flushInterval time.Duration
		wantDelivered bool
	}{
		{name: "streaming response is buffered without flush interval", flushInterval: 0, wantDelivered: false},
		{name: "streaming response is flushed with flush interval", flushInterval: 50 * time.Millisecond, wantDelivered: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := newSSEServer(t)
			proxy := newFlushTestProxy(t, upstream, tt.flushInterval)
			if got := receivesFirstEvent(t, proxy.URL); got != tt.wantDelivered {
				t.Errorf("first event delivered = %v, want %v", got, tt.wantDelivered)
			}
		})
	}
}

func newSSEServer(t *testing.T) *httptest.Server {
	done := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Write([]byte("data: hello\n\n"))
		w.(http.Flusher).Flush()
		select {
		case <-done:
		case <-r.Context().Done():
		}
	}))
	t.Cleanup(func() { close(done); server.Close() })
	return server
}

func newFlushTestProxy(t *testing.T, upstream *httptest.Server, flushInterval time.Duration) *httptest.Server {
	host, portStr, _ := net.SplitHostPort(upstream.Listener.Addr().String())
	port, _ := strconv.Atoi(portStr)
	cfg, err := mirageecs.NewConfig(context.Background(), &mirageecs.ConfigParams{Domain: "example.net"})
	if err != nil {
		t.Fatal(err)
	}
	cfg.Listen.HTTP = []mirageecs.PortMap{{ListenPort: 80, TargetPort: port}}
	cfg.Network.FlushInterval = flushInterval
	rp := mirageecs.NewReverseProxy(cfg)
	rp.AddSubdomain("sse", host, port)
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Host = "sse.example.net"
		rp.ServeHTTPWithPort(w, r, 80)
	}))
	t.Cleanup(proxy.Close)
	return proxy
}

func receivesFirstEvent(t *testing.T, url string) bool {
	client := &http.Client{Timeout: time.Second}
	resp, err := client.Get(url)
	if err != nil {
		t.Logf("request failed: %v", err)
		return false
	}
	defer resp.Body.Close()
	line, err := bufio.NewReader(resp.Body).ReadString('\n')
	return err == nil && line == "data: hello\n"
}
