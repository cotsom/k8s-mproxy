package main

import (
	"bufio"
	"bytes"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

//go:embed dashboard.html
var dashboardHTML []byte

const (
	maxEvents              = 100
	maxHeaderBytes         = 64 * 1024
	defaultBodySampleBytes = 32 * 1024
)

type proxyConfig struct {
	UpstreamAddr    string
	BodySampleBytes int
	CaptureBytes    int
}

type payloadSample struct {
	Encoding    string `json:"encoding"`
	ContentType string `json:"contentType,omitempty"`
	Size        int64  `json:"size"`
	Captured    int    `json:"captured"`
	Truncated   bool   `json:"truncated"`
	Text        string `json:"text,omitempty"`
	Base64      string `json:"base64,omitempty"`
}

type httpRequest struct {
	Method        string         `json:"method"`
	Path          string         `json:"path"`
	Host          string         `json:"host,omitempty"`
	ContentType   string         `json:"contentType,omitempty"`
	ContentLength int64          `json:"contentLength,omitempty"`
	Body          *payloadSample `json:"body,omitempty"`
}

type httpResponse struct {
	Status        string         `json:"status"`
	StatusCode    int            `json:"statusCode"`
	ContentType   string         `json:"contentType,omitempty"`
	ContentLength int64          `json:"contentLength,omitempty"`
	Body          *payloadSample `json:"body,omitempty"`
}

type tcpSamples struct {
	ToTarget *payloadSample `json:"toTarget,omitempty"`
	ToClient *payloadSample `json:"toClient,omitempty"`
}

type event struct {
	ID                uint64        `json:"id"`
	StartedAt         time.Time     `json:"startedAt"`
	FinishedAt        time.Time     `json:"finishedAt"`
	Client            string        `json:"client"`
	Upstream          string        `json:"upstream"`
	Protocol          string        `json:"protocol"`
	HTTP              *httpRequest  `json:"http,omitempty"`
	Response          *httpResponse `json:"response,omitempty"`
	TCP               *tcpSamples   `json:"tcp,omitempty"`
	BytesToTarget     int64         `json:"bytesToTarget"`
	BytesToClient     int64         `json:"bytesToClient"`
	BodySampleBytes   int           `json:"bodySampleBytes"`
	StreamSampleBytes int           `json:"streamSampleBytes"`
	Error             string        `json:"error,omitempty"`
}

type eventStore struct {
	mu     sync.RWMutex
	nextID uint64
	events []event
}

func (s *eventStore) add(e event) event {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextID++
	e.ID = s.nextID
	if len(s.events) == maxEvents {
		s.events = s.events[1:]
	}
	s.events = append(s.events, e)
	return e
}

func (s *eventStore) snapshot() []event {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]event, len(s.events))
	copy(out, s.events)
	return out
}

// flowRecorder stores a bounded copy of a TCP direction.
// The original stream continues unchanged through io.TeeReader.
type flowRecorder struct {
	mu     sync.Mutex
	limit  int
	buffer []byte
}

func newFlowRecorder(limit int) *flowRecorder {
	if limit < 0 {
		limit = 0
	}
	return &flowRecorder{limit: limit}
}

func (r *flowRecorder) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	originalLength := len(p)
	if r.limit <= len(r.buffer) {
		return originalLength, nil
	}

	remaining := r.limit - len(r.buffer)
	if len(p) > remaining {
		p = p[:remaining]
	}
	r.buffer = append(r.buffer, p...)
	return originalLength, nil
}

func (r *flowRecorder) snapshot() []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]byte, len(r.buffer))
	copy(out, r.buffer)
	return out
}

func inspectTraffic(toTarget, toClient []byte, bytesToTarget, bytesToClient int64, bodyLimit int) (string, *httpRequest, *httpResponse, *tcpSamples) {
	protocol, req := parseHTTPRequest(toTarget, bytesToTarget, bodyLimit)
	if protocol == "http" {
		return protocol, req, parseHTTPResponse(toClient, bytesToClient, bodyLimit), nil
	}
	if protocol == "http2" {
		return protocol, req, nil, nil
	}
	return "tcp", nil, nil, &tcpSamples{
		ToTarget: makePayloadSample(toTarget, bytesToTarget, "", bodyLimit),
		ToClient: makePayloadSample(toClient, bytesToClient, "", bodyLimit),
	}
}

func possibleHTTPPrefix(data []byte) bool {
	for _, method := range []string{"GET ", "POST ", "PUT ", "PATCH ", "DELETE ", "HEAD ", "OPTIONS ", "CONNECT ", "TRACE "} {
		if bytes.HasPrefix(data, []byte(method)) || bytes.HasPrefix([]byte(method), data) {
			return true
		}
	}
	return false
}

func parseHTTPRequest(data []byte, totalBytes int64, bodyLimit int) (string, *httpRequest) {
	if bytes.HasPrefix(data, []byte("PRI * HTTP/2.0")) {
		return "http2", nil
	}
	if len(data) == 0 || !possibleHTTPPrefix(data) {
		return "tcp", nil
	}

	headerEnd := bytes.Index(data, []byte("\r\n\r\n"))
	if headerEnd == -1 {
		return "tcp", nil
	}

	req, err := http.ReadRequest(bufio.NewReader(bytes.NewReader(data[:headerEnd+4])))
	if err != nil {
		return "tcp", nil
	}

	bodyStart := headerEnd + 4
	bodyObserved := totalBytes - int64(bodyStart)
	if bodyObserved < 0 {
		bodyObserved = 0
	}
	contentLength := req.ContentLength
	if contentLength < 0 {
		contentLength = 0
	}
	return "http", &httpRequest{
		Method:        req.Method,
		Path:          req.URL.RequestURI(),
		Host:          req.Host,
		ContentType:   req.Header.Get("Content-Type"),
		ContentLength: contentLength,
		Body:          makePayloadSample(sliceFrom(data, bodyStart, bodyLimit), bodyObserved, req.Header.Get("Content-Type"), bodyLimit),
	}
}

func parseHTTPResponse(data []byte, totalBytes int64, bodyLimit int) *httpResponse {
	if !bytes.HasPrefix(data, []byte("HTTP/")) {
		return nil
	}
	headerEnd := bytes.Index(data, []byte("\r\n\r\n"))
	if headerEnd == -1 {
		return nil
	}

	resp, err := http.ReadResponse(
		bufio.NewReader(bytes.NewReader(data[:headerEnd+4])),
		&http.Request{Method: http.MethodGet},
	)
	if err != nil {
		return nil
	}

	bodyStart := headerEnd + 4
	bodyObserved := totalBytes - int64(bodyStart)
	if bodyObserved < 0 {
		bodyObserved = 0
	}
	contentLength := resp.ContentLength
	if contentLength < 0 {
		contentLength = 0
	}
	return &httpResponse{
		Status:        resp.Status,
		StatusCode:    resp.StatusCode,
		ContentType:   resp.Header.Get("Content-Type"),
		ContentLength: contentLength,
		Body:          makePayloadSample(sliceFrom(data, bodyStart, bodyLimit), bodyObserved, resp.Header.Get("Content-Type"), bodyLimit),
	}
}

func sliceFrom(data []byte, offset int, limit int) []byte {
	if offset >= len(data) || limit <= 0 {
		return nil
	}
	end := offset + limit
	if end > len(data) {
		end = len(data)
	}
	out := make([]byte, end-offset)
	copy(out, data[offset:end])
	return out
}

func makePayloadSample(data []byte, observedBytes int64, contentType string, limit int) *payloadSample {
	if observedBytes <= 0 && len(data) == 0 {
		return nil
	}

	sample := &payloadSample{
		ContentType: contentType,
		Size:        observedBytes,
		Captured:    len(data),
		Truncated:   observedBytes > int64(len(data)),
	}
	if limit == 0 {
		sample.Encoding = "none"
		return sample
	}
	if looksText(data, contentType) {
		sample.Encoding = "text"
		sample.Text = string(data)
		return sample
	}
	sample.Encoding = "base64"
	sample.Base64 = base64.StdEncoding.EncodeToString(data)
	return sample
}

func looksText(data []byte, contentType string) bool {
	if len(data) == 0 {
		return true
	}
	if !utf8.Valid(data) {
		return false
	}

	lowerContentType := strings.ToLower(contentType)
	if strings.HasPrefix(lowerContentType, "text/") ||
		strings.Contains(lowerContentType, "json") ||
		strings.Contains(lowerContentType, "xml") ||
		strings.Contains(lowerContentType, "javascript") ||
		strings.Contains(lowerContentType, "x-www-form-urlencoded") {
		return true
	}

	control := 0
	for _, r := range string(data) {
		if r < 32 && r != '\n' && r != '\r' && r != '\t' {
			control++
		}
	}
	return control*20 < len(data)
}

type copyResult struct {
	n   int64
	err error
}

func proxyConnection(client net.Conn, cfg proxyConfig, events *eventStore) {
	defer client.Close()
	started := time.Now().UTC()
	e := event{
		StartedAt:         started,
		Client:            client.RemoteAddr().String(),
		Upstream:          cfg.UpstreamAddr,
		BodySampleBytes:   cfg.BodySampleBytes,
		StreamSampleBytes: cfg.CaptureBytes,
	}

	upstream, err := net.DialTimeout("tcp", cfg.UpstreamAddr, 5*time.Second)
	if err != nil {
		e.FinishedAt = time.Now().UTC()
		e.Protocol = "tcp"
		e.Error = fmt.Sprintf("dial upstream: %v", err)
		e = events.add(e)
		logEvent(e)
		return
	}
	defer upstream.Close()

	toTargetRecorder := newFlowRecorder(cfg.CaptureBytes)
	toClientRecorder := newFlowRecorder(cfg.CaptureBytes)
	toTarget := make(chan copyResult, 1)
	go func() {
		n, err := io.Copy(upstream, io.TeeReader(client, toTargetRecorder))
		closeWrite(upstream)
		toTarget <- copyResult{n: n, err: err}
	}()

	toClient, clientErr := io.Copy(client, io.TeeReader(upstream, toClientRecorder))
	closeWrite(client)
	targetResult := <-toTarget

	e.FinishedAt = time.Now().UTC()
	e.BytesToTarget = targetResult.n
	e.BytesToClient = toClient
	e.Protocol, e.HTTP, e.Response, e.TCP = inspectTraffic(
		toTargetRecorder.snapshot(),
		toClientRecorder.snapshot(),
		e.BytesToTarget,
		e.BytesToClient,
		cfg.BodySampleBytes,
	)
	if targetResult.err != nil && targetResult.err != io.EOF {
		e.Error = fmt.Sprintf("copy to target: %v", targetResult.err)
	} else if clientErr != nil && clientErr != io.EOF {
		e.Error = fmt.Sprintf("copy to client: %v", clientErr)
	}
	e = events.add(e)
	logEvent(e)
}

func closeWrite(conn net.Conn) {
	if c, ok := conn.(interface{ CloseWrite() error }); ok {
		_ = c.CloseWrite()
	}
}

func logEvent(e event) {
	b, err := json.Marshal(e)
	if err != nil {
		log.Printf("cannot marshal event: %v", err)
		return
	}
	log.Print(string(b))
}

func runAdmin(addr string, events *eventStore) {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("Cache-Control", "no-store")
		_, _ = w.Write(dashboardHTML)
	})
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(events.snapshot())
	})

	server := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Printf("admin API listens on %s", addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("admin API failed: %v", err)
	}
}

func main() {
	upstream := os.Getenv("UPSTREAM_ADDR")
	if upstream == "" {
		log.Fatal("UPSTREAM_ADDR is required, for example 10.244.0.5:80")
	}
	listenAddr := envOr("LISTEN_ADDR", ":8080")
	adminAddr := envOr("ADMIN_ADDR", ":9090")
	bodySampleBytes := envIntOr("BODY_SAMPLE_BYTES", defaultBodySampleBytes)
	cfg := proxyConfig{
		UpstreamAddr:    upstream,
		BodySampleBytes: bodySampleBytes,
		CaptureBytes:    maxHeaderBytes + bodySampleBytes,
	}

	events := &eventStore{}
	go runAdmin(adminAddr, events)

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		log.Fatalf("listen on %s: %v", listenAddr, err)
	}
	log.Printf("proxy listens on %s and forwards to %s", listenAddr, upstream)
	for {
		conn, err := listener.Accept()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Temporary() {
				log.Printf("temporary accept error: %v", err)
				continue
			}
			log.Fatalf("accept: %v", err)
		}
		go proxyConnection(conn, cfg, events)
	}
}

func envOr(name, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(name)); value != "" {
		return value
	}
	return fallback
}

func envIntOr(name string, fallback int) int {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < 0 {
		log.Fatalf("%s must be a non-negative integer, got %q", name, value)
	}
	return n
}
