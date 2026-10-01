// clickstream-generator (Go) — высоконагруженный PRODUCER на franz-go.
//
// Шлёт поток «кликов» в топик clickstream с настраиваемой скоростью (до десятков тысяч msg/s).
// На нём удобно смотреть, как batching (linger), компрессия и acks влияют на пропускную способность,
// и как producer буферизует сообщения, когда брокеры недоступны (backpressure).
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	topic       = "clickstream"
	maxBuffered = 200_000
)

// ---------------------------------------------------------------- конфигурация

type Config struct {
	Rate        int    `json:"rate"`        // сообщений в секунду
	SizeBytes   int    `json:"sizeBytes"`   // примерный размер сообщения
	Acks        string `json:"acks"`        // "0" | "1" | "all"
	Compression string `json:"compression"` // none | gzip | snappy | lz4 | zstd
	LingerMs    int    `json:"lingerMs"`    // сколько ждать наполнения batch
	Keyed       bool   `json:"keyed"`       // ключ = sessionId (иначе ключа нет → sticky partitioner)
}

type ConfigPatch struct {
	Rate        *int    `json:"rate"`
	SizeBytes   *int    `json:"sizeBytes"`
	Acks        *string `json:"acks"`
	Compression *string `json:"compression"`
	LingerMs    *int    `json:"lingerMs"`
	Keyed       *bool   `json:"keyed"`
}

func (c Config) sameClient(o Config) bool {
	return c.Acks == o.Acks && c.Compression == o.Compression && c.LingerMs == o.LingerMs
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// ---------------------------------------------------------------- метрики

type Rate struct {
	mu      sync.Mutex
	total   int64
	buckets [8]int64
	sec     int64
}

func (r *Rate) advance() {
	now := time.Now().Unix()
	if now == r.sec {
		return
	}
	steps := now - r.sec
	if steps > 8 {
		steps = 8
	}
	for i := int64(1); i <= steps; i++ {
		r.buckets[(r.sec+i)%8] = 0
	}
	r.sec = now
}

func (r *Rate) Add(n int64) {
	r.mu.Lock()
	r.advance()
	r.total += n
	r.buckets[r.sec%8] += n
	r.mu.Unlock()
}

func (r *Rate) Snapshot() (int64, float64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.advance()
	var sum int64
	for i := int64(1); i <= 3; i++ {
		sum += r.buckets[(r.sec-i+8)%8]
	}
	return r.total, float64(int(float64(sum)/3.0*10)) / 10
}

type Latency struct {
	mu    sync.Mutex
	ring  [4096]struct{ at time.Time; ms float64 }
	next  int
	count int
}

func (l *Latency) Record(d time.Duration) {
	l.mu.Lock()
	l.ring[l.next] = struct {
		at time.Time
		ms float64
	}{time.Now(), float64(d.Microseconds()) / 1000}
	l.next = (l.next + 1) % len(l.ring)
	if l.count < len(l.ring) {
		l.count++
	}
	l.mu.Unlock()
}

func (l *Latency) Snapshot() map[string]any {
	l.mu.Lock()
	since := time.Now().Add(-5 * time.Second)
	vals := make([]float64, 0, l.count)
	for i := 0; i < l.count; i++ {
		if l.ring[i].at.After(since) {
			vals = append(vals, l.ring[i].ms)
		}
	}
	l.mu.Unlock()
	if len(vals) == 0 {
		return map[string]any{"p50": nil, "p95": nil, "p99": nil, "max": nil, "samples": 0}
	}
	sort.Float64s(vals)
	p := func(q float64) float64 {
		i := int(q * float64(len(vals)))
		if i >= len(vals) {
			i = len(vals) - 1
		}
		return float64(int(vals[i]*10)) / 10
	}
	return map[string]any{"p50": p(0.5), "p95": p(0.95), "p99": p(0.99), "max": p(1), "samples": len(vals)}
}

type Event struct {
	ID    int64  `json:"id"`
	Ts    int64  `json:"ts"`
	Level string `json:"level"`
	Text  string `json:"text"`
	Learn string `json:"learn,omitempty"`
}

type EventLog struct {
	mu       sync.Mutex
	events   []Event
	nextID   int64
	throttle map[string]time.Time
}

func (e *EventLog) Add(level, text, learn, key string, every time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if key != "" {
		if t, ok := e.throttle[key]; ok && time.Since(t) < every {
			return
		}
		e.throttle[key] = time.Now()
	}
	e.nextID++
	e.events = append(e.events, Event{ID: e.nextID, Ts: time.Now().UnixMilli(), Level: level, Text: text, Learn: learn})
	if len(e.events) > 100 {
		e.events = e.events[len(e.events)-100:]
	}
	log.Printf("[%s] %s", level, text)
}

func (e *EventLog) Recent() []Event {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := len(e.events)
	if n > 50 {
		return append([]Event(nil), e.events[n-50:]...)
	}
	return append([]Event(nil), e.events...)
}

// ---------------------------------------------------------------- генератор

type Generator struct {
	seeds []string

	mu       sync.Mutex
	cfg      Config
	client   *kgo.Client
	stopLoop context.CancelFunc
	gen      int64

	sent, acked, failed, bytes Rate
	bufferFull                 atomic.Int64
	lastErrTs                  atomic.Int64
	partitions                 [64]atomic.Int64
	latency                    Latency
	errMu                      sync.Mutex
	errCounts                  map[string]int64
	events                     EventLog
	started                    time.Time
	seq                        atomic.Int64
}

func newGenerator(seeds []string) *Generator {
	g := &Generator{
		seeds:     seeds,
		cfg:       Config{Rate: 1000, SizeBytes: 200, Acks: "1", Compression: "lz4", LingerMs: 10, Keyed: false},
		errCounts: map[string]int64{},
		events:    EventLog{throttle: map[string]time.Time{}},
		started:   time.Now(),
	}
	return g
}

func (g *Generator) buildClient(cfg Config) (*kgo.Client, error) {
	codec := map[string]kgo.CompressionCodec{
		"none": kgo.NoCompression(), "gzip": kgo.GzipCompression(), "snappy": kgo.SnappyCompression(),
		"lz4": kgo.Lz4Compression(), "zstd": kgo.ZstdCompression(),
	}[cfg.Compression]

	opts := []kgo.Opt{
		kgo.SeedBrokers(g.seeds...),
		kgo.ClientID("clickstream-generator"),
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerLinger(time.Duration(cfg.LingerMs) * time.Millisecond),
		kgo.ProducerBatchCompression(codec),
		kgo.RecordDeliveryTimeout(30 * time.Second),
		kgo.ProduceRequestTimeout(10 * time.Second),
		kgo.MaxBufferedRecords(maxBuffered),
		kgo.MetadataMaxAge(30 * time.Second),
	}
	switch cfg.Acks {
	case "0":
		// Fire-and-forget: максимальная скорость, но producer даже не узнает о потере.
		opts = append(opts, kgo.RequiredAcks(kgo.NoAck()), kgo.DisableIdempotentWrite())
	case "1":
		opts = append(opts, kgo.RequiredAcks(kgo.LeaderAck()), kgo.DisableIdempotentWrite())
	default:
		// В franz-go идемпотентность включена по умолчанию и требует acks=all.
		opts = append(opts, kgo.RequiredAcks(kgo.AllISRAcks()))
	}
	return kgo.NewClient(opts...)
}

// apply применяет конфигурацию; при смене acks/compression/linger пересоздаёт клиента.
func (g *Generator) apply(next Config) error {
	g.mu.Lock()
	cur := g.cfg
	if g.client != nil && next.sameClient(cur) {
		g.cfg = next
		g.mu.Unlock()
		return nil
	}
	client, err := g.buildClient(next)
	if err != nil {
		g.mu.Unlock()
		return err
	}
	oldClient, oldStop := g.client, g.stopLoop
	ctx, stop := context.WithCancel(context.Background())
	g.client, g.stopLoop, g.cfg = client, stop, next
	g.gen++
	g.mu.Unlock()

	if oldStop != nil {
		oldStop()
	}
	if oldClient != nil {
		go func() {
			fctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = oldClient.Flush(fctx) // дождаться подтверждения уже отправленного
			oldClient.Close()
		}()
		g.events.Add("info", fmt.Sprintf("Producer пересоздан: acks=%s, compression=%s, linger=%dms", next.Acks, next.Compression, next.LingerMs), "producer-config", "", 0)
	}
	go g.loop(ctx, client)
	return nil
}

var (
	pages  = []string{"/", "/catalog", "/catalog/phones", "/catalog/laptops", "/product/42", "/product/7", "/cart", "/checkout", "/profile", "/search"}
	events = []string{"page_view", "page_view", "page_view", "click", "click", "scroll", "add_to_cart", "purchase"}
)

type click struct {
	SessionID string `json:"sessionId"`
	UserID    string `json:"userId"`
	Event     string `json:"event"`
	Page      string `json:"page"`
	Seq       int64  `json:"seq"`
	Ts        int64  `json:"ts"`
	Pad       string `json:"pad,omitempty"`
}

func (g *Generator) makeRecord(cfg Config) *kgo.Record {
	session := rand.IntN(5000)
	c := click{
		SessionID: fmt.Sprintf("sess-%04d", session),
		UserID:    fmt.Sprintf("user-%04d", session%1500),
		Event:     events[rand.IntN(len(events))],
		Page:      pages[rand.IntN(len(pages))],
		Seq:       g.seq.Add(1),
		Ts:        time.Now().UnixMilli(),
	}
	value, _ := json.Marshal(c)
	if pad := cfg.SizeBytes - len(value) - 9; pad > 0 {
		c.Pad = strings.Repeat("x", pad)
		value, _ = json.Marshal(c)
	}
	rec := &kgo.Record{Value: value}
	if cfg.Keyed {
		// С ключом партиция = murmur2(key) % N — все события сессии упорядочены в одной партиции.
		rec.Key = []byte(c.SessionID)
	}
	return rec
}

func (g *Generator) loop(ctx context.Context, client *kgo.Client) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	last := time.Now()
	budget := 0.0
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			g.mu.Lock()
			cfg := g.cfg
			g.mu.Unlock()

			budget += float64(cfg.Rate) * now.Sub(last).Seconds()
			last = now
			if budget > float64(cfg.Rate) {
				budget = float64(cfg.Rate)
			}
			for budget >= 1 {
				if ctx.Err() != nil {
					return
				}
				if client.BufferedProduceRecords() >= maxBuffered {
					// Буфер producer-а полон: брокеры не успевают подтверждать (или недоступны).
					g.bufferFull.Add(1)
					g.events.Add("warn", fmt.Sprintf("Буфер producer-а заполнен (%d записей): брокеры не успевают — генератор притормаживает (backpressure)", maxBuffered), "backpressure", "buffer-full", 10*time.Second)
					budget = 0
					break
				}
				client.TryProduce(context.Background(), g.makeRecord(cfg), g.onDelivery)
				g.sent.Add(1)
				budget--
			}
		}
	}
}

func (g *Generator) onDelivery(r *kgo.Record, err error) {
	if err != nil {
		if errors.Is(err, kgo.ErrMaxBuffered) {
			g.bufferFull.Add(1)
			return
		}
		if errors.Is(err, kgo.ErrClientClosed) || errors.Is(err, context.Canceled) {
			return
		}
		g.failed.Add(1)
		g.lastErrTs.Store(time.Now().UnixMilli())
		msg := err.Error()
		if len(msg) > 120 {
			msg = msg[:120]
		}
		g.errMu.Lock()
		g.errCounts[msg]++
		g.errMu.Unlock()
		learn := ""
		switch {
		case errors.Is(err, kgo.ErrRecordTimeout):
			learn = "delivery-timeout"
		case strings.Contains(msg, "NOT_ENOUGH_REPLICAS"):
			learn = "min-isr"
		}
		g.events.Add("error", "Доставка не удалась: "+msg, learn, "err:"+msg, 5*time.Second)
		return
	}
	g.acked.Add(1)
	g.bytes.Add(int64(len(r.Value)))
	g.latency.Record(time.Since(r.Timestamp))
	if r.Partition >= 0 && r.Partition < 64 {
		g.partitions[r.Partition].Add(1)
	}
}

func (g *Generator) stats() map[string]any {
	g.mu.Lock()
	cfg, client, gen := g.cfg, g.client, g.gen
	g.mu.Unlock()

	sent, sentRate := g.sent.Snapshot()
	acked, ackedRate := g.acked.Snapshot()
	failed, failedRate := g.failed.Snapshot()
	bytesTotal, bytesRate := g.bytes.Snapshot()

	parts := map[string]int64{}
	for i := range g.partitions {
		if v := g.partitions[i].Load(); v > 0 {
			parts[fmt.Sprint(i)] = v
		}
	}
	g.errMu.Lock()
	errs := map[string]int64{}
	for k, v := range g.errCounts {
		errs[k] = v
	}
	g.errMu.Unlock()

	var buffered int64
	if client != nil {
		buffered = client.BufferedProduceRecords()
	}

	return map[string]any{
		"service":    "clickstream-generator",
		"lang":       "Go",
		"role":       "producer",
		"instanceId": hostname(),
		"uptimeSec":  int64(time.Since(g.started).Seconds()),
		"topic":      topic,
		"config":     cfg,
		"producer": map[string]any{
			"sent": sent, "acked": acked, "failed": failed, "bytes": bytesTotal,
			"inFlight": buffered, "buffered": buffered, "bufferFull": g.bufferFull.Load(), "generation": gen,
			"rates":       map[string]any{"sent": sentRate, "acked": ackedRate, "failed": failedRate, "bytes": bytesRate},
			"latencyMs":   g.latency.Snapshot(),
			"partitions":  parts,
			"errorCounts": errs,
			"lastErrorTs": g.lastErrTs.Load(),
		},
		"events": g.events.Recent(),
	}
}

func hostname() string {
	h, _ := os.Hostname()
	return h
}

// ---------------------------------------------------------------- HTTP

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func main() {
	bootstrap := os.Getenv("KAFKA_BOOTSTRAP")
	if bootstrap == "" {
		bootstrap = "localhost:19092,localhost:19093,localhost:19094"
	}
	g := newGenerator(strings.Split(bootstrap, ","))
	if err := g.apply(g.cfg); err != nil {
		log.Fatalf("kafka client: %v", err)
	}
	g.events.Add("info", "clickstream-generator запущен, bootstrap: "+bootstrap, "", "", 0)

	http.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]bool{"ok": true}) })
	http.HandleFunc("/api/stats", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, g.stats()) })
	http.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			g.mu.Lock()
			cfg := g.cfg
			g.mu.Unlock()
			writeJSON(w, 200, cfg)
			return
		}
		var p ConfigPatch
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil {
			writeJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}
		g.mu.Lock()
		next := g.cfg
		g.mu.Unlock()
		if p.Rate != nil {
			next.Rate = clamp(*p.Rate, 0, 200_000)
		}
		if p.SizeBytes != nil {
			next.SizeBytes = clamp(*p.SizeBytes, 50, 100_000)
		}
		if p.Acks != nil && (*p.Acks == "0" || *p.Acks == "1" || *p.Acks == "all") {
			next.Acks = *p.Acks
		}
		if p.Compression != nil {
			switch *p.Compression {
			case "none", "gzip", "snappy", "lz4", "zstd":
				next.Compression = *p.Compression
			}
		}
		if p.LingerMs != nil {
			next.LingerMs = clamp(*p.LingerMs, 0, 2000)
		}
		if p.Keyed != nil {
			next.Keyed = *p.Keyed
		}
		if err := g.apply(next); err != nil {
			writeJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, 200, map[string]any{"settings": next})
	})

	log.Println("clickstream-generator: HTTP на :8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
