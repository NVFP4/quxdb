package main

import (
	"encoding/json"
	"fmt"
	mrand "math/rand/v2"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/brianvoe/gofakeit/v7"
	"github.com/google/uuid"
	"github.com/oklog/ulid/v2"
)

type keyMode string

const (
	keyModeUUID4      keyMode = "uuid4"
	keyModeUUID7      keyMode = "uuid7"
	keyModeULID       keyMode = "ulid"
	keyModeSequential keyMode = "seq"
	keyModeISO        keyMode = "ts"
	keyModeMixed      keyMode = "mixed"
)

var mixedKeyModes = [...]keyMode{
	keyModeSequential,
	keyModeISO,
	keyModeUUID4,
	keyModeUUID7,
	keyModeULID,
}

// isoKeyLayout is RFC3339 with a fixed-width nanosecond field so keys sort lexically in time order.
const isoKeyLayout = "2006-01-02T15:04:05.000000000Z"

type keyGenerator struct {
	mode      keyMode
	prefix    bool
	max       uint64
	next      atomic.Uint64
	mixedNext atomic.Uint64
	lastISO   atomic.Int64
}

type generatedKey struct {
	value       string
	verifyValue bool
}

func newKeyGenerator(mode keyMode, max uint64, prefix bool) *keyGenerator {
	return &keyGenerator{
		mode:   mode,
		prefix: prefix,
		max:    max,
	}
}

func (g *keyGenerator) Next() generatedKey {
	mode := g.mode
	if mode == keyModeMixed {
		mode = mixedKeyModes[(g.mixedNext.Add(1)-1)%uint64(len(mixedKeyModes))]
	}

	var key string
	switch mode {
	case keyModeUUID4:
		key = uuid.NewString()
	case keyModeUUID7:
		key = uuid.Must(uuid.NewV7()).String()
	case keyModeULID:
		key = ulid.Make().String()
	case keyModeSequential:
		key = g.nextSequentialKey()
	case keyModeISO:
		key = g.nextISOKey()
	}
	if g.prefix {
		key = string(mode) + "/" + key
	}
	return generatedKey{
		value:       key,
		verifyValue: mode != keyModeSequential || g.max == 0,
	}
}

// nextISOKey returns a strictly increasing UTC timestamp: when the clock has not advanced
// past the previous key (coarse clock or concurrent callers), it bumps by one nanosecond.
func (g *keyGenerator) nextISOKey() string {
	now := time.Now().UnixNano()
	for {
		last := g.lastISO.Load()
		next := max(now, last+1)
		if g.lastISO.CompareAndSwap(last, next) {
			return time.Unix(0, next).UTC().Format(isoKeyLayout)
		}
	}
}

func (g *keyGenerator) nextSequentialKey() string {
	if g.max == 0 {
		return fmt.Sprintf("%010d", g.next.Add(1))
	}
	for {
		current := g.next.Load()
		next := current + 1
		if current >= g.max {
			next = 1
		}
		if g.next.CompareAndSwap(current, next) {
			return fmt.Sprintf("%010d", next)
		}
	}
}

type jsonPayload struct {
	ID   string    `json:"id"`
	TS   string    `json:"ts"`
	IP   string    `json:"ip"`
	UA   string    `json:"ua"`
	User string    `json:"user"`
	Loc  []float64 `json:"loc"`
	Bio  string    `json:"bio"`
}

type payloadPool struct {
	values []string
}

func payloadPoolSize(cfg *runConfig) int {
	if cfg.requests > 0 && cfg.requests < uint64(PayloadPoolSize) {
		return int(cfg.requests)
	}
	return PayloadPoolSize
}

func newPayloadPool(size int) *payloadPool {
	if size <= 0 {
		size = 1
	}

	values := make([]string, size)
	generator := newValGenerator(nil, false)
	for i := range values {
		values[i] = generator.newJSON()
	}
	return &payloadPool{values: values}
}

func (p *payloadPool) Get() string {
	return p.values[mrand.IntN(len(p.values))]
}

type valGenerator struct {
	mu       sync.Mutex
	faker    *gofakeit.Faker
	payloads *payloadPool
	tsValue  bool
}

func newValGenerator(payloads *payloadPool, tsValue bool) *valGenerator {
	return &valGenerator{
		faker:    gofakeit.New(0),
		payloads: payloads,
		tsValue:  tsValue,
	}
}

func (g *valGenerator) IP() string {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.ipLocked()
}

func (g *valGenerator) ipLocked() string {
	if g.faker.Bool() {
		return g.faker.IPv4Address()
	}
	return g.faker.IPv6Address()
}

func (g *valGenerator) Value() string {
	if g.tsValue {
		return strconv.FormatInt(time.Now().UTC().UnixNano(), 10)
	}
	return g.payloads.Get()
}

func (g *valGenerator) ContentType() string {
	if g.tsValue {
		return "text/plain"
	}
	return "application/json"
}

func (g *valGenerator) newJSON() string {
	g.mu.Lock()
	defer g.mu.Unlock()

	payload := jsonPayload{
		ID:   g.faker.ID(),
		TS:   time.Now().UTC().Format(time.RFC3339Nano),
		IP:   g.ipLocked(),
		UA:   g.faker.UserAgent(),
		User: g.faker.Email(),
		Loc: []float64{
			g.faker.Latitude(),
			g.faker.Longitude(),
		},
		Bio: g.faker.LoremIpsumParagraph(g.faker.IntRange(1, 2), g.faker.IntRange(1, 3), g.faker.IntRange(7, 14), " "),
	}

	b, err := json.Marshal(payload)
	if err != nil {
		return "{}"
	}
	return string(b)
}
