package proxypool

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"
)

// ProxyStatus represents the state of a proxy
type ProxyStatus int

const (
	StatusOnline ProxyStatus = iota
	StatusOffline
)

// Proxy holds the state of a single proxy
type Proxy struct {
	URL       *url.URL
	Status    ProxyStatus
	FailCount int
	LastCheck time.Time
	Transport http.RoundTripper // Dedicated transport for this proxy to ensure pooling
	mu        sync.RWMutex
}

// Manager manages a pool of proxies
type Manager struct {
	proxies []*Proxy
	direct  http.RoundTripper // Fallback transport
	
	currentIndex int
	mu           sync.Mutex // Protects index and list modifications

	probeURL      string
	probeInterval time.Duration
	timeout       time.Duration

	ctx    context.Context
	cancel context.CancelFunc
}

// Config holds configuration for the Manager
type Config struct {
	ProxyURLs     []string
	ProbeURL      string // Defaults to https://www.google.com
	ProbeInterval time.Duration // Defaults to 30s
	Timeout       time.Duration // Timeout for requests and probes, defaults to 10s
}

// NewManager creates a new proxy pool manager
func NewManager(cfg Config) (*Manager, error) {
	if cfg.ProbeURL == "" {
		cfg.ProbeURL = "https://www.google.com"
	}
	if cfg.ProbeInterval == 0 {
		cfg.ProbeInterval = 30 * time.Second
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())
	
	m := &Manager{
		direct: &http.Transport{
			Proxy:                 nil, // Direct
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		},
		probeURL:      cfg.ProbeURL,
		probeInterval: cfg.ProbeInterval,
		timeout:       cfg.Timeout,
		ctx:           ctx,
		cancel:        cancel,
	}

	for _, u := range cfg.ProxyURLs {
		parsed, err := url.Parse(u)
		if err != nil {
			return nil, fmt.Errorf("invalid proxy url %s: %w", u, err)
		}
		
		// Create a dedicated transport for this proxy
		transport := &http.Transport{
			Proxy:                 http.ProxyURL(parsed),
			MaxIdleConns:          100,
			IdleConnTimeout:       90 * time.Second,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
		}

		m.proxies = append(m.proxies, &Proxy{
			URL:       parsed,
			Status:    StatusOnline, // Assume online initially, probe will correct
			Transport: transport,
		})
	}

	// Start probing
	go m.runProbe()

	return m, nil
}

// Stop stops the background probe
func (m *Manager) Stop() {
	m.cancel()
}

// RoundTrip implements http.RoundTripper
// It selects a proxy via Round Robin, or falls back to direct if all are offline.
func (m *Manager) RoundTrip(req *http.Request) (*http.Response, error) {
	proxy := m.nextProxy()

	var transport http.RoundTripper
	if proxy == nil {
		// Fallback to direct
		transport = m.direct
	} else {
		transport = proxy.Transport
	}

	// Execute request
	resp, err := transport.RoundTrip(req)

	// Error handling
	if proxy != nil {
		if err != nil {
			log.Printf("Proxy request error for %s: %v", proxy.URL, err)
			m.markFailure(proxy)
		} else if resp != nil && (resp.StatusCode < 200 || resp.StatusCode >= 400) {
			log.Printf("Proxy returned bad status %d for %s", resp.StatusCode, proxy.URL)
			m.markFailure(proxy)
		} else {
			m.markSuccess(proxy)
		}
	}

	return resp, err
}

// nextProxy selects the next available proxy using Round Robin
func (m *Manager) nextProxy() *Proxy {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.proxies) == 0 {
		return nil
	}

	// Try to find an online proxy
	// We iterate at most len(proxies) times starting from currentIndex
	start := m.currentIndex
	for i := 0; i < len(m.proxies); i++ {
		idx := (start + i) % len(m.proxies)
		p := m.proxies[idx]
		
		p.mu.RLock()
		online := p.Status == StatusOnline
		p.mu.RUnlock()

		if online {
			m.currentIndex = (idx + 1) % len(m.proxies)
			return p
		}
	}

	// None online
	return nil
}

func (m *Manager) markFailure(p *Proxy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.FailCount++
	// If it fails too many times, maybe we should mark it offline?
	// The probing logic usually handles re-enabling.
	// But if we are in a request and it fails, we should probably mark it offline immediately 
	// so next requests don't use it until probe checks it.
	if p.FailCount >= 1 {
		p.Status = StatusOffline
	}
}

func (m *Manager) markSuccess(p *Proxy) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.FailCount = 0
	p.Status = StatusOnline
}

func (m *Manager) runProbe() {
	ticker := time.NewTicker(m.probeInterval)
	defer ticker.Stop()

	// Initial probe
	m.probeAll()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C:
			m.probeAll()
		}
	}
}

func (m *Manager) probeAll() {
	var wg sync.WaitGroup
	
	// Copy slice to avoid holding lock too long (list is static currently though)
	m.mu.Lock()
	proxies := make([]*Proxy, len(m.proxies))
	copy(proxies, m.proxies)
	m.mu.Unlock()

	for _, p := range proxies {
		wg.Add(1)
		go func(proxy *Proxy) {
			defer wg.Done()
			if m.checkProxy(proxy) {
				m.markSuccess(proxy)
			} else {
				// We don't necessarily increment fail count here, just set status
				proxy.mu.Lock()
				proxy.Status = StatusOffline
				proxy.mu.Unlock()
			}
		}(p)
	}
	wg.Wait()
}

func (m *Manager) checkProxy(p *Proxy) bool {
	client := &http.Client{
		Transport: p.Transport,
		Timeout:   m.timeout,
	}

	// Using HEAD or GET
	resp, err := client.Get(m.probeURL)
	if err != nil {
		log.Printf("Proxy probe failed for %s: %v", p.URL, err)
		return false
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 200 && resp.StatusCode < 400 {
		return true
	}
	
	log.Printf("Proxy probe returned bad status for %s: %d", p.URL, resp.StatusCode)
	return false
}

// ProxySnapshot represents the state of a proxy at a point in time
type ProxySnapshot struct {
	URL       string
	Status    ProxyStatus
	FailCount int
	LastCheck time.Time
}

// Snapshot returns the current state of all proxies
func (m *Manager) Snapshot() []ProxySnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	snaps := make([]ProxySnapshot, 0, len(m.proxies))
	for _, p := range m.proxies {
		p.mu.RLock()
		snaps = append(snaps, ProxySnapshot{
			URL:       p.URL.String(),
			Status:    p.Status,
			FailCount: p.FailCount,
			LastCheck: p.LastCheck,
		})
		p.mu.RUnlock()
	}
	return snaps
}
