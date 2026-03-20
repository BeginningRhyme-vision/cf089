package main

import (
	"crypto/tls"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

func main() {
	proxyURL, _ := url.Parse("http://furion2025-zone-adam:furion2025@i7g4h7p1q3e7-na.grassdata.net:2345")
	
	transport := &http.Transport{
		Proxy:             http.ProxyURL(proxyURL),
		DisableKeepAlives: true,
		ForceAttemptHTTP2: false,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true},
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   15 * time.Second,
	}

	var wg sync.WaitGroup
	success := 0
	failed := 0
	var mu sync.Mutex

	// 模拟低并发：每次只发起 2 个请求，中间休眠 500ms，总共测 10 次
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			req, _ := http.NewRequest("GET", "https://testchange.unboundfuture.ai/worker/upload-part", nil)
			resp, err := client.Do(req)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				if strings.Contains(err.Error(), "Method Not Allowed") {
					success++
				} else {
					log.Printf("[%d] Error: %v", id, err)
					failed++
				}
			} else {
				if resp.StatusCode == 405 {
					success++
				} else {
					log.Printf("[%d] Unexpected status: %d", id, resp.StatusCode)
					failed++
				}
				resp.Body.Close()
			}
		}(i)
		
		if i%2 == 1 {
			time.Sleep(500 * time.Millisecond)
		}
	}

	wg.Wait()
	fmt.Printf("Total: 10, Success: %d, Failed: %d\n", success, failed)
}
