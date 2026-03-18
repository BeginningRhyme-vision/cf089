package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

func main() {
	proxyAddr := "i7g4h7p1q3e7-na.grassdata.net:2345"
	proxyUser := "furion2025-zone-adam:furion2025"
	auth := "Basic " + base64.StdEncoding.EncodeToString([]byte(proxyUser))

	dialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
		// 1. Connect to proxy
		conn, err := net.DialTimeout("tcp", proxyAddr, 10*time.Second)
		if err != nil {
			return nil, err
		}

		// 2. Send CONNECT request
		req := fmt.Sprintf("CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: %s\r\n\r\n", addr, addr, auth)
		_, err = conn.Write([]byte(req))
		if err != nil {
			conn.Close()
			return nil, err
		}

		// 3. Read response
		reader := bufio.NewReader(conn)
		resp, err := http.ReadResponse(reader, nil)
		if err != nil {
			conn.Close()
			return nil, fmt.Errorf("read proxy response failed: %v", err)
		}
		if resp.StatusCode != 200 {
			body, _ := ioutil.ReadAll(resp.Body)
			conn.Close()
			return nil, fmt.Errorf("proxy refused connection: %s, body: %s", resp.Status, string(body))
		}

		// 关键：Python 不会去管剩下的 buffer，Go 的 http 包有时会读多。
		// 这里我们返回裸的 conn 给上层 TLS 握手
		return conn, nil
	}

	transport := &http.Transport{
		DialContext:       dialer,
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

	// 模拟并发 20 次请求
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			req, _ := http.NewRequest("GET", "https://r2-worker.cf022.workers.dev/upload-part", nil)
			resp, err := client.Do(req)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				// We expect 405 Method Not Allowed from our worker if connection succeeds
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
	}

	wg.Wait()
	fmt.Printf("Total: 20, Success: %d, Failed: %d\n", success, failed)
}
