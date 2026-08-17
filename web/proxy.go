package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"net"
	"os/exec"
	"sync"
	"time"
)

// SSHProxy defines a remote SSH server used as an HTTP proxy
type SSHProxy struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Host     string `json:"host"`
	Port     int    `json:"port"`
	Username string `json:"username"`
	Password string `json:"password,omitempty"` // masked in list responses
	HasPass  bool   `json:"hasPass"`            // indicates password is set
}

// MaskPassword returns a copy with password masked
func (p *SSHProxy) MaskPassword() SSHProxy {
	cp := *p
	if cp.Password != "" {
		cp.Password = "****"
		cp.HasPass = true
	}
	return cp
}

// activeProxy tracks a running SSH SOCKS5 process
type activeProxy struct {
	proxyID   string
	localPort int
	cmd       *exec.Cmd
	cancel    context.CancelFunc
}

// ProxyManager manages SSH proxy definitions and active SSH processes
type ProxyManager struct {
	mu      sync.RWMutex
	proxies map[string]*SSHProxy
	active  map[string]*activeProxy
}

// NewProxyManager creates a ProxyManager
func NewProxyManager() *ProxyManager {
	return &ProxyManager{
		proxies: make(map[string]*SSHProxy),
		active:  make(map[string]*activeProxy),
	}
}

// AddProxy creates and stores a new SSH proxy config
func (pm *ProxyManager) AddProxy(p *SSHProxy) (*SSHProxy, error) {
	if p.Host == "" {
		return nil, fmt.Errorf("host 不能为空")
	}
	if p.Username == "" {
		return nil, fmt.Errorf("username 不能为空")
	}
	if p.Port == 0 {
		p.Port = 22
	}
	if p.Name == "" {
		p.Name = p.Username + "@" + p.Host
	}
	b := make([]byte, 4)
	rand.Read(b)
	p.ID = fmt.Sprintf("%x", b)
	p.HasPass = p.Password != ""

	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.proxies[p.ID] = p
	return p, nil
}

// UpdateProxy updates an existing proxy config
func (pm *ProxyManager) UpdateProxy(id string, p *SSHProxy) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	existing, ok := pm.proxies[id]
	if !ok {
		return fmt.Errorf("proxy not found")
	}
	if p.Name != "" {
		existing.Name = p.Name
	}
	if p.Host != "" {
		existing.Host = p.Host
	}
	if p.Port != 0 {
		existing.Port = p.Port
	}
	if p.Username != "" {
		existing.Username = p.Username
	}
	if p.Password != "" && p.Password != "****" {
		existing.Password = p.Password
		existing.HasPass = true
	}
	return nil
}

// DeleteProxy removes a proxy config and stops any active connection
func (pm *ProxyManager) DeleteProxy(id string) error {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if ap, ok := pm.active[id]; ok {
		ap.cancel()
		ap.cmd.Process.Kill()
		ap.cmd.Wait()
		delete(pm.active, id)
	}
	delete(pm.proxies, id)
	return nil
}

// GetProxy returns a proxy config by ID
func (pm *ProxyManager) GetProxy(id string) (*SSHProxy, bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	p, ok := pm.proxies[id]
	return p, ok
}

// ListProxies returns all proxy configs with passwords masked
func (pm *ProxyManager) ListProxies() []SSHProxy {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	result := make([]SSHProxy, 0, len(pm.proxies))
	for _, p := range pm.proxies {
		result = append(result, p.MaskPassword())
	}
	return result
}

// StartProxy launches an SSH SOCKS5 proxy, returns the local port
func (pm *ProxyManager) StartProxy(proxyID string) (int, error) {
	pm.mu.RLock()
	p, ok := pm.proxies[proxyID]
	if !ok {
		pm.mu.RUnlock()
		return 0, fmt.Errorf("proxy %s not found", proxyID)
	}
	pm.mu.RUnlock()

	// Check if already running
	pm.mu.RLock()
	if ap, ok := pm.active[proxyID]; ok {
		localPort := ap.localPort
		pm.mu.RUnlock()
		return localPort, nil
	}
	pm.mu.RUnlock()

	// Find a free local port
	localPort, err := getFreePort()
	if err != nil {
		return 0, fmt.Errorf("no free port: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	// sshpass -e ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -D LOCAL_PORT -N user@host -p SSH_PORT
	cmd := exec.CommandContext(ctx,
		"sshpass", "-e",
		"ssh",
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "ConnectTimeout=10",
		"-D", fmt.Sprintf("127.0.0.1:%d", localPort),
		"-N",
		fmt.Sprintf("%s@%s", p.Username, p.Host),
		"-p", fmt.Sprintf("%d", p.Port),
	)
	cmd.Env = append(cmd.Env, fmt.Sprintf("SSHPASS=%s", p.Password))

	// Start SSH process
	if err := cmd.Start(); err != nil {
		cancel()
		return 0, fmt.Errorf("ssh start failed: %w", err)
	}

	// Wait for SOCKS5 port to be reachable
	if err := waitForPort(ctx, "127.0.0.1", localPort, 10*time.Second); err != nil {
		cmd.Process.Kill()
		cmd.Wait()
		cancel()
		return 0, fmt.Errorf("socks5 port not ready: %w", err)
	}

	pm.mu.Lock()
	pm.active[proxyID] = &activeProxy{
		proxyID:   proxyID,
		localPort: localPort,
		cmd:       cmd,
		cancel:    cancel,
	}
	pm.mu.Unlock()

	// Monitor process exit
	go func() {
		err := cmd.Wait()
		pm.mu.Lock()
		delete(pm.active, proxyID)
		pm.mu.Unlock()
		if err != nil && ctx.Err() == nil {
			log.Printf("SSH proxy %s exited unexpectedly: %v", proxyID, err)
		}
	}()

	return localPort, nil
}

// StopProxy kills the SSH process for a given proxy
func (pm *ProxyManager) StopProxy(proxyID string) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	if ap, ok := pm.active[proxyID]; ok {
		ap.cancel()
		ap.cmd.Process.Kill()
		ap.cmd.Wait()
		delete(pm.active, proxyID)
	}
}

// StopAll kills all active SSH processes
func (pm *ProxyManager) StopAll() {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for id, ap := range pm.active {
		ap.cancel()
		ap.cmd.Process.Kill()
		ap.cmd.Wait()
		delete(pm.active, id)
	}
}

// IsProxyActive checks if a proxy SSH process is running
func (pm *ProxyManager) IsProxyActive(proxyID string) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	_, ok := pm.active[proxyID]
	return ok
}

// TestProxy tries to establish an SSH connection and reports success/failure
func (pm *ProxyManager) TestProxy(proxyID string) error {
	pm.mu.RLock()
	p, ok := pm.proxies[proxyID]
	if !ok {
		pm.mu.RUnlock()
		return fmt.Errorf("proxy not found")
	}
	pm.mu.RUnlock()

	localPort, err := getFreePort()
	if err != nil {
		return fmt.Errorf("no free port: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx,
		"sshpass", "-e",
		"ssh",
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		"-o", "ConnectTimeout=10",
		"-D", fmt.Sprintf("127.0.0.1:%d", localPort),
		"-N",
		fmt.Sprintf("%s@%s", p.Username, p.Host),
		"-p", fmt.Sprintf("%d", p.Port),
	)
	cmd.Env = append(cmd.Env, fmt.Sprintf("SSHPASS=%s", p.Password))

	if err := cmd.Start(); err != nil {
		return fmt.Errorf("ssh connect failed: %w", err)
	}

	// Wait for SOCKS port
	err = waitForPort(ctx, "127.0.0.1", localPort, 10*time.Second)
	cmd.Process.Kill()
	cmd.Wait()
	return err
}

// getFreePort returns a free TCP port on localhost
func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	ln, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()
	return port, nil
}

// waitForPort repeatedly tries to connect to addr:port until success or timeout
func waitForPort(ctx context.Context, host string, port int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	addr := fmt.Sprintf("%s:%d", host, port)
	for time.Now().Before(deadline) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
		if err == nil {
			conn.Close()
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return fmt.Errorf("timeout waiting for %s", addr)
}
