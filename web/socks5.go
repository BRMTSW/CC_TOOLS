package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
)

// socks5DialContext connects to target addr through a SOCKS5 proxy.
// Implements RFC 1928 with no authentication (the SSH tunnel already authenticates).
func socks5DialContext(ctx context.Context, network, targetAddr string, proxyAddr string) (net.Conn, error) {
	dialer := net.Dialer{}
	conn, err := dialer.DialContext(ctx, "tcp", proxyAddr)
	if err != nil {
		return nil, fmt.Errorf("socks5: connect to proxy %s: %w", proxyAddr, err)
	}

	// Close conn on error to prevent leak
	var closeOnErr bool = true
	defer func() {
		if closeOnErr {
			conn.Close()
		}
	}()

	// Phase 1: Client hello - no auth methods
	if _, err := conn.Write([]byte{0x05, 0x01, 0x00}); err != nil {
		return nil, fmt.Errorf("socks5: write hello: %w", err)
	}

	// Phase 2: Server response
	buf := make([]byte, 2)
	if _, err := conn.Read(buf); err != nil {
		return nil, fmt.Errorf("socks5: read hello response: %w", err)
	}
	if buf[0] != 0x05 {
		return nil, fmt.Errorf("socks5: unexpected SOCKS version %d", buf[0])
	}
	if buf[1] != 0x00 {
		return nil, fmt.Errorf("socks5: server requires auth method 0x%02x", buf[1])
	}

	// Phase 3: Connect request
	host, portStr, err := net.SplitHostPort(targetAddr)
	if err != nil {
		return nil, fmt.Errorf("socks5: invalid target address %q: %w", targetAddr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil || port < 0 || port > 65535 {
		return nil, fmt.Errorf("socks5: invalid port %q", portStr)
	}

	// Try to parse as IP first
	ip := net.ParseIP(host)
	var req []byte
	if ip == nil {
		// Domain name
		if len(host) > 255 {
			return nil, fmt.Errorf("socks5: domain name too long")
		}
		req = []byte{
			0x05, // version
			0x01, // connect
			0x00, // reserved
			0x03, // address type: domain
			byte(len(host)),
		}
		req = append(req, []byte(host)...)
	} else if ip4 := ip.To4(); ip4 != nil {
		// IPv4
		req = []byte{
			0x05, 0x01, 0x00, 0x01, // version, connect, reserved, ipv4
			ip4[0], ip4[1], ip4[2], ip4[3],
		}
	} else {
		// IPv6
		req = []byte{
			0x05, 0x01, 0x00, 0x04, // version, connect, reserved, ipv6
		}
		ip6 := ip.To16()
		req = append(req, ip6[:]...)
	}

	// Port (2 bytes big-endian)
	portBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(portBytes, uint16(port))
	req = append(req, portBytes...)

	if _, err := conn.Write(req); err != nil {
		return nil, fmt.Errorf("socks5: write connect request: %w", err)
	}

	// Phase 4: Read connect response
	// Minimum 10 bytes for IPv4 response: ver(1) + rep(1) + rsv(1) + atyp(1) + addr(4) + port(2)
	resp := make([]byte, 4)
	if _, err := conn.Read(resp); err != nil {
		return nil, fmt.Errorf("socks5: read connect response: %w", err)
	}
	if resp[0] != 0x05 {
		return nil, fmt.Errorf("socks5: unexpected response version %d", resp[0])
	}
	if resp[1] != 0x00 {
		return nil, fmt.Errorf("socks5: connect failed with code %d", resp[1])
	}

	// Read bound address based on address type
	switch resp[3] {
	case 0x01: // IPv4
		discard := make([]byte, 4+2)
		if _, err := conn.Read(discard); err != nil {
			return nil, fmt.Errorf("socks5: read bound addr: %w", err)
		}
	case 0x03: // Domain
		lenBuf := make([]byte, 1)
		if _, err := conn.Read(lenBuf); err != nil {
			return nil, fmt.Errorf("socks5: read domain length: %w", err)
		}
		discard := make([]byte, int(lenBuf[0])+2)
		if _, err := conn.Read(discard); err != nil {
			return nil, fmt.Errorf("socks5: read domain addr: %w", err)
		}
	case 0x04: // IPv6
		discard := make([]byte, 16+2)
		if _, err := conn.Read(discard); err != nil {
			return nil, fmt.Errorf("socks5: read ipv6 addr: %w", err)
		}
	default:
		return nil, fmt.Errorf("socks5: unknown address type %d", resp[3])
	}

	closeOnErr = false
	return conn, nil
}
