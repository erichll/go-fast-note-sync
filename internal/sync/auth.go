package sync

import (
	"log"
	"os"
)

// handleAuthorization processes the Authorization response.
// Success range: 1 ≤ code ≤ 299. Failure: code ≤ 0 || code ≥ 300 → log, no reconnect.
func handleAuthorization(env Envelope, s *SyncService) {
	if env.Code <= 0 || env.Code >= 300 {
		log.Printf("[auth] authorization failed: code=%d message=%q details=%q",
			env.Code, env.Message, env.Details)
		return
	}
	s.mu.Lock()
	s.isAuth = true
	s.mu.Unlock()
	log.Printf("[auth] authorization successful (code=%d)", env.Code)
	sendClientInfo(s)
}

// sendClientInfo sends the ClientInfo message with Linux platform identifiers.
func sendClientInfo(s *SyncService) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	clientType := s.cfg.ClientType
	if clientType == "" {
		clientType = "LinuxCLI"
	}
	if err := s.Send("ClientInfo", map[string]interface{}{
		"name":                hostname,
		"version":             s.version,
		"type":                clientType,
		"isDesktop":           false,
		"isMobile":            false,
		"isPhone":             false,
		"isTablet":            false,
		"isMacOS":             false,
		"isWin":               false,
		"isLinux":             true,
		"offlineSyncStrategy": s.cfg.OfflineSyncStrategy,
	}); err != nil {
		log.Printf("[auth] failed to send ClientInfo: %v", err)
	}
}

// handleClientInfo processes the ClientInfo response and triggers startup sync on success.
func handleClientInfo(env Envelope, s *SyncService) {
	if env.Code <= 0 || env.Code >= 300 {
		log.Printf("[auth] ClientInfo error: code=%d message=%q", env.Code, env.Message)
		return
	}
	log.Printf("[auth] ClientInfo acknowledged (code=%d)", env.Code)
	s.mu.Lock()
	isLoadLastTime := s.st.IsInitSync
	s.mu.Unlock()
	s.handleSync(isLoadLastTime)
}
