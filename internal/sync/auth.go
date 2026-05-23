package sync

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/erichll/go-fast-note-sync/internal/config"
)

var authErrorFallbackMessages = map[int]string{
	307: "Authorization token is missing",
	308: "Session expired or token has been revoked",
	309: "Authorization token is invalid or incomplete",
	310: "Authorization token has expired",
	312: "Authorization token is restricted by IP",
	313: "Authorization token is restricted by user agent",
	314: "Authorization token is restricted by client",
	315: "Authorization token scope is restricted",
}

var authErrorReimportCodes = map[int]struct{}{
	307: {},
	308: {},
	309: {},
	310: {},
}

// handleAuthorization processes the Authorization response.
// Success range: 1 ≤ code ≤ 299. Failure: code ≤ 0 || code ≥ 300 → log, no reconnect.
func handleAuthorization(env Envelope, s *SyncService) {
	if env.Code <= 0 || env.Code >= 300 {
		log.Printf("[auth] %s", formatAuthorizationError(env, s.cfg.APIToken))
		return
	}
	s.mu.Lock()
	s.isAuth = true
	s.mu.Unlock()
	log.Printf("[auth] authorization successful (code=%d)", env.Code)
	sendClientInfo(s)
}

func formatAuthorizationError(env Envelope, token string) string {
	message := normalizeAuthNoticeValue(env.Message)
	if message == "" {
		message = authErrorFallbackMessages[env.Code]
	}
	if message == "" {
		message = "Authorization failed"
	}
	details := normalizeAuthNoticeValue(env.Details)
	message = redactAuthToken(message, token)
	details = redactAuthToken(details, token)

	detailsText := ""
	if details != "" {
		detailsText = " Details=" + details
	}

	authFailureText := strings.ToLower(message + " " + details)
	_, codeNeedsHint := authErrorReimportCodes[env.Code]
	needsReimportHint := codeNeedsHint ||
		strings.Contains(authFailureText, "rotated") ||
		strings.Contains(authFailureText, "revoked") ||
		strings.Contains(authFailureText, "no longer exists") ||
		strings.Contains(authFailureText, "missing")
	hint := ""
	if needsReimportHint {
		hint = " Hint=Please re-import the API configuration from the management console."
	}
	return fmt.Sprintf("Service Authorization Error: Code=%d Msg=%s%s%s", env.Code, message, detailsText, hint)
}

func normalizeAuthNoticeValue(value string) string {
	text := strings.TrimSpace(value)
	if text == "undefined" {
		return ""
	}
	return text
}

func redactAuthToken(text, token string) string {
	if text == "" || token == "" {
		return text
	}
	return strings.ReplaceAll(text, token, "[redacted]")
}

// sendClientInfo sends the ClientInfo message with Linux platform identifiers.
func sendClientInfo(s *SyncService) {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	clientType := s.cfg.ClientType
	if clientType == "" {
		clientType = config.DefaultClientType
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
