package sync

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

// Envelope is the common server response structure for all text messages.
type Envelope struct {
	Code      int             `json:"code"`
	Message   string          `json:"message"`
	Details   string          `json:"details"`
	Data      json.RawMessage `json:"data"`
	Vault     string          `json:"vault"`
	Context   string          `json:"context"`
	PageIndex int             `json:"pageIndex"`
}

// parseTextMessage splits a raw WebSocket text frame into action and envelope.
//
// Format: "ACTION|JSON" (split on first "|") or bare "JSON" (no "|").
// When no "|" is present, action is "" and the full string is parsed as JSON.
func parseTextMessage(raw string) (action string, env Envelope, err error) {
	var jsonStr string
	if idx := strings.Index(raw, "|"); idx != -1 {
		action = raw[:idx]
		jsonStr = raw[idx+1:]
	} else {
		jsonStr = raw
	}
	err = json.Unmarshal([]byte(jsonStr), &env)
	return
}

// dispatchText processes an incoming text frame.
func (s *SyncService) dispatchText(raw string) {
	action, env, err := parseTextMessage(raw)
	if err != nil {
		log.Printf("[ws] parse message error: %v (raw=%.120q)", err, raw)
		return
	}

	// No "|": envelope-only message — parse but do not dispatch.
	if action == "" {
		return
	}

	if action == "Authorization" {
		handleAuthorization(env, s)
		return
	}
	if action == "ClientInfo" {
		handleClientInfo(env, s)
		return
	}

	if env.Code <= 0 || env.Code >= 300 {
		if env.Code == 530 {
			log.Printf("[ws] sync conflict: message=%q details=%q", env.Message, env.Details)
		} else {
			log.Printf("[ws] service error: code=%d message=%q details=%q action=%q",
				env.Code, env.Message, env.Details, action)
		}
		if isSyncRoundAction(action) {
			s.onSyncFailed(s.currentSyncRoundID(), fmt.Errorf("service %s failed: code=%d message=%s details=%s",
				action, env.Code, env.Message, env.Details))
		}
		return
	}

	// Vault mismatch: skip and log.
	if env.Vault != "" && env.Vault != s.cfg.Vault {
		log.Printf("[ws] vault mismatch: got %q, want %q, skipping %q", env.Vault, s.cfg.Vault, action)
		return
	}
	if module, ok := syncPageModule(action); ok {
		s.handleSyncPage(module, env)
		return
	}

	handler, ok := s.receiveHandlers[action]
	if !ok {
		return
	}
	data := env.Data
	if module, ok := syncWorkModule(action); ok {
		if pageIndex, found := s.resolveSyncPageIndex(module, env.PageIndex, data); found {
			data = withPageIndex(data, pageIndex)
		}
	}
	handler(data, s)
	if module, ok := syncEndModule(action); ok {
		s.startSyncPaging(module, env.Context)
	}
}

func isSyncRoundAction(action string) bool {
	switch action {
	case "NoteSync", "FileSync", "SettingSync", "FolderSync",
		"NoteSyncPage", "FileSyncPage", "SettingSyncPage", "FolderSyncPage":
		return true
	default:
		return false
	}
}

func (s *SyncService) resolveSyncPageIndex(module string, envelopePageIndex int, data json.RawMessage) (int, bool) {
	var detail struct {
		PageIndex *int `json:"pageIndex"`
	}
	_ = json.Unmarshal(data, &detail)

	s.mu.Lock()
	defer s.mu.Unlock()
	tracker := s.syncPages[module]
	if tracker == nil {
		return 0, false
	}
	if detail.PageIndex != nil && tracker.Pages[*detail.PageIndex] != nil {
		return *detail.PageIndex, true
	}
	if tracker.Pages[envelopePageIndex] != nil {
		return envelopePageIndex, true
	}
	if len(tracker.Pages) != 1 {
		return 0, false
	}
	for pageIndex := range tracker.Pages {
		return pageIndex, true
	}
	return 0, false
}

func withPageIndex(data json.RawMessage, pageIndex int) json.RawMessage {
	var payload map[string]interface{}
	if err := json.Unmarshal(data, &payload); err != nil || payload == nil {
		return data
	}
	if _, exists := payload["pageIndex"]; exists {
		return data
	}
	payload["pageIndex"] = pageIndex
	updated, err := json.Marshal(payload)
	if err != nil {
		return data
	}
	return updated
}

func syncWorkModule(action string) (string, bool) {
	switch action {
	case "NoteSyncModify", "NoteSyncDelete", "NoteSyncNeedPush", "NoteSyncMtime", "NoteSyncRename":
		return "note", true
	case "FileSyncUpdate", "FileUpload", "FileSyncDelete", "FileSyncMtime", "FileSyncRename":
		return "file", true
	case "SettingSyncModify", "SettingSyncNeedUpload", "SettingSyncMtime", "SettingSyncDelete", "SettingSyncClear":
		return "config", true
	case "FolderSyncModify", "FolderSyncDelete", "FolderSyncRename":
		return "folder", true
	default:
		return "", false
	}
}

func syncPageModule(action string) (string, bool) {
	switch action {
	case "NoteSyncPage":
		return "note", true
	case "FileSyncPage":
		return "file", true
	case "SettingSyncPage":
		return "config", true
	case "FolderSyncPage":
		return "folder", true
	default:
		return "", false
	}
}

func syncEndModule(action string) (string, bool) {
	switch action {
	case "NoteSyncEnd":
		return "note", true
	case "FileSyncEnd":
		return "file", true
	case "SettingSyncEnd":
		return "config", true
	case "FolderSyncEnd":
		return "folder", true
	default:
		return "", false
	}
}

func asyncReceiveHandler(handler func(json.RawMessage, *SyncService)) func(json.RawMessage, *SyncService) {
	return func(data json.RawMessage, s *SyncService) {
		go handler(data, s)
	}
}

// buildReceiveHandlers returns the full 32-entry receive handler map.
func buildReceiveHandlers() map[string]func(json.RawMessage, *SyncService) {
	return map[string]func(json.RawMessage, *SyncService){
		// --- Note ---
		"NoteSyncEnd":      handleNoteSyncEnd,
		"NoteSyncModify":   handleNoteSyncModify,
		"NoteSyncDelete":   handleNoteSyncDelete,
		"NoteSyncNeedPush": asyncReceiveHandler(handleNoteSyncNeedPush),
		"NoteSyncMtime":    handleNoteSyncMtime,
		"NoteSyncRename":   handleNoteSyncRename,
		"NoteModifyAck":    handleNoteModifyAck,
		"NoteRenameAck":    handleNoteRenameAck,
		"NoteDeleteAck":    handleNoteDeleteAck,

		// --- File ---
		"FileSyncEnd":           handleFileSyncEnd,
		"FileSyncUpdate":        asyncReceiveHandler(handleFileSyncUpdate),
		"FileSyncChunkDownload": handleFileSyncChunkDownload,
		"FileUpload":            asyncReceiveHandler(handleFileUpload),
		"FileSyncDelete":        handleFileSyncDelete,
		"FileSyncMtime":         handleFileSyncMtime,
		"FileSyncRename":        handleFileSyncRename,
		"FileUploadAck":         handleFileUploadAck,
		"FileRenameAck":         handleFileRenameAck,
		"FileDeleteAck":         handleFileDeleteAck,

		// --- Setting ---
		"SettingSyncEnd":        handleSettingSyncEnd,
		"SettingSyncModify":     handleSettingSyncModify,
		"SettingSyncNeedUpload": asyncReceiveHandler(handleSettingSyncNeedUpload),
		"SettingSyncMtime":      handleSettingSyncMtime,
		"SettingSyncDelete":     handleSettingSyncDelete,
		"SettingSyncClear":      handleSettingSyncClear,
		"SettingModifyAck":      handleSettingModifyAck,
		"SettingDeleteAck":      handleSettingDeleteAck,

		// --- Folder ---
		"FolderSyncEnd":    handleFolderSyncEnd,
		"FolderSyncModify": handleFolderSyncModify,
		"FolderSyncDelete": handleFolderSyncDelete,
		"FolderSyncRename": handleFolderSyncRename,

		// --- Other ---
		"ShareSyncRefresh": handleShareSyncRefresh,
	}
}

// handleShareSyncRefresh is a no-op in M1.2; the server notifies the client to refresh shares.
func handleShareSyncRefresh(_ json.RawMessage, _ *SyncService) {
	log.Printf("[handler] ShareSyncRefresh (no-op)")
}
