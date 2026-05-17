package sync

import (
	"encoding/json"
	"log"
	"strings"
)

// Envelope is the common server response structure for all text messages.
type Envelope struct {
	Code    int             `json:"code"`
	Message string          `json:"message"`
	Details string          `json:"details"`
	Data    json.RawMessage `json:"data"`
	Vault   string          `json:"vault"`
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
		return
	}

	// Vault mismatch: skip and log.
	if env.Vault != "" && env.Vault != s.cfg.Vault {
		log.Printf("[ws] vault mismatch: got %q, want %q, skipping %q", env.Vault, s.cfg.Vault, action)
		return
	}

	handler, ok := s.receiveHandlers[action]
	if !ok {
		return
	}
	handler(env.Data, s)
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
