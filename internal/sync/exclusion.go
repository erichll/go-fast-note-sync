package sync

import (
	"path"
	"strings"
)

// Rules are case-sensitive literals, not the official client's regex patterns.
func normalizeExclusionRule(rule string) string {
	return strings.TrimRight(strings.ReplaceAll(rule, "\\", "/"), "/")
}

func literalPathPrefix(rel, rule string) bool {
	return rule != "" && (rel == rule || strings.HasPrefix(rel, rule+"/"))
}

func folderRuleMatches(rel, rule string) bool {
	rule = normalizeExclusionRule(rule)
	if literalPathPrefix(rel, rule) {
		return true
	}
	if rule == "" || strings.Contains(rule, "/") {
		return false
	}
	for _, part := range strings.Split(rel, "/") {
		if part == rule {
			return true
		}
	}
	return false
}

// Temporary/junk components are hard exclusions, even inside a whitelist or
// setting scope. Internal atomic writes use .<basename>.tmp-<random> names.
func isFilesystemJunkPath(rel string) bool {
	for _, part := range strings.Split(strings.ReplaceAll(rel, "\\", "/"), "/") {
		lower := strings.ToLower(part)
		if strings.HasPrefix(part, "._") || lower == ".ds_store" ||
			strings.HasSuffix(lower, ".tmp") || strings.Contains(lower, ".tmp.") ||
			(strings.HasPrefix(part, ".") && strings.Contains(lower, ".tmp-")) {
			return true
		}
	}
	return false
}

func hasHiddenComponent(rel string) bool {
	for i, part := range strings.Split(rel, "/") {
		// .obsidian remains governed by the dedicated setting scope.
		if strings.HasPrefix(part, ".") && (i != 0 || part != obsidianConfigDir) {
			return true
		}
	}
	return false
}

func (s *SyncService) isWhitelisted(rel string) bool {
	for _, rule := range s.cfg.SyncExcludeWhitelist {
		if literalPathPrefix(rel, normalizeExclusionRule(rule)) {
			return true
		}
	}
	return false
}

func (s *SyncService) isFolderPathExcluded(rel string) bool {
	rel, err := normalizeSyncPath(rel)
	if err != nil {
		return true
	}
	if isFilesystemJunkPath(rel) || isSensitivePluginConfigPath(rel) {
		return true
	}
	if s.isWhitelisted(rel) {
		return false
	}
	if hasHiddenComponent(rel) {
		return true
	}
	for _, rule := range s.cfg.SyncExcludeFolders {
		if folderRuleMatches(rel, rule) {
			return true
		}
	}
	return false
}

func (s *SyncService) isVaultFileExcluded(rel string) bool {
	rel, err := normalizeSyncPath(rel)
	if err != nil {
		return true
	}
	if isFilesystemJunkPath(rel) || isSensitivePluginConfigPath(rel) {
		return true
	}
	if s.isWhitelisted(rel) {
		return false
	}
	if hasHiddenComponent(rel) {
		return true
	}
	if dir := path.Dir(rel); dir != "." && s.isFolderPathExcluded(dir) {
		return true
	}
	// Preserve legacy root-prefix matches without treating a nested file's
	// basename as a directory-name rule.
	for _, rule := range s.cfg.SyncExcludeFolders {
		if literalPathPrefix(rel, normalizeExclusionRule(rule)) {
			return true
		}
	}
	ext := strings.ToLower(path.Ext(rel))
	for _, rule := range s.cfg.SyncExcludeExtensions {
		if ext == "."+strings.TrimPrefix(strings.ToLower(rule), ".") {
			return true
		}
	}
	return false
}

// Traversal is not sync eligibility: excluded ancestors must remain reachable
// when they lead to a whitelist, without syncing them or unrelated siblings.
func (s *SyncService) shouldTraverseVaultDir(rel string) bool {
	if isFilesystemJunkPath(rel) || isSensitivePluginConfigPath(rel) {
		return false
	}
	if !s.isFolderPathExcluded(rel) {
		return true
	}
	for _, rule := range s.cfg.SyncExcludeWhitelist {
		if literalPathPrefix(normalizeExclusionRule(rule), rel) {
			return true
		}
	}
	return false
}
