package parser

import (
	"strings"
)

// parseInlineCommand parses inline Redis commands (like "GET key" or "SET key value")
// This handles Redis-style inline commands that are more forgiving than strict RESP protocol
func parseInlineCommand(line []byte) [][]byte {
	// Convert to string for easier processing
	command := string(line)

	// Trim leading and trailing whitespace
	command = strings.TrimSpace(command)

	// If the command is empty after trimming, return empty slice
	if command == "" {
		return [][]byte{}
	}

	// Split by spaces and filter out empty strings
	parts := strings.Fields(command) // Fields automatically handles multiple spaces and trims

	// Convert back to [][]byte
	args := make([][]byte, len(parts))
	for i, part := range parts {
		args[i] = []byte(part)
	}

	return args
}
