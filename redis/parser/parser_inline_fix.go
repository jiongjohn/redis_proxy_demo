package parser

import (
	"strings"
)

// parseInlineCommand parses inline Redis commands (like "GET key" or "SET key value")
// This handles Redis-style inline commands including proper quote handling
func parseInlineCommand(line []byte) [][]byte {
	// Convert to string for easier processing
	command := strings.TrimSpace(string(line))

	// If the command is empty after trimming, return empty slice
	if command == "" {
		return [][]byte{}
	}

	// Parse command with quote support (similar to shell parsing)
	parts := parseQuotedArgs(command)

	// Convert back to [][]byte
	args := make([][]byte, len(parts))
	for i, part := range parts {
		args[i] = []byte(part)
	}

	return args
}

// parseQuotedArgs parses a command line with quote support
// Handles both single quotes ('value') and double quotes ("value")
func parseQuotedArgs(input string) []string {
	var args []string
	var current strings.Builder
	inQuote := false
	quoteChar := byte(0)

	i := 0
	for i < len(input) {
		char := input[i]

		switch char {
		case '\\':
			// Handle escape sequences
			if i+1 < len(input) {
				nextChar := input[i+1]
				if inQuote {
					// Inside quotes, handle specific escape sequences
					switch nextChar {
					case '\'':
						if quoteChar == '\'' {
							// Escaped single quote inside single quotes
							current.WriteByte('\'')
							i++ // Skip the next character
						} else {
							// Backslash + single quote in double quotes, keep both
							current.WriteByte(char)
						}
					case '"':
						if quoteChar == '"' {
							// Escaped double quote inside double quotes
							current.WriteByte('"')
							i++ // Skip the next character
						} else {
							// Backslash + double quote in single quotes, keep both
							current.WriteByte(char)
						}
					case '\\':
						// Escaped backslash
						current.WriteByte('\\')
						i++ // Skip the next character
					default:
						// Other escape sequences, keep the backslash
						current.WriteByte(char)
					}
				} else {
					// Outside quotes, keep backslash as-is
					current.WriteByte(char)
				}
			} else {
				// Backslash at end of string
				current.WriteByte(char)
			}

		case '\'', '"':
			if !inQuote {
				// Start of quoted string
				inQuote = true
				quoteChar = char
			} else if char == quoteChar {
				// End of quoted string
				inQuote = false
				quoteChar = 0
			} else {
				// Quote character inside different quotes
				current.WriteByte(char)
			}

		case ' ', '\t':
			if inQuote {
				// Space inside quotes, add to current arg
				current.WriteByte(char)
			} else {
				// Space outside quotes, end current arg
				if current.Len() > 0 {
					args = append(args, current.String())
					current.Reset()
				}
				// Skip multiple spaces
				for i+1 < len(input) && (input[i+1] == ' ' || input[i+1] == '\t') {
					i++
				}
			}

		default:
			current.WriteByte(char)
		}

		i++
	}

	// Add final argument if exists
	if current.Len() > 0 {
		args = append(args, current.String())
	}

	return args
}
