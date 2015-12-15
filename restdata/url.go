// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restdata

import (
	"encoding/base64"
)

// MaybeEncodeName examines a name, and if it cannot be directly
// inserted into a URL as-is, base64 encodes it.  More specifically,
// the encoded name begins with - and uses the URL-safe base64
// alphabet with no padding.
func MaybeEncodeName(name string) string {
	// We must encode empty name, name starting with "-" (because
	// it is otherwise ambiguous), and name that includes anything
	// that's not URL-safe.
	safe := true
	if len(name) == 0 {
		safe = false
	} else if name[0] == '-' {
		safe = false
	} else {
		for _, c := range name {
			switch {
			// These characters are "unreserved"
			// in RFC 3986 section 2.3:
			case c == '-', c == '.', c == '_', c == ':',
				(c >= 'a' && c <= 'z'),
				(c >= 'A' && c <= 'Z'),
				(c >= '0' && c <= '9'):
				continue
			default:
				safe = false
				break
			}
		}
	}
	if safe {
		return name
	}
	return "-" + base64.RawURLEncoding.EncodeToString([]byte(name))
}

// MaybeDecodeName examines a name, and if it appears to be base64
// encoded, decodes it.  base64 encoded strings begin with an - sign.
// This function is the dual of MaybeEncodeName().  Returns an error
// if the string begins with - and the remainder of the string isn't
// actually base64 encoded.
func MaybeDecodeName(name string) (string, error) {
	if len(name) == 0 || name[0] != '-' {
		// Not base64 encoded, so return as is
		return name, nil
	}
	bytes, err := base64.RawURLEncoding.DecodeString(name[1:])
	if err != nil {
		return "", err
	}
	return string(bytes), nil
}
