// Copyright 2015 Diffeo, Inc.
// This software is released under an MIT/X11 open source license.

package restdata

import (
	"testing"
)

func TestEncodeDecode(t *testing.T) {
	tests := []struct{ plain, encoded string }{
		{"foo", "foo"},
		{"", "-"},
		{"-", "-LQ"},
		{"\u0000", "-AA"},
	}
	for _, test := range tests {
		enc := MaybeEncodeName(test.plain)
		if enc != test.encoded {
			t.Errorf("MaybeEncodeName(%q) => %q, want %q",
				test.plain, enc, test.encoded)
		}

		dec, err := MaybeDecodeName(test.encoded)
		if err != nil {
			t.Errorf("MaybeDecodeName(%q) => error %v",
				test.encoded, err)
		} else if dec != test.plain {
			t.Errorf("MaybeDecodeName(%q) => %q, want %q",
				test.encoded, dec, test.plain)
		}
	}
}
