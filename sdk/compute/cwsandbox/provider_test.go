package cwsandbox

import (
	"reflect"
	"testing"
)

func TestSplitArgs(t *testing.T) {
	tests := map[string]struct {
		in   string
		want []string
	}{
		"empty":      {in: "", want: nil},
		"whitespace": {in: "  \t ", want: nil},
		"simple":     {in: "python worker.py", want: []string{"python", "worker.py"}},
		"json":       {in: `["python", "-m", "worker"]`, want: []string{"python", "-m", "worker"}},
		"json-with-space": {
			in:   `["bash", "-c", "echo hello world"]`,
			want: []string{"bash", "-c", "echo hello world"},
		},
		"json-malformed-falls-back": {in: `["unterminated`, want: []string{`["unterminated`}},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			got := splitArgs(tc.in)
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("splitArgs(%q) = %#v, want %#v", tc.in, got, tc.want)
			}
		})
	}
}

func TestNewAuthedRequest(t *testing.T) {
	req := newAuthedRequest("test-token", &struct{}{})
	if got := req.Header().Get("Authorization"); got != "Bearer test-token" {
		t.Fatalf("Authorization header = %q, want %q", got, "Bearer test-token")
	}
}
