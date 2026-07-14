package main

import "testing"

func TestBuildRelativeKey(t *testing.T) {
	tests := []struct {
		name   string
		srcDir string
		srcKey string
		want   string
	}{
		{
			name:   "empty src dir returns src key",
			srcDir: "",
			srcKey: "foo/bar/baz.mp4",
			want:   "foo/bar/baz.mp4",
		},
		{
			name:   "exact directory match returns empty key",
			srcDir: "foo/bar",
			srcKey: "foo/bar",
			want:   "",
		},
		{
			name:   "child path trims strict directory prefix",
			srcDir: "foo/bar",
			srcKey: "foo/bar/baz.mp4",
			want:   "baz.mp4",
		},
		{
			name:   "non child path is left untouched",
			srcDir: "foo/bar",
			srcKey: "foo/bar2/baz.mp4",
			want:   "foo/bar2/baz.mp4",
		},
		{
			name:   "leading and trailing slashes are normalized",
			srcDir: "/foo/bar/",
			srcKey: "/foo/bar/baz.mp4",
			want:   "baz.mp4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildRelativeKey(tt.srcDir, tt.srcKey)
			if got != tt.want {
				t.Fatalf("buildRelativeKey(%q, %q) = %q, want %q", tt.srcDir, tt.srcKey, got, tt.want)
			}
		})
	}
}
