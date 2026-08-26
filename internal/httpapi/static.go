package httpapi

import (
	"bytes"
	"io/fs"
	"net/http"
	"os"
	"path"
	"strings"
)

// WithSPA serves built frontend assets while reserving management API paths.
// Unknown browser routes fall back to index.html for client-side routing.
func WithSPA(api http.Handler, directory string) http.Handler {
	root := os.DirFS(directory)
	files := http.FileServer(http.Dir(directory))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" || r.URL.Path == "/api" || strings.HasPrefix(r.URL.Path, "/api/") {
			api.ServeHTTP(w, r)
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			api.ServeHTTP(w, r)
			return
		}
		cleaned := path.Clean("/" + r.URL.Path)
		relative := strings.TrimPrefix(cleaned, "/")
		if relative == "" || relative == "." {
			relative = "index.html"
		}
		info, err := fs.Stat(root, relative)
		if err != nil || info.IsDir() {
			relative = "index.html"
			info, err = fs.Stat(root, relative)
			if err != nil || info.IsDir() {
				http.Error(w, "frontend assets unavailable", http.StatusServiceUnavailable)
				return
			}
		}
		request := r.Clone(r.Context())
		request.URL.Path = "/" + relative
		if relative == "index.html" {
			w.Header().Set("Cache-Control", "no-cache")
			contents, readErr := fs.ReadFile(root, relative)
			if readErr != nil {
				http.Error(w, "frontend assets unavailable", http.StatusServiceUnavailable)
				return
			}
			http.ServeContent(w, r, relative, info.ModTime(), bytes.NewReader(contents))
			return
		} else {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		}
		files.ServeHTTP(w, request)
	})
}
