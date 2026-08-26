package httpapi

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
)

const adminCSRFCookie = "llmgw_csrf"

type AdminSecurityConfig struct {
	Username     string
	Password     string
	SecureCookie bool
	Random       io.Reader
}

type AdminSecurity struct {
	usernameDigest [sha256.Size]byte
	passwordDigest [sha256.Size]byte
	secureCookie   bool
	random         io.Reader
	randomMu       sync.Mutex
}

func NewAdminSecurity(config AdminSecurityConfig) (*AdminSecurity, error) {
	if strings.TrimSpace(config.Username) == "" {
		return nil, errors.New("admin username is required")
	}
	if config.Password == "" {
		return nil, errors.New("admin password is required")
	}
	if config.Random == nil {
		return nil, errors.New("CSRF random source is required")
	}
	return &AdminSecurity{
		usernameDigest: sha256.Sum256([]byte(config.Username)),
		passwordDigest: sha256.Sum256([]byte(config.Password)),
		secureCookie:   config.SecureCookie,
		random:         config.Random,
	}, nil
}

func (s *AdminSecurity) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		setAdminSecurityHeaders(writer.Header())
		username, password, ok := request.BasicAuth()
		usernameDigest := sha256.Sum256([]byte(username))
		passwordDigest := sha256.Sum256([]byte(password))
		valid := subtle.ConstantTimeCompare(usernameDigest[:], s.usernameDigest[:]) &
			subtle.ConstantTimeCompare(passwordDigest[:], s.passwordDigest[:])
		if !ok || valid != 1 {
			writer.Header().Set("WWW-Authenticate", `Basic realm="vLLM Priority Gateway", charset="UTF-8"`)
			http.Error(writer, "Unauthorized", http.StatusUnauthorized)
			return
		}

		token := csrfToken(request)
		if token == "" {
			var err error
			token, err = s.newCSRFToken()
			if err != nil {
				http.Error(writer, "Unable to initialize admin session", http.StatusServiceUnavailable)
				return
			}
			http.SetCookie(writer, &http.Cookie{
				Name: adminCSRFCookie, Value: token, Path: "/admin", HttpOnly: true,
				Secure: s.secureCookie, SameSite: http.SameSiteStrictMode,
			})
		}
		if mutating(request.Method) && !validCSRF(request, token) {
			http.Error(writer, "CSRF token mismatch", http.StatusForbidden)
			return
		}
		next.ServeHTTP(writer, request)
	})
}

func (s *AdminSecurity) newCSRFToken() (string, error) {
	random := make([]byte, 32)
	s.randomMu.Lock()
	_, err := io.ReadFull(s.random, random)
	s.randomMu.Unlock()
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(random), nil
}

func csrfToken(request *http.Request) string {
	cookie, err := request.Cookie(adminCSRFCookie)
	if err != nil {
		return ""
	}
	decoded, err := base64.RawURLEncoding.DecodeString(cookie.Value)
	if err != nil || len(decoded) != 32 {
		return ""
	}
	return cookie.Value
}

func validCSRF(request *http.Request, expected string) bool {
	presented := request.Header.Get("X-CSRF-Token")
	if presented == "" && strings.HasPrefix(request.Header.Get("Content-Type"), "application/x-www-form-urlencoded") {
		if err := request.ParseForm(); err == nil {
			presented = request.Form.Get("csrf_token")
		}
	}
	expectedDigest := sha256.Sum256([]byte(expected))
	presentedDigest := sha256.Sum256([]byte(presented))
	return expected != "" && presented != "" && subtle.ConstantTimeCompare(expectedDigest[:], presentedDigest[:]) == 1
}

func mutating(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func setAdminSecurityHeaders(header http.Header) {
	header.Set("Cache-Control", "no-store")
	header.Set("Pragma", "no-cache")
	header.Set("X-Frame-Options", "DENY")
	header.Set("X-Content-Type-Options", "nosniff")
	header.Set("Referrer-Policy", "no-referrer")
	header.Set("Content-Security-Policy", "default-src 'none'; style-src 'self'; script-src 'self'; connect-src 'self'; img-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'")
}

// AdminCSRFToken returns the validated double-submit token for HTML forms.
func AdminCSRFToken(request *http.Request) string {
	return csrfToken(request)
}
