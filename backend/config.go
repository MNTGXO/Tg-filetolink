package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// maxPoolTokens caps the number of bot clients so RAM stays under Koyeb's 512 MB.
const maxPoolTokens = 15

// Config holds everything loaded from environment variables.
type Config struct {
	APIID        int
	APIHash      string
	Tokens       []string // Tokens[0] is the primary bot (runs the dispatcher)
	BinChannel   int64    // raw id as given, e.g. -1001234567890
	BinChannelID int64    // bare channel id, e.g. 1234567890
	WorkerURL    string   // public Cloudflare Worker URL, no trailing slash
	HMACSecret   string   // signs download links (shared with the Worker)
	WorkerSecret string   // value of the X-Worker-Secret header (shared with the Worker)
	Port         string
	MetaTTL      time.Duration
	MetaMax      int
	AllowedUsers map[int64]struct{} // empty = everyone allowed
	Admins       map[int64]struct{} // may use /fsub, /broadcast, /stats; bypass fsub
	MongoURI     string
	MongoDB      string
}

// LoadConfig reads and validates the environment.
//
// Required: API_ID, API_HASH, BOT_TOKEN, BIN_CHANNEL, WORKER_URL, HMAC_SECRET, WORKER_SECRET, MONGO_URI
// Also: ADMINS (comma-separated user ids), MONGO_DB (default tgstream)
// Optional: MULTI_TOKENS (comma-separated extra bot tokens), ALLOWED_USERS (comma-separated
// user ids), PORT (default 8000), META_TTL_MINUTES (default 30), META_CACHE_MAX (default 2000).
func LoadConfig() (*Config, error) {
	var missing []string
	get := func(key string) string {
		v := strings.TrimSpace(os.Getenv(key))
		if v == "" {
			missing = append(missing, key)
		}
		return v
	}

	apiIDStr := get("API_ID")
	apiHash := get("API_HASH")
	primary := get("BOT_TOKEN")
	binStr := get("BIN_CHANNEL")
	workerURL := get("WORKER_URL")
	hmacSecret := get("HMAC_SECRET")
	workerSecret := get("WORKER_SECRET")
	mongoURI := get("MONGO_URI")
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}

	apiID, err := strconv.Atoi(apiIDStr)
	if err != nil {
		return nil, fmt.Errorf("API_ID must be an integer: %w", err)
	}

	bin, err := strconv.ParseInt(binStr, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("BIN_CHANNEL must be an integer like -1001234567890: %w", err)
	}
	if bin > -1000000000000 {
		return nil, fmt.Errorf("BIN_CHANNEL must be a channel/supergroup id starting with -100")
	}

	// Primary token first, then MULTI_TOKENS, de-duplicated and capped.
	seen := map[string]struct{}{}
	var tokens []string
	add := func(t string) {
		t = strings.TrimSpace(t)
		if t == "" {
			return
		}
		if _, dup := seen[t]; dup {
			return
		}
		if len(tokens) >= maxPoolTokens {
			return
		}
		seen[t] = struct{}{}
		tokens = append(tokens, t)
	}
	add(primary)
	for _, t := range strings.Split(os.Getenv("MULTI_TOKENS"), ",") {
		add(t)
	}

	allowed := map[int64]struct{}{}
	for _, s := range strings.Split(os.Getenv("ALLOWED_USERS"), ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		id, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("ALLOWED_USERS contains a non-numeric id %q", s)
		}
		allowed[id] = struct{}{}
	}

	admins := map[int64]struct{}{}
	for _, s := range strings.Split(os.Getenv("ADMINS"), ",") {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		id, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("ADMINS contains a non-numeric id %q", s)
		}
		admins[id] = struct{}{}
	}

	mongoDB := strings.TrimSpace(os.Getenv("MONGO_DB"))
	if mongoDB == "" {
		mongoDB = "tgstream"
	}

	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8000"
	}

	ttlMin := 30
	if v := strings.TrimSpace(os.Getenv("META_TTL_MINUTES")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("META_TTL_MINUTES must be a positive integer")
		}
		ttlMin = n
	}
	metaMax := 2000
	if v := strings.TrimSpace(os.Getenv("META_CACHE_MAX")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			return nil, fmt.Errorf("META_CACHE_MAX must be a positive integer")
		}
		metaMax = n
	}

	return &Config{
		APIID:        apiID,
		APIHash:      apiHash,
		Tokens:       tokens,
		BinChannel:   bin,
		BinChannelID: -bin - 1000000000000,
		WorkerURL:    strings.TrimRight(workerURL, "/"),
		HMACSecret:   hmacSecret,
		WorkerSecret: workerSecret,
		Port:         port,
		MetaTTL:      time.Duration(ttlMin) * time.Minute,
		MetaMax:      metaMax,
		AllowedUsers: allowed,
		Admins:       admins,
		MongoURI:     mongoURI,
		MongoDB:      mongoDB,
	}, nil
}
