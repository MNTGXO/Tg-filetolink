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
	LinkTTL      time.Duration // how long signed links stay valid
	StartVideo   string        // t.me link of the post whose video is shown on /start
}

// LoadConfig reads and validates the environment.
//
// Required: API_ID, API_HASH, BOT_TOKEN, BIN_CHANNEL, WORKER_URL, HMAC_SECRET, WORKER_SECRET, MONGO_URI
// Also: ADMINS (comma-separated user ids), MONGO_DB (default tgstream)
