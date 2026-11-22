package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/telegram/message"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

const welcomeText = "Send or forward me any file, video, audio or photo and I will reply with a direct download link and a stream link."

// Bot holds the primary bot's handlers and state.
type Bot struct {
	cfg     *Config
	pool    *Pool
	store   *Store
	rootCtx context.Context // lives until shutdown; used by background broadcasts

	broadcasting atomic.Bool
	seen         sync.Map // user ids already persisted in this process
}

func NewBot(ctx context.Context, cfg *Config, pool *Pool, store *Store) *Bot {
	return &Bot{cfg: cfg, pool: pool, store: store, rootCtx: ctx}
}

// Register installs the message handler on the update dispatcher.
func (b *Bot) Register(d *tg.UpdateDispatcher) {
	d.OnNewMessage(b.onMessage)
}

func (b *Bot) isAdmin(id int64) bool {
	_, ok := b.cfg.Admins[id]
	return ok
}

func humanSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for m := n / unit; m >= unit; m /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

// forwardedMsgID finds the id of the message that landed in BIN_CHANNEL.
func forwardedMsgID(upd tg.UpdatesClass) (int, bool) {
	var list []tg.UpdateClass
	switch v := upd.(type) {
	case *tg.Updates:
		list = v.Updates
	case *tg.UpdatesCombined:
		list = v.Updates
	}
	for _, u := range list {
		if nm, ok := u.(*tg.UpdateNewChannelMessage); ok {
			if m, ok := nm.Message.(*tg.Message); ok {
				return m.ID, true
			}
		}
	}
	return 0, false
}

func randomID() (int64, error) {
	var rb [8]byte
	if _, err := rand.Read(rb[:]); err != nil {
		return 0, err
	}
	return int64(binary.LittleEndian.Uint64(rb[:])), nil
}

func parseCommand(text string) (cmd, rest string) {
	if !strings.HasPrefix(text, "/") {
		return "", ""
	}
	fields := strings.Fields(text)
	if len(fields) == 0 {
		return "", ""
	}
	cmd = strings.ToLower(strings.TrimPrefix(fields[0], "/"))
	if i := strings.IndexByte(cmd, '@'); i >= 0 {
		cmd = cmd[:i]
	}
	rest = strings.TrimSpace(strings.TrimPrefix(text, fields[0]))
	return cmd, rest
}

// trackUser persists the user once per process run (id + access hash).
func (b *Bot) trackUser(ctx context.Context, user *tg.User) {
	if _, loaded := b.seen.LoadOrStore(user.ID, struct{}{}); loaded {
		return
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := b.store.AddUser(cctx, user.ID, user.AccessHash); err != nil {
		b.seen.Delete(user.ID)
		log.Printf("db: add user %d: %v", user.ID, err)
	}
}

func (b *Bot) onMessage(ctx context.Context, e tg.Entities, u *tg.UpdateNewMessage) error {
	msg, ok := u.Message.(*tg.Message)
	if !ok || msg.Out {
		return nil
	}
	peer, ok := msg.PeerID.(*tg.PeerUser) // private chats only
	if !ok {
		return nil
	}
	user, ok := e.Users[peer.UserID]
	if !ok || user.Bot {
		return nil
	}
	api := b.pool.Primary().api.Load()
	if api == nil {
		return nil
	}
	say := func(text string) error {
		_, err := message.NewSender(api).Reply(e, u).Text(ctx, text)
		return err
	}

	b.trackUser(ctx, user)
	admin := b.isAdmin(user.ID)

	if msg.Media == nil {
		cmd, rest := parseCommand(strings.TrimSpace(msg.Message))
		switch cmd {
		case "start", "help":
			return say(welcomeText)
		case "fsub":
			if admin {
				return b.cmdFsub(ctx, api, say, rest)
			}
		case "broadcast":
			if admin {
				return b.cmdBroadcast(say, user, msg, rest)
			}
		case "stats":
			if admin {
				return b.cmdStats(ctx, say)
			}
		}
	}

	if !admin && len(b.cfg.AllowedUsers) > 0 {
		if _, ok := b.cfg.AllowedUsers[user.ID]; !ok {
			return say("You are not allowed to use this bot.")
		}
	}

	meta, _, _, hasMedia := parseMedia(msg)
	if !hasMedia {
		return say(welcomeText)
	}

	if !admin {
		if missing := b.missingFsub(ctx, user); len(missing) > 0 {
			return say(fsubMessage(missing))
		}
	}

	return b.handleFile(ctx, api, say, user, msg, meta)
}

// handleFile copies the file to BIN_CHANNEL and replies with signed links.
func (b *Bot) handleFile(ctx context.Context, api *tg.Client, say func(string) error, user *tg.User, msg *tg.Message, meta fileMeta) error {
	primary := b.pool.Primary()
	ch, err := primary.inputChannel(ctx, b.cfg.BinChannelID)
	if err != nil {
		log.Printf("bot: resolve BIN_CHANNEL: %v", err)
		return say("Storage channel is not reachable. Make sure the bot is an admin of BIN_CHANNEL.")
	}
	rid, err := randomID()
	if err != nil {
		return say("Internal error, please try again.")
	}

	upd, err := api.MessagesForwardMessages(ctx, &tg.MessagesForwardMessagesRequest{
		FromPeer:   &tg.InputPeerUser{UserID: user.ID, AccessHash: user.AccessHash},
		ID:         []int{msg.ID},
		RandomID:   []int64{rid},
		ToPeer:     &tg.InputPeerChannel{ChannelID: ch.ChannelID, AccessHash: ch.AccessHash},
		DropAuthor: true, // copy instead of showing "forwarded from"
	})
	if err != nil {
		log.Printf("bot: forward failed: %v", err)
		return say("Could not store that file (it may be protected from forwarding). Please try another one.")
	}
	binID, ok := forwardedMsgID(upd)
	if !ok {
		return say("Could not determine the stored message id. Please try again.")
	}

	exp := time.Now().Add(b.cfg.LinkTTL).Unix()
	sig := signLink(b.cfg.HMACSecret, binID, exp)
	dl := fmt.Sprintf("%s/dl/%d?exp=%d&hash=%s", b.cfg.WorkerURL, binID, exp, sig)
	st := dl + "&inline=1"

	return say(fmt.Sprintf("Your link is ready\n\nFile: %s\nSize: %s\n\nDownload:\n%s\n\nStream:\n%s\n\nLinks expire in %s.",
		meta.Name, humanSize(meta.Size), dl, st, b.cfg.LinkTTL.Round(time.Minute)))
}

// ---------------------------------------------------------------------------
// Admin commands
// ---------------------------------------------------------------------------

// cmdFsub: /fsub            show current setting
//          /fsub <id> [id2] set force-subscribe chats (replaces the list)
//          /fsub off        disable
func (b *Bot) cmdFsub(ctx context.Context, api *tg.Client, say func(string) error, rest string) error {
	args := strings.Fields(rest)
	usage := "Usage:\n/fsub <chat_id> [chat_id2 ...]\n/fsub off"

	if len(args) == 0 {
		cur := b.store.Fsub()
		if len(cur) == 0 {
			return say("Force-subscribe is OFF.\n\n" + usage)
		}
		return say("Force-subscribe is ON for:\n" + formatFsub(cur) + "\n\n" + usage)
	}

	switch strings.ToLower(args[0]) {
	case "off", "clear", "disable":
		if err := b.store.SetFsub(ctx, nil); err != nil {
			log.Printf("db: clear fsub: %v", err)
			return say("Database error, please try again.")
		}
		return say("Force-subscribe disabled.")
	}

	if len(args) > maxFsubChats {
		return say(fmt.Sprintf("At most %d chats are supported.", maxFsubChats))
	}

	var chats []FsubChat
	var warnings []string
	seen := map[int64]struct{}{}
	for _, a := range args {
		id, err := strconv.ParseInt(a, 10, 64)
		if err != nil {
			return say("Invalid chat id: " + a + "\n\n" + usage)
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}

		c, warn, err := b.resolveFsubChat(ctx, api, id)
		if err != nil {
			return say(fmt.Sprintf("Cannot use %d: %v", id, err))
		}
		chats = append(chats, c)
		if warn != "" {
			warnings = append(warnings, fmt.Sprintf("%s: %s", c.Title, warn))
		}
	}

	if err := b.store.SetFsub(ctx, chats); err != nil {
		log.Printf("db: set fsub: %v", err)
		return say("Database error, please try again.")
	}

	out := "Force-subscribe enabled for:\n" + formatFsub(chats)
	if len(warnings) > 0 {
		out += "\n\nWarnings:\n- " + strings.Join(warnings, "\n- ")
	}
	return say(out)
}

func (b *Bot) cmdStats(ctx context.Context, say func(string) error) error {
	n, err := b.store.CountUsers(ctx)
	if err != nil {
		log.Printf("db: count users: %v", err)
		return say("Database error, please try again.")
	}
	fsub := "OFF"
	if c := b.store.Fsub(); len(c) > 0 {
		fsub = fmt.Sprintf("ON (%d chat(s))", len(c))
	}
	return say(fmt.Sprintf("Users: %d\nForce-subscribe: %s\nBot clients ready: %d/%d",
		n, fsub, b.pool.ReadyCount(), len(b.pool.workers)))
}

// cmdBroadcast: reply to any message with /broadcast, or "/broadcast <text>".
func (b *Bot) cmdBroadcast(say func(string) error, admin *tg.User, msg *tg.Message, rest string) error {
	replyID := 0
	if h, ok := msg.ReplyTo.(*tg.MessageReplyHeader); ok {
		replyID = h.ReplyToMsgID
	}
	if replyID == 0 && rest == "" {
		return say("Usage: reply to a message with /broadcast, or send /broadcast <text>")
	}
	if !b.broadcasting.CompareAndSwap(false, true) {
		return say("A broadcast is already running.")
	}
	go b.runBroadcast(admin.ID, admin.AccessHash, replyID, rest)
	return say("Broadcast started. I will report when it finishes.")
}

const (
	sendOK = iota
	sendGone
	sendFailed
)

func (b *Bot) sendOne(ctx context.Context, adminPeer *tg.InputPeerUser, u UserDoc, replyID int, text string) int {
	peer := &tg.InputPeerUser{UserID: u.ID, AccessHash: u.Hash}
	for attempt := 0; attempt < 2; attempt++ {
		api := b.pool.Primary().api.Load()
		if api == nil {
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return sendFailed
			}
			continue
		}

		var err error
		if replyID != 0 {
			var rid int64
			if rid, err = randomID(); err == nil {
				_, err = api.MessagesForwardMessages(ctx, &tg.MessagesForwardMessagesRequest{
					FromPeer:   adminPeer,
					ID:         []int{replyID},
					RandomID:   []int64{rid},
					ToPeer:     peer,
					DropAuthor: true,
				})
			}
		} else {
			_, err = message.NewSender(api).To(peer).Text(ctx, text)
		}
		if err == nil {
			return sendOK
		}

		var rpcErr *tgerr.Error
		if errors.As(err, &rpcErr) {
			switch {
			case rpcErr.IsType("FLOOD_WAIT"):
				wait := time.Duration(rpcErr.Argument+1) * time.Second
				select {
				case <-time.After(wait):
				case <-ctx.Done():
					return sendFailed
				}
				continue
			case rpcErr.IsOneOf("USER_IS_BLOCKED", "INPUT_USER_DEACTIVATED", "USER_DEACTIVATED", "PEER_ID_INVALID"):
				cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				_ = b.store.RemoveUser(cctx, u.ID)
				cancel()
				b.seen.Delete(u.ID)
				return sendGone
			}
		}
		return sendFailed
	}
	return sendFailed
}

// runBroadcast runs in the background (~20 messages/s) and reports to the admin.
func (b *Bot) runBroadcast(adminID, adminHash int64, replyID int, text string) {
	defer b.broadcasting.Store(false)

	ctx := b.rootCtx
	adminPeer := &tg.InputPeerUser{UserID: adminID, AccessHash: adminHash}
	started := time.Now()
	var sent, failed, removed int

	err := b.store.EachUser(ctx, func(u UserDoc) bool {
		if ctx.Err() != nil {
			return false
		}
		switch b.sendOne(ctx, adminPeer, u, replyID, text) {
		case sendOK:
			sent++
		case sendGone:
			removed++
		default:
			failed++
		}
		time.Sleep(50 * time.Millisecond)
		return true
	})
	if err != nil {
		log.Printf("broadcast: iterate users: %v", err)
	}

	report := fmt.Sprintf("Broadcast finished in %s\nSent: %d\nFailed: %d\nRemoved (blocked/deleted): %d",
		time.Since(started).Round(time.Second), sent, failed, removed)
	if err != nil {
		report += "\nStopped early: database error"
	}
	api := b.pool.Primary().api.Load()
	if api == nil || ctx.Err() != nil {
		log.Print(report)
		return
	}
	rctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if _, err := message.NewSender(api).To(adminPeer).Text(rctx, report); err != nil {
		log.Printf("broadcast: report failed: %v (%s)", err, report)
	}
}
