package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"strconv"
	"strings"

	"github.com/gotd/td/tg"
)

// The start video is not uploaded by the bot. It is read once from an existing
// Telegram post (START_VIDEO_LINK, e.g. https://t.me/mntgxo/230) and re-sent
// by reference, so /start never uploads anything.

type videoRef struct {
	username  string // public channel username, or ""
	channelID int64  // bare channel id for t.me/c/<id>/<msg> links
	msgID     int
}

func parseMessageLink(link string) (videoRef, error) {
	u, err := url.Parse(strings.TrimSpace(link))
	if err != nil || u.Host == "" {
		return videoRef{}, fmt.Errorf("invalid START_VIDEO_LINK %q", link)
	}
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")

	// Private channel: t.me/c/<channel id>/<message id>
	if len(parts) >= 3 && parts[0] == "c" {
		cid, err1 := strconv.ParseInt(parts[1], 10, 64)
		mid, err2 := strconv.Atoi(parts[2])
		if err1 != nil || err2 != nil || cid <= 0 || mid <= 0 {
			return videoRef{}, fmt.Errorf("invalid START_VIDEO_LINK %q", link)
		}
		return videoRef{channelID: cid, msgID: mid}, nil
	}
	// Public channel: t.me/<username>/<message id>
	if len(parts) >= 2 {
		mid, err := strconv.Atoi(parts[1])
		if err != nil || mid <= 0 || parts[0] == "" {
			return videoRef{}, fmt.Errorf("invalid START_VIDEO_LINK %q", link)
		}
		return videoRef{username: parts[0], msgID: mid}, nil
	}
	return videoRef{}, fmt.Errorf("invalid START_VIDEO_LINK %q", link)
}

func (b *Bot) cachedStartMedia() tg.InputMediaClass {
	b.startMu.Lock()
	defer b.startMu.Unlock()
	return b.startMedia
}

func (b *Bot) setStartMedia(m tg.InputMediaClass) {
	b.startMu.Lock()
	b.startMedia = m
	b.startMu.Unlock()
}

// fetchStartMedia reads the post and returns its document as input media.
func (b *Bot) fetchStartMedia(ctx context.Context, api *tg.Client) (tg.InputMediaClass, error) {
	ref, err := parseMessageLink(b.cfg.StartVideo)
	if err != nil {
		return nil, err
	}

	var ch *tg.InputChannel
	if ref.username == "" {
		ch, err = b.pool.Primary().inputChannel(ctx, ref.channelID)
		if err != nil {
			return nil, fmt.Errorf("cannot read the private channel (add the bot to it): %w", err)
		}
	} else {
		res, err := api.ContactsResolveUsername(ctx, &tg.ContactsResolveUsernameRequest{Username: ref.username})
		if err != nil {
			return nil, fmt.Errorf("resolve @%s: %w", ref.username, err)
		}
		for _, c := range res.Chats {
			if chn, ok := c.(*tg.Channel); ok {
				ch = &tg.InputChannel{ChannelID: chn.ID, AccessHash: chn.AccessHash}
				break
			}
		}
		if ch == nil {
			return nil, fmt.Errorf("@%s is not a channel", ref.username)
		}
	}

	res, err := api.ChannelsGetMessages(ctx, &tg.ChannelsGetMessagesRequest{
		Channel: ch,
		ID:      []tg.InputMessageClass{&tg.InputMessageID{ID: ref.msgID}},
	})
	if err != nil {
		return nil, fmt.Errorf("read message %d: %w", ref.msgID, err)
	}
	g, ok := res.(interface{ GetMessages() []tg.MessageClass })
	if !ok {
		return nil, errors.New("unexpected response")
	}
	for _, mc := range g.GetMessages() {
		msg, ok := mc.(*tg.Message)
		if !ok || msg.ID != ref.msgID {
			continue
		}
		md, ok := msg.Media.(*tg.MessageMediaDocument)
		if !ok {
			break
		}
		doc, ok := md.Document.AsNotEmpty()
		if !ok {
			break
		}
		return &tg.InputMediaDocument{
			ID: &tg.InputDocument{
				ID:            doc.ID,
				AccessHash:    doc.AccessHash,
				FileReference: doc.FileReference,
			},
		}, nil
	}
	return nil, fmt.Errorf("message %d has no video (deleted, or not a video/document)", ref.msgID)
}

// sendStart sends the video with welcomeText as its caption; on any failure it
// falls back to the plain text message.
func (b *Bot) sendStart(ctx context.Context, api *tg.Client, say func(string) error, user *tg.User) error {
	if err := b.sendStartVideo(ctx, api, user); err != nil {
		log.Printf("start video: %v", err)
		return say(welcomeText)
	}
	return nil
}

func (b *Bot) sendStartVideo(ctx context.Context, api *tg.Client, user *tg.User) error {
	peer := &tg.InputPeerUser{UserID: user.ID, AccessHash: user.AccessHash}

	media := b.cachedStartMedia()
	var lastErr error
	// Two attempts: the second one refreshes the media (expired file reference).
	for attempt := 0; attempt < 2; attempt++ {
		if media == nil {
			m, err := b.fetchStartMedia(ctx, api)
			if err != nil {
				return err
			}
			media = m
			b.setStartMedia(media)
		}
		rid, err := randomID()
		if err != nil {
			return err
		}
		_, err = api.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
			Peer:     peer,
			Media:    media,
			Message:  welcomeText,
			RandomID: rid,
		})
		if err == nil {
			return nil
		}
		lastErr = err
		media = nil
		b.setStartMedia(nil)
	}
	return lastErr
}
