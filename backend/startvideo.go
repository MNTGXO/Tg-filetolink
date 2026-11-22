package main

import (
	"context"
	_ "embed"
	"fmt"
	"log"

	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
)

// startVideo is shown with the /start message. It is embedded in the binary,
// uploaded to Telegram once per process start and then re-sent by reference.
//
//go:embed start.mp4
var startVideo []byte

// Properties of start.mp4 (1920x1080, ~6.5 s) so Telegram plays it inline.
const (
	startVideoWidth    = 1920
	startVideoHeight   = 1080
	startVideoDuration = 7
)

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

// mediaFromUpdates extracts the media of the message just sent.
func mediaFromUpdates(upd tg.UpdatesClass) tg.MessageMediaClass {
	switch v := upd.(type) {
	case *tg.UpdateShortSentMessage:
		return v.Media
	case *tg.Updates:
		for _, u := range v.Updates {
			if nm, ok := u.(*tg.UpdateNewMessage); ok {
				if m, ok := nm.Message.(*tg.Message); ok {
					return m.Media
				}
			}
		}
	}
	return nil
}

// sendStart sends the start video with welcomeText as its caption. If anything
// goes wrong it falls back to the plain text message.
func (b *Bot) sendStart(ctx context.Context, api *tg.Client, say func(string) error, user *tg.User) error {
	if err := b.sendStartVideo(ctx, api, user); err != nil {
		log.Printf("start video: %v", err)
		return say(welcomeText)
	}
	return nil
}

func (b *Bot) sendStartVideo(ctx context.Context, api *tg.Client, user *tg.User) error {
	peer := &tg.InputPeerUser{UserID: user.ID, AccessHash: user.AccessHash}

	// Fast path: re-send the already uploaded video by reference.
	if media := b.cachedStartMedia(); media != nil {
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
		log.Printf("start video: cached media rejected (%v); uploading again", err)
		b.setStartMedia(nil)
	}

	// Slow path: upload once, then cache the document reference.
	file, err := uploader.NewUploader(api).FromBytes(ctx, "start.mp4", startVideo)
	if err != nil {
		return fmt.Errorf("upload: %w", err)
	}
	rid, err := randomID()
	if err != nil {
		return err
	}
	upd, err := api.MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
		Peer: peer,
		Media: &tg.InputMediaUploadedDocument{
			File:     file,
			MimeType: "video/mp4",
			Attributes: []tg.DocumentAttributeClass{
				&tg.DocumentAttributeVideo{
					SupportsStreaming: true,
					W:                 startVideoWidth,
					H:                 startVideoHeight,
					Duration:          startVideoDuration,
				},
				&tg.DocumentAttributeFilename{FileName: "start.mp4"},
			},
		},
		Message:  welcomeText,
		RandomID: rid,
	})
	if err != nil {
		return fmt.Errorf("send: %w", err)
	}

	if md, ok := mediaFromUpdates(upd).(*tg.MessageMediaDocument); ok {
		if doc, ok := md.Document.AsNotEmpty(); ok {
			b.setStartMedia(&tg.InputMediaDocument{
				ID: &tg.InputDocument{
					ID:            doc.ID,
					AccessHash:    doc.AccessHash,
					FileReference: doc.FileReference,
				},
			})
		}
	}
	return nil
}

