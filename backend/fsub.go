package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
)

const maxFsubChats = 5

// bareChannelID converts -1001234567890 to 1234567890.
func bareChannelID(raw int64) (int64, error) {
	if raw > -1000000000000 {
		return 0, errors.New("must be a channel or supergroup id starting with -100")
	}
	return -raw - 1000000000000, nil
}

// isMember reports whether the user is currently in the chat. The primary bot
// must be an admin of the chat for channels.getParticipant to work.
func (b *Bot) isMember(ctx context.Context, chatID int64, user *tg.User) (bool, error) {
	cid, err := bareChannelID(chatID)
	if err != nil {
		return false, err
	}
	primary := b.pool.Primary()
	api := primary.api.Load()
	if api == nil {
		return false, errNotReady
	}
	ch, err := primary.inputChannel(ctx, cid)
	if err != nil {
		return false, err
	}

	res, err := api.ChannelsGetParticipant(ctx, &tg.ChannelsGetParticipantRequest{
		Channel:     ch,
		Participant: &tg.InputPeerUser{UserID: user.ID, AccessHash: user.AccessHash},
	})
	if err != nil {
		var rpcErr *tgerr.Error
		if errors.As(err, &rpcErr) && rpcErr.IsType("USER_NOT_PARTICIPANT") {
			return false, nil
		}
		return false, err
	}

	switch p := res.Participant.(type) {
	case *tg.ChannelParticipantLeft:
		return false, nil
	case *tg.ChannelParticipantBanned:
		return !p.Left, nil
	}
	return true, nil
}

// missingFsub returns the force-subscribe chats the user has not joined.
// Errors (e.g. the bot lost admin rights) fail open so users are not locked out.
func (b *Bot) missingFsub(ctx context.Context, user *tg.User) []FsubChat {
	chats := b.store.Fsub()
	var missing []FsubChat
	for _, c := range chats {
		ok, err := b.isMember(ctx, c.ID, user)
		if err != nil {
			log.Printf("fsub: membership check for chat %d failed: %v", c.ID, err)
			continue
		}
		if !ok {
			missing = append(missing, c)
		}
	}
	return missing
}

// resolveFsubChat looks the chat up with the primary bot, builds a join link
// and returns an optional warning (e.g. the bot is not an admin there).
func (b *Bot) resolveFsubChat(ctx context.Context, api *tg.Client, chatID int64) (FsubChat, string, error) {
	cid, err := bareChannelID(chatID)
	if err != nil {
		return FsubChat{}, "", err
	}

	res, err := api.ChannelsGetChannels(ctx, []tg.InputChannelClass{&tg.InputChannel{ChannelID: cid}})
	if err != nil {
		return FsubChat{}, "", fmt.Errorf("the bot cannot see this chat (add it as admin first): %w", err)
	}
	var channel *tg.Channel
	if g, ok := res.(interface{ GetChats() []tg.ChatClass }); ok {
		for _, c := range g.GetChats() {
			if chn, ok := c.(*tg.Channel); ok && chn.ID == cid {
				channel = chn
			}
		}
	}
	if channel == nil {
		return FsubChat{}, "", errors.New("chat not found")
	}

	var warn string
	me, err := api.ChannelsGetParticipant(ctx, &tg.ChannelsGetParticipantRequest{
		Channel:     &tg.InputChannel{ChannelID: cid, AccessHash: channel.AccessHash},
		Participant: &tg.InputPeerSelf{},
	})
	if err != nil {
		warn = "the bot is not an admin there, so membership checks will not work"
	} else {
		switch me.Participant.(type) {
		case *tg.ChannelParticipantAdmin, *tg.ChannelParticipantCreator:
		default:
			warn = "the bot is not an admin there, so membership checks will not work"
		}
	}

	link := ""
	if channel.Username != "" {
		link = "https://t.me/" + channel.Username
	} else {
		inv, err := api.MessagesExportChatInvite(ctx, &tg.MessagesExportChatInviteRequest{
			Peer: &tg.InputPeerChannel{ChannelID: cid, AccessHash: channel.AccessHash},
		})
		if err != nil {
			return FsubChat{}, "", fmt.Errorf("private chat needs an invite link and the bot cannot create one (give it the invite-users right): %w", err)
		}
		if e, ok := inv.(*tg.ChatInviteExported); ok {
			link = e.Link
		}
		if link == "" {
			return FsubChat{}, "", errors.New("could not create an invite link")
		}
	}

	return FsubChat{ID: chatID, Title: channel.Title, Link: link}, warn, nil
}

func formatFsub(chats []FsubChat) string {
	var sb strings.Builder
	for _, c := range chats {
		fmt.Fprintf(&sb, "- %s (%d)\n  %s\n", c.Title, c.ID, c.Link)
	}
	return strings.TrimRight(sb.String(), "\n")
}

func fsubMessage(missing []FsubChat) string {
	var sb strings.Builder
	sb.WriteString("To use this bot you must join the following chat(s) first:\n\n")
	for i, c := range missing {
		fmt.Fprintf(&sb, "%d. %s\n%s\n\n", i+1, c.Title, c.Link)
	}
	sb.WriteString("After joining, send your file again.")
	return sb.String()
}
