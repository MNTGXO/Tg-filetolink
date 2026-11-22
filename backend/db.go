package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// UserDoc is one bot user. The access hash is stored next to the id because
// Telegram needs it to message the user again (broadcast).
type UserDoc struct {
	ID     int64     `bson:"_id"`
	Hash   int64     `bson:"hash"`
	Joined time.Time `bson:"joined"`
}

// FsubChat is one force-subscribe channel/supergroup.
type FsubChat struct {
	ID    int64  `bson:"id"` // raw id, e.g. -1001234567890
	Title string `bson:"title"`
	Link  string `bson:"link"` // public t.me link or exported invite link
}

type fsubDoc struct {
	Chats []FsubChat `bson:"chats"`
}

// Store wraps MongoDB. The force-subscribe list is cached in memory so the
// hot path (every file request) never touches the database.
type Store struct {
	client   *mongo.Client
	users    *mongo.Collection
	settings *mongo.Collection

	mu   sync.RWMutex
	fsub []FsubChat
}

// OpenStore connects, verifies the connection and loads the fsub list.
// The pool is kept tiny to save RAM on the 512 MB Koyeb instance.
func OpenStore(ctx context.Context, uri, dbName string) (*Store, error) {
	opts := options.Client().
		ApplyURI(uri).
		SetMaxPoolSize(4).
		SetMinPoolSize(0).
		SetMaxConnIdleTime(2 * time.Minute).
		SetServerSelectionTimeout(10 * time.Second)

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, err
	}

	db := client.Database(dbName)
	s := &Store{
		client:   client,
		users:    db.Collection("users"),
		settings: db.Collection("settings"),
	}

	var doc fsubDoc
	err = s.settings.FindOne(ctx, bson.M{"_id": "fsub"}).Decode(&doc)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		_ = client.Disconnect(context.Background())
		return nil, err
	}
	s.fsub = doc.Chats
	return s, nil
}

func (s *Store) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.client.Disconnect(ctx)
}

// AddUser upserts a user (id + access hash).
func (s *Store) AddUser(ctx context.Context, id, hash int64) error {
	_, err := s.users.UpdateOne(ctx,
		bson.M{"_id": id},
		bson.M{
			"$set":         bson.M{"hash": hash},
			"$setOnInsert": bson.M{"joined": time.Now().UTC()},
		},
		options.Update().SetUpsert(true),
	)
	return err
}

func (s *Store) RemoveUser(ctx context.Context, id int64) error {
	_, err := s.users.DeleteOne(ctx, bson.M{"_id": id})
	return err
}

func (s *Store) CountUsers(ctx context.Context) (int64, error) {
	return s.users.CountDocuments(ctx, bson.M{})
}

// EachUser streams every user to fn; return false from fn to stop.
func (s *Store) EachUser(ctx context.Context, fn func(UserDoc) bool) error {
	cur, err := s.users.Find(ctx, bson.D{}, options.Find().SetBatchSize(500))
	if err != nil {
		return err
	}
	defer cur.Close(ctx)
	for cur.Next(ctx) {
		var u UserDoc
		if err := cur.Decode(&u); err != nil {
			continue
		}
		if !fn(u) {
			break
		}
	}
	return cur.Err()
}

// Fsub returns a copy of the cached force-subscribe list.
func (s *Store) Fsub() []FsubChat {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]FsubChat, len(s.fsub))
	copy(out, s.fsub)
	return out
}

// SetFsub replaces the force-subscribe list (nil/empty disables it).
func (s *Store) SetFsub(ctx context.Context, chats []FsubChat) error {
	if chats == nil {
		chats = []FsubChat{}
	}
	_, err := s.settings.UpdateOne(ctx,
		bson.M{"_id": "fsub"},
		bson.M{"$set": bson.M{"chats": chats}},
		options.Update().SetUpsert(true),
	)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.fsub = chats
	s.mu.Unlock()
	return nil
}
