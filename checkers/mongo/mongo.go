package mongochk

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/globalsign/mgo"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readpref"
)

const (
	DefaultDialTimeout = 1 * time.Second
)

// MongoConfig is used for configuring the go-mongo check.
//
// "Auth" is _required_; mongo connection/auth config.
//
// "Collection" is optional; method checks if collection exist
//
// "Ping" is optional; Ping runs a trivial ping command just to get in touch with the server.
//
// "DialTimeout" is optional; default @ 10s; determines the max time we'll wait to reach a server.
//
// Note: At least _one_ check method must be set/enabled; you can also enable
// _all_ of the check methods (i.e. perform a ping, or check particular collection for existence).
type MongoConfig struct {
	Auth        *MongoAuthConfig
	Collection  string
	DB          string
	Ping        bool
	DialTimeout time.Duration
}

// MongoAuthConfig used to set up connection params for go-mongo check
// Url format is localhost:27017 or mongo://localhost:27017
// https://www.mongodb.com/docs/manual/core/authentication-mechanisms/.
type MongoAuthConfig struct {
	Url         string
	Credentials options.Credential
}

type Mongo struct {
	Config *MongoConfig
	Client *mongo.Client
}

func NewMongo(cfg *MongoConfig) (*Mongo, error) {
	// validate settings
	if err := validateMongoConfig(cfg); err != nil {
		return nil, fmt.Errorf("unable to validate mongodb config: %v", err)
	}

	uri := normalizeMongoURI(cfg.Auth.Url)

	clientOpts := options.Client().ApplyURI(uri)

	dt := cfg.DialTimeout

	clientOpts.
		SetConnectTimeout(dt).
		SetServerSelectionTimeout(dt)

	if cfg.Auth.Credentials.Username != "" || cfg.Auth.Credentials.Password != "" || cfg.Auth.Credentials.AuthSource != "" {
		clientOpts.SetAuth(cfg.Auth.Credentials)
	}
	ctx, cancel := context.WithTimeout(context.Background(), dt)
	defer cancel()
	client, err := mongo.Connect(clientOpts)

	if err != nil {
		return nil, err
	}

	//defer func() {
	//	if err = client.Disconnect(ctx); err != nil {
	//		panic(err)
	//	}
	//}()

	// Initial ping to ensure connectivity
	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		_ = client.Disconnect(context.Background())
		return nil, fmt.Errorf("unable to establish initial connection to mongodb: %w", err)
	}
	return &Mongo{
		Config: cfg,
		Client: client,
	}, nil
}

func (m *Mongo) Status() (interface{}, error) {
	dt := m.Config.DialTimeout
	if dt <= 0 {
		dt = DefaultDialTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), dt)
	defer cancel()

	if m.Config.Ping {
		if err := m.Client.Ping(ctx, readpref.Primary()); err != nil {
			return nil, fmt.Errorf("ping failed: %w", err)
		}
	}

	if m.Config.Collection != "" {
		if m.Config.DB == "" {
			return nil, fmt.Errorf("db name must be set when checking collection existence")
		}
		db := m.Client.Database(m.Config.DB)
		names, err := db.ListCollectionNames(ctx, bson.D{{Key: "name", Value: m.Config.Collection}})
		if err != nil {
			return nil, fmt.Errorf("unable to list collections: %w", err)
		}
		if !contains(names, m.Config.Collection) {
			return nil, fmt.Errorf("mongo db %q collection %q not found", m.Config.DB, m.Config.Collection)
		}
	}

	return nil, nil
}

func (m *Mongo) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), m.Config.DialTimeout)
	defer cancel()
	return m.Client.Disconnect(ctx)
}

func contains(data []string, needle string) bool {
	for _, item := range data {
		if item == needle {
			return true
		}
	}
	return false
}

func validateMongoConfig(cfg *MongoConfig) error {
	if cfg == nil {
		return fmt.Errorf("main config cannot be nil")
	}

	if cfg.Auth == nil {
		return fmt.Errorf("auth config cannot be nil")
	}

	if cfg.Auth.Url == "" {
		return fmt.Errorf("url string must be set in auth config")
	}

	if _, err := mgo.ParseURL(cfg.Auth.Url); err != nil {
		return fmt.Errorf("unable to parse URL: %v", err)
	}

	if !cfg.Ping && cfg.Collection == "" {
		return fmt.Errorf("at minimum, either cfg.Ping or cfg.Collection")
	}

	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = DefaultDialTimeout
	}

	return nil
}

func normalizeMongoURI(u string) string {
	us := strings.TrimSpace(u)
	if strings.HasPrefix(us, "mongodb://") || strings.HasPrefix(us, "mongodb+srv://") {
		return us
	}
	// Accept "mongo://..." too, map it to mongodb://
	if strings.HasPrefix(us, "mongo://") {
		return "mongodb://" + strings.TrimPrefix(us, "mongo://")
	}
	// Bare host[:port]
	return "mongodb://" + us
}
