package mongochk

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/gomega"

	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/mongo"
	mongodb "go.mongodb.org/mongo-driver/mongo"
	mongooptions "go.mongodb.org/mongo-driver/mongo/options"
)

func TestNewMongo(t *testing.T) {
	RegisterTestingT(t)

	t.Run("Happy path", func(t *testing.T) {
		preset := mongo.Preset()
		container, err := gnomock.Start(preset)
		defer gnomock.Stop(container)
		addr := container.DefaultAddress()
		uri := fmt.Sprintf("mongodb://%s:%s@%s", "gnomock", "gnomick", addr)
		clientOptions := mongooptions.Client().ApplyURI(uri)

		client, err := mongodb.Connect(context.Background(), clientOptions)
		defer client.Disconnect(context.Background())

		Expect(err).ToNot(HaveOccurred())
	})

	t.Run("Bad config should error", func(t *testing.T) {
		var cfg *MongoConfig
		r, err := NewMongo(cfg)

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("unable to validate mongodb config"))
		Expect(r).To(BeNil())
	})

	t.Run("Should error when mongo server is not available", func(t *testing.T) {
		cfg := &MongoConfig{
			Ping: true,
			Auth: &MongoAuthConfig{
				Url: "foobar:42848",
			},
			DialTimeout: 20 * time.Millisecond,
		}

		r, err := NewMongo(cfg)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("no reachable servers"))
		Expect(r).To(BeNil())
	})
}

func TestValidateMongoConfig(t *testing.T) {
	RegisterTestingT(t)

	t.Run("Should error with nil main config", func(t *testing.T) {
		var cfg *MongoConfig
		err := validateMongoConfig(cfg)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("Main config cannot be nil"))
	})

	t.Run("Should error with nil auth config", func(t *testing.T) {
		err := validateMongoConfig(&MongoConfig{})
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("Auth config cannot be nil"))
	})

	t.Run("Auth config must have an addr set", func(t *testing.T) {
		cfg := &MongoConfig{
			Auth: &MongoAuthConfig{},
		}

		err := validateMongoConfig(cfg)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("Url string must be set in auth config"))
	})

	t.Run("Should error if none of the check methods are enabled", func(t *testing.T) {
		cfg := &MongoConfig{
			Auth: &MongoAuthConfig{
				Url: "localhost:6379",
			},
		}

		err := validateMongoConfig(cfg)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("At minimum, either cfg.Ping or cfg.Collection"))
	})

	t.Run("Should error if url has wrong format", func(t *testing.T) {
		cfg := &MongoConfig{
			Auth: &MongoAuthConfig{
				Url: "localhost:40001?foo=1&bar=2",
			},
		}

		err := validateMongoConfig(cfg)
		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("Unable to parse URL"))
	})

}

func TestMongoStatus(t *testing.T) {
	RegisterTestingT(t)

	t.Run("Shouldn't error when ping is enabled", func(t *testing.T) {
		cfg := &MongoConfig{
			Ping: true,
		}
		checker, err := setupMongo(cfg)
		if err != nil {
			t.Fatal(err)
		}

		Expect(err).ToNot(HaveOccurred())

		_, err = checker.Status()

		Expect(err).To(BeNil())
	})

	t.Run("Should error if collection not found(available)", func(t *testing.T) {
		cfg := &MongoConfig{
			Collection: "go-check",
		}
		checker, err := setupMongo(cfg)
		if err != nil {
			t.Fatal(err)
		}

		_, err = checker.Status()

		Expect(err).To(HaveOccurred())
		Expect(err.Error()).To(ContainSubstring("collection not found"))
	})

}

func setupMongo(cfg *MongoConfig) (*Mongo, error) {
	preset := mongo.Preset()
	container, err := gnomock.Start(preset)
	addr := container.DefaultAddress()
	uri := fmt.Sprintf("mongodb://%s", addr)
	clientOptions := mongooptions.Client().ApplyURI(uri)

	mongodb.Connect(context.Background(), clientOptions)
	if err != nil {
		return nil, fmt.Errorf("unable to setup mongo: %v", err)
	}

	cfg.Auth = &MongoAuthConfig{
		Url: uri,
	}

	checker, err := NewMongo(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to setup checker: %v", err)
	}

	return checker, nil
}
