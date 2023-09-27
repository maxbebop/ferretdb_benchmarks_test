package main

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func BenchmarkPushdownsIndex(b *testing.B) {
	ctx := context.TODO()
	opts := options.Client().ApplyURI("mongodb://testuser:12345@127.0.0.1/ferretdb?authMechanism=PLAIN")

	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		panic(err)
	}

	defer client.Disconnect(ctx)
	fmt.Printf("%T\n", client)
	dbNames, err := client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		panic(err)
	}
	fmt.Println(dbNames)

	collection := client.Database("ferretdb").Collection("Benchmarks")

	b.Run("Pushdown string =", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cur, err := collection.Find(ctx, bson.D{{"ystr", "117"}})
			require.NoError(b, err)

			var res []bson.D
			err = cur.All(ctx, &res)
			require.NoError(b, err)

			require.NotEmpty(b, res)
		}
	})

	b.Run("Pushdown int =", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cur, err := collection.Find(ctx, bson.D{{"yint", 117}})
			require.NoError(b, err)

			var res []bson.D
			err = cur.All(ctx, &res)
			require.NoError(b, err)

			require.NotEmpty(b, res)
		}
	})

	b.Run("Pushdown string eq", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cur, err := collection.Find(ctx, bson.D{{"ystr", bson.M{"$eq": "117"}}})
			require.NoError(b, err)

			var res []bson.D
			err = cur.All(ctx, &res)
			require.NoError(b, err)

			require.NotEmpty(b, res)
		}
	})

	b.Run("Pushdown int eq", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cur, err := collection.Find(ctx, bson.D{{"yint", bson.M{"$eq": 117}}})
			require.NoError(b, err)

			var res []bson.D
			err = cur.All(ctx, &res)
			require.NoError(b, err)

			require.NotEmpty(b, res)
		}
	})

	b.Run("NoPushdown int gt", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			cur, err := collection.Find(ctx, bson.D{{"yint", bson.M{"$gt": 198}}})
			require.NoError(b, err)

			var res []bson.D
			err = cur.All(ctx, &res)
			require.NoError(b, err)

			require.NotEmpty(b, res)
		}
	})
}
