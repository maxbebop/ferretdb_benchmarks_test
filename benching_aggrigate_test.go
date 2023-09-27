package main

import (
	"context"
	"fmt"
	"testing"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func BenchmarkAggrigate(b *testing.B) {
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

	b.Run("aggrigate", func(b *testing.B) {
		pipeline := `[
			{"$match": { "xint": { $eq: 150 } }},
			{"$group": { "_id": "$yint", "count": { "$sum": 1 } }},
			{"$project": { "brand": "$_id", "_id": 0, "count": 1 }}
		]`
		optsAggr := options.Aggregate()
		optsAggr.SetAllowDiskUse(true)
		optsAggr.SetBatchSize(5)
		cur, err := collection.Aggregate(ctx, bson.D{{"xstr", "117"}})
		if cur, err = collection.Aggregate(ctx, MongoPipeline(pipeline), optsAggr); err != nil {
			fmt.Println("Error: ", err)
			return
		}

		defer cur.Close(ctx)
		total := 0
		for cur.Next(ctx) {
			total++
		}
		if total == 0 {
			b.Fatal("expected", total)
		}
	})

}

func BenchmarkAggrigate2(b *testing.B) {
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

	b.Run("aggrigate", func(b *testing.B) {
		pipeline := `[
			{"$match": { "xint": { $eq: 150 } }},
			{"$group": { "_id": "$yint", "count": { "$sum": 1 } }},
			{"$project": { "brand": "$_id", "_id": 0, "count": 1 }}
		]`
		optsAggr := options.Aggregate()
		optsAggr.SetAllowDiskUse(true)
		optsAggr.SetBatchSize(5)
		cur, err := collection.Aggregate(ctx, bson.D{{"xstr", "117"}})
		if cur, err = collection.Aggregate(ctx, MongoPipeline(pipeline), optsAggr); err != nil {
			fmt.Println("Error: ", err)
			return
		}

		defer cur.Close(ctx)
		total := 0
		for cur.Next(ctx) {
			total++
		}
		if total == 0 {
			b.Fatal("expected", total)
		}
	})

}
