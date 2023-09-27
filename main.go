package main

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Point struct {
	ID   int
	XStr string
	YStr string
	ZStr string
	XInt int
	YInt int
	ZInt int
}

func main() {
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
	/*for i := 1000; i < 10000; i++ {

		//_, err := AddOneItem(collection, generateRandomPoint(i))
		point := generateRandomPoint(i)
		//fmt.Printf("%+v\n", point)

		_, err := collection.InsertOne(context.TODO(), point)
		if err != nil {
			panic(err)
		}
	}*/

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
	for cur.Next(ctx) {
		fmt.Println("cur: ", cur)
	}

	/*
		for i := 0; i < 10; i++ {
			cur, err := collection.Find(ctx, bson.D{{"xstr", "117"}})
			//require.NoError(b, err)

			var res []bson.D
			err = cur.All(ctx, &res)
			//require.NoError(b, err)

			//require.NotEmpty(b, res)
			if err != nil {
				fmt.Printf("err: %v\n", err)
			} else {
				fmt.Printf("res: %+v\n", res)
			}
		}
	*/
	/*
			options := options.Find()
		filter := bson.M{}
		cursor, err := collection.Find(ctx, filter, options)
		if err != nil {
			panic(err)
		}
		var results []*User
		for cursor.Next(context.TODO()) {
			var elem User
			err := cursor.Decode(&elem)
			if err != nil {
				panic(err)
			}

			results = append(results, &elem)
		}

		if err := cursor.Err(); err != nil {
			log.Fatal(err)
		}

		// Close the cursor once finished
		cursor.Close(ctx)

		fmt.Printf("Found multiple documents (array of pointers): %v\n", results)
		for i, val := range results {
			fmt.Printf("i: %v, val: %v\n", i, val)
		} */

	/*
			res, err := GetOneItem[User](collection, bson.M{"name": "testuser"})
		if err != nil {
			panic(err)
		}

		fmt.Printf("Found one document : %v\n", res)
		res, err = GetOneItem[User](collection, bson.M{"name": "Misty", "role": "test"})
		if err != nil {
			panic(err)
		}

		fmt.Printf("Found one document : %v\n", res)

		resT, err := GetOneItem[Trainer](collection, bson.M{"name": bson.M{"$search": "Bro"}})
		if err != nil {
			panic(err)
		}

		fmt.Printf("Found one document : %v\n", resT)
	*/
	/*
	   addRes, err := AddOneItem(collection, User{Name: "Misty", Role: "test"})

	   	if err != nil {
	   		panic(err)
	   	}

	   fmt.Printf("Added one document : %v\n", addRes.InsertedID)

	   	ash := Trainer{"Ash", 10, "Pallet Town"}
	   	misty := Trainer{"Misty", 10, "Cerulean City"}
	   	brock := Trainer{"Brock", 15, "Pewter City"}
	   	addRes, err = AddOneItem(collection, ash)
	   	if err != nil {
	   		panic(err)
	   	}
	   	fmt.Printf("Added one document : %v\n", addRes.InsertedID)
	   	addRes, err = AddOneItem(collection, misty)
	   	if err != nil {
	   		panic(err)
	   	}
	   	fmt.Printf("Added one document : %v\n", addRes.InsertedID)
	   	addRes, err = AddOneItem(collection, brock)
	   	if err != nil {
	   		panic(err)
	   	}
	   	fmt.Printf("Added one document : %v\n", addRes.InsertedID)
	*/
}

func AddOneItem[T any](collection *mongo.Collection, item T) (*mongo.InsertOneResult, error) {
	return collection.InsertOne(context.TODO(), item)
}

func GetOneItem[T any](collection *mongo.Collection, filter primitive.M) (*T, error) {
	var result T
	err := collection.FindOne(context.TODO(), filter).Decode(&result)
	return &result, err
}

/*
func InsertMany[T any](collection *mongo.Collection, documents T) error {
	res, err := collection.InsertMany(collection.TODO(), documents)
}*/

func generateRandomPoint(index int) Point {
	count := 100
	minVal := 100

	x := rand.Intn(count-1) + minVal + 1
	y := rand.Intn(count-1) + minVal + 1
	z := rand.Intn(count-1) + minVal + 1
	return Point{
		ID:   index + 1,
		XStr: strconv.Itoa(x),
		XInt: x,
		YStr: strconv.Itoa(y),
		YInt: y,
		ZStr: strconv.Itoa(z),
		ZInt: z,
	}
}

func MongoPipeline(str string) mongo.Pipeline {
	var pipeline = []bson.D{}
	str = strings.TrimSpace(str)
	if strings.Index(str, "[") != 0 {
		var doc bson.D
		bson.UnmarshalExtJSON([]byte(str), false, &doc)
		pipeline = append(pipeline, doc)
	} else {
		bson.UnmarshalExtJSON([]byte(str), false, &pipeline)
	}
	return pipeline
}
