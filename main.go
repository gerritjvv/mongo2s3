package main

import (
	"context"
	"fmt"
	goparquet "github.com/fraugster/parquet-go"
	"github.com/fraugster/parquet-go/parquet"
	"github.com/fraugster/parquet-go/parquetschema"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"os"
	"os/signal"
	"syscall"
)

type ChangeEvent struct {
	OperationType string `bson:"operationType"`
	FullDocument  bson.M `bson:"fullDocument"`
	Namespace     struct {
		DB   string `bson:"db"`
		Coll string `bson:"coll"`
	} `bson:"ns"`
	DocumentKey struct {
		ID interface{} `bson:"_id"`
	} `bson:"documentKey"`
}

func ConvertBSON(v interface{}) interface{} {
	switch x := v.(type) {

	case bson.ObjectID:
		return x.Hex()

	case bson.D:
		m := make(map[string]interface{})
		for _, e := range x {
			m[e.Key] = ConvertBSON(e.Value)
		}
		return m

	case bson.M:
		m := make(map[string]interface{})
		for k, v2 := range x {
			m[k] = ConvertBSON(v2)
		}
		return m

	case []interface{}:
		arr := make([]interface{}, len(x))
		for i, v2 := range x {
			arr[i] = ConvertBSON(v2)
		}
		return arr

	default:
		return x
	}
}

func main() {

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Capture CTRL+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	MONGO_URL := "mongodb://mongodb.mongo2s3.orb.local:27017"
	println("Hello world")

	conn, err := mongo.Connect(options.Client().ApplyURI(MONGO_URL))
	if err != nil {
		panic(err)
	}
	defer conn.Disconnect(context.Background())

	println("Connected to MongoDB!")
	db := conn.Database("test")
	println("Got db")
	users := db.Collection("users")
	println("Got users")
	cs, err := users.Watch(ctx, mongo.Pipeline{})

	if err != nil {
		panic(err)
	}
	println("Watching ")

	schemaStr, _ := os.ReadFile("user.parquet.schema")
	schema, err := parquetschema.ParseSchemaDefinition(string(schemaStr))
	if err != nil {
		panic(err)
	}

	f, err := os.Create("users.parquet")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	pw := goparquet.NewFileWriter(
		f,
		goparquet.WithSchemaDefinition(schema),
		goparquet.WithCompressionCodec(parquet.CompressionCodec_SNAPPY),
	)

	defer pw.Close()

	go func() {
		<-sigCh
		fmt.Println("\nShutting down... flushing Parquet file...")
		pw.Close()
		f.Close()
		os.Exit(0)
	}()

	for cs.Next(context.Background()) {
		if err := cs.Err(); err != nil {
			panic(err)
		}
		raw := cs.Current
		event := ChangeEvent{}
		err := bson.Unmarshal(raw, &event)
		if err != nil {
			panic(err)
		}

		document := event.FullDocument
		record := ConvertBSON(document).(map[string]interface{})
		fmt.Printf("db: %s, coll: %s, op: %s,  %#v\n",
			event.Namespace.DB, event.Namespace.Coll, event.OperationType,
			// id can be ObjectId
			record)
		if err := pw.AddData(record); err != nil {
			panic(err)
		}
	}
	println("Done ")
}
