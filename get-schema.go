package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/parquet-go/parquet-go"
)

func main() {
	schema, err := getSchema()
	if err != nil {
		log.Fatal(err)
	}
	r, err := os.Open("parquet-go.parquet")
	if err != nil {
		log.Fatal(err)
	}

	f := parquet.NewReader(r, schema)
	defer r.Close()

	for {
		record := map[string]interface{}{}
		err = f.Read(&record)

		if err != nil && errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(record)
	}
}

func getSchema() (*parquet.Schema, error) {
	r, err := os.Open("parquet-go.parquet")
	if err != nil {
		return nil, err
	}
	f, err := parquet.OpenFile(r, 3526)
	if err != nil {
		return nil, err
	}
	defer r.Close()

	md := f.Metadata()
	nodes := map[string]parquet.Node{}
	for _, schema := range md.Schema {
		if schema.Type == nil {
			continue
		}
		typ := schema.Type.String()
		logicalType := ""
		if schema.LogicalType != nil {
			logicalType = schema.LogicalType.String()
		}

		switch typ {
		case "INT8":
			nodes[schema.Name] = parquet.Required(parquet.Int(8))
		case "INT16":
			nodes[schema.Name] = parquet.Required(parquet.Int(16))
		case "INT32":
			nodes[schema.Name] = parquet.Required(parquet.Int(32))
		case "INT64":
			nodes[schema.Name] = parquet.Required(parquet.Int(64))
		case "DOUBLE":
			nodes[schema.Name] = parquet.Required(parquet.Leaf(parquet.DoubleType))
		case "BOOLEAN":
			nodes[schema.Name] = parquet.Required(parquet.Leaf(parquet.BooleanType))
		case "BYTE_ARRAY":
			if logicalType == "STRING" {
				nodes[schema.Name] = parquet.Required(parquet.String())
			} else {
				nodes[schema.Name] = parquet.Required(parquet.Leaf(parquet.ByteArrayType))
			}
		default:
			return nil, fmt.Errorf("unsupported type: %s, logical %s", typ, logicalType)
		}
	}

	return parquet.NewSchema("schema", parquet.Group(nodes)), nil
}
