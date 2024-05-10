package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/parquet-go/parquet-go"
)

type LogRow struct {
	Timestamp   int64   `parquet:"timestamp, convertedtype=TIMESTAMP_MILLIS"`
	Value       float64 `parquet:"value"`
	Provider    string  `parquet:"_provider"`
	ID          string  `parquet:"_id"`
	Fingerprint int64   `parquet:"_fingerprint"`
	Filtered    bool    `parquet:"_filtered"`
	RuleID      string  `parquet:"_rule_id"`
	ClusterID   string  `parquet:"_cluster_id"`
	Name        string  `parquet:"name"`
	Source      string  `parquet:"source"`
	Hostname    string  `parquet:"hostname"`
	Service     string  `parquet:"service"`
	Message     string  `parquet:"message"`
	Level       string  `parquet:"level"`
	TagA        string  `parquet:"tag_a"`
	TagB        string  `parquet:"tag_b"`
	TagC        string  `parquet:"tag_c"`
}

func nodeFromType(t any) (parquet.Node, error) {
	switch t.(type) {
	case int8, byte:
		return parquet.Required(parquet.Int(8)), nil
	case int16:
		return parquet.Required(parquet.Int(16)), nil
	case int32, int:
		return parquet.Required(parquet.Int(32)), nil
	case int64:
		return parquet.Required(parquet.Int(64)), nil
	case float64, float32:
		return parquet.Required(parquet.Leaf(parquet.DoubleType)), nil
	case string:
		return parquet.Required(parquet.String()), nil
	case bool:
		return parquet.Required(parquet.Leaf(parquet.BooleanType)), nil
	default:
		return nil, fmt.Errorf("unsupported type %T", t)
	}
}

func schemaFromMap(name string, typemap map[string]any) (*parquet.Schema, error) {
	fields := map[string]parquet.Node{}
	for name, t := range typemap {
		node, err := nodeFromType(t)
		if err != nil {
			return nil, err
		}
		fields[name] = node
	}
	return parquet.NewSchema(name, parquet.Group(fields)), nil
}

type MapWriter interface {
	WriteRows(rows []map[string]any) (count int, err error)
	Close() error
}

type ParquetMapWriter struct {
	writer   *parquet.GenericWriter[map[string]any]
	filename string
	tmpname  string
}

var (
	_ MapWriter = (*ParquetMapWriter)(nil)
)

func NewParquetMapWriter(filename string, schema *parquet.Schema) (*ParquetMapWriter, error) {
	tmpname := filename + ".tmp"
	f, err := os.Create(tmpname)
	if err != nil {
		return nil, fmt.Errorf("error creating file: %v", err)
	}
	wc, err := parquet.NewWriterConfig(schema, parquet.Compression(&parquet.Zstd))
	if err != nil {
		return nil, fmt.Errorf("error creating writer config: %v", err)
	}
	writer := parquet.NewGenericWriter[map[string]any](f, wc)
	return &ParquetMapWriter{writer: writer, filename: filename, tmpname: tmpname}, nil
}

func (w *ParquetMapWriter) WriteRows(rows []map[string]any) (count int, err error) {
	return w.writer.Write(rows)
}

func (w *ParquetMapWriter) Close() error {
	if err := w.writer.Close(); err != nil {
		return fmt.Errorf("error closing writer: %v", err)
	}
	if err := os.Rename(w.tmpname, w.filename); err != nil {
		return fmt.Errorf("error renaming file: %v", err)
	}
	return nil
}

func main() {
	typemap := map[string]any{
		"timestamp":    int64(0),
		"value":        float64(0),
		"_provider":    "",
		"_id":          "",
		"_fingerprint": int64(0),
		"_filtered":    false,
		"_rule_id":     "",
		"_cluster_id":  "",
		"name":         "",
		"source":       "",
		"hostname":     "",
		"service":      "",
		"message":      "",
		"level":        "",
		"tag_a":        "",
		"tag_b":        "",
		"tag_c":        "",
	}
	schema, err := schemaFromMap("schema", typemap)
	if err != nil {
		log.Fatal(err)
	}

	filename := "parquet-go.parquet"

	var wr MapWriter
	wr, err = NewParquetMapWriter(filename, schema)
	if err != nil {
		log.Fatal(err)
	}

	rows := []map[string]any{}
	for i := 0; i < 10; i++ {
		item := map[string]any{
			"timestamp":    int64(20),
			"value":        float64(1.0265 * float64(i)),
			"_provider":    "provider",
			"_id":          fmt.Sprintf("id-%d", i),
			"_fingerprint": int64(i),
			"_filtered":    i%2 == 0,
			"_rule_id":     "rule-id",
			"_cluster_id":  "cluster-id",
			"name":         "name",
			"source":       "source",
			"hostname":     "hostname",
			"service":      "service",
			"message":      "message",
			"level":        "level",
		}
		switch i % 6 {
		case 0:
			item["tag_a"] = "tag-a"
		case 1:
			item["tag_b"] = "tag-b"
		case 2:
			item["tag_c"] = "tag-c"
		case 3:
			item["tag_a"] = "tag-a"
			item["tag_b"] = "tag-b"
		case 4:
			item["tag_a"] = "tag-a"
			item["tag_c"] = "tag-c"
		case 5:
			item["tag_b"] = "tag-b"
			item["tag_c"] = "tag-c"
		}

		rows = append(rows, item)
	}

	n, err := wr.WriteRows(rows)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("wrote %d rows\n", n)

	if err := wr.Close(); err != nil {
		log.Fatal(err)
	}

	readback(filename)
}

func readback(filename string) {
	rf, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	pf := parquet.NewReader(rf)
	readrows := []LogRow{}
	for {
		var addr LogRow
		err := pf.Read(&addr)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}
		readrows = append(readrows, addr)
	}
	b, ret := json.MarshalIndent(readrows, "", "  ")
	if ret != nil {
		log.Fatal(ret)
	}
	fmt.Println(string(b))
}
