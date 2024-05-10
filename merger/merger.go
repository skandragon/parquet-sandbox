package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"

	"github.com/parquet-go/parquet-go"
)

//
// Example of loading multiple schemas from multiple files and merging them into a single schema,
// then reading records from each file and writing them to a new file, without holding all of the records
// in memory at once, so long as the writer does not hold on to too many unwritten records.
//

func main() {
	PrintMemUsage()
	merge()
	PrintMemUsage()
}

// PrintMemUsage outputs the current, total and OS memory being used. As well as the number
// of garage collection cycles completed.
func PrintMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

func merge() {
	files := []string{
		"/Users/mgraff/Downloads/otel-raw-year=2024-month=05-day=10-hour=05-minute=09-metrics_1715317740000_191656998.parquet",
		"/Users/mgraff/Downloads/otel-raw-year=2024-month=05-day=10-hour=05-minute=09-metrics_1715317740000_806476119.parquet",
		"/Users/mgraff/Downloads/otel-raw-year=2024-month=05-day=10-hour=05-minute=09-metrics_1715317750000_589053731.parquet",
		"/Users/mgraff/Downloads/otel-raw-year=2024-month=05-day=10-hour=05-minute=09-metrics_1715317750000_817577671.parquet",
		"/Users/mgraff/Downloads/otel-raw-year=2024-month=05-day=10-hour=05-minute=09-metrics_1715317760000_385929257.parquet",
		"/Users/mgraff/Downloads/otel-raw-year=2024-month=05-day=10-hour=05-minute=09-metrics_1715317760000_494377519.parquet",
		"/Users/mgraff/Downloads/otel-raw-year=2024-month=05-day=10-hour=05-minute=09-metrics_1715317770000_448974791.parquet",
		"/Users/mgraff/Downloads/otel-raw-year=2024-month=05-day=10-hour=05-minute=09-metrics_1715317770000_824349254.parquet",
		"/Users/mgraff/Downloads/otel-raw-year=2024-month=05-day=10-hour=05-minute=09-metrics_1715317780000_253101403.parquet",
		"/Users/mgraff/Downloads/otel-raw-year=2024-month=05-day=10-hour=05-minute=09-metrics_1715317780000_545395187.parquet",
		"/Users/mgraff/Downloads/otel-raw-year=2024-month=05-day=10-hour=05-minute=09-metrics_1715317790000_273852496.parquet",
		"/Users/mgraff/Downloads/otel-raw-year=2024-month=05-day=10-hour=05-minute=09-metrics_1715317790000_732180120.parquet",
	}

	mergedSchema := map[string]parquet.Node{}
	fileSchema := map[string]*parquet.Schema{}
	for _, file := range files {
		nodes, err := getSchemaNodes(file)
		if err != nil {
			log.Fatal(err)
		}
		fileSchema[file] = parquet.NewSchema(file, parquet.Group(nodes))
		for k, v := range nodes {
			if currentNode, ok := mergedSchema[k]; ok {
				if currentNode != v {
					log.Fatalf("schema mismatch: %s", k)
				}
			} else {
				mergedSchema[k] = v
			}
		}
	}
	schema := parquet.NewSchema("merged", parquet.Group(mergedSchema))

	outf, err := os.Create("merged.parquet")
	if err != nil {
		log.Fatal(err)
	}
	wc, err := parquet.NewWriterConfig(schema, parquet.Compression(&parquet.Zstd))
	if err != nil {
		log.Fatalf("error creating writer config: %v", err)
	}
	writer := parquet.NewGenericWriter[map[string]any](outf, wc)
	for _, file := range files {
		stat, err := os.Stat(file)
		if err != nil {
			log.Fatal(err)
		}
		inf, err := os.Open(file)
		if err != nil {
			log.Fatal(err)
		}
		copyFromFile(inf, stat.Size(), writer, fileSchema[file])
		inf.Close()
	}

	if err := writer.Close(); err != nil {
		log.Fatalf("error closing writer: %v", err)
	}
}

func copyFromFile(inf io.ReaderAt, size int64, writer *parquet.GenericWriter[map[string]any], schema *parquet.Schema) error {
	pf, err := parquet.OpenFile(inf, size)
	if err != nil {
		return err
	}
	f := parquet.NewReader(pf, schema)
	defer f.Close()

	for {
		record := map[string]any{}
		err := f.Read(&record)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		n, err := writer.Write([]map[string]any{record})
		if err != nil {
			return err
		}
		if n != 1 {
			return fmt.Errorf("expected to write 1 record, wrote %d", n)
		}
	}
	return nil
}

func getSchemaNodes(fname string) (map[string]parquet.Node, error) {
	stat, err := os.Stat(fname)
	if err != nil {
		return nil, err
	}
	r, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	f, err := parquet.OpenFile(r, stat.Size())
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

		stype, err := schemaTypeToNode(typ, logicalType)
		if err != nil {
			return nil, err
		}
		if currentNode, ok := nodes[schema.Name]; ok {
			if currentNode != stype {
				return nil, fmt.Errorf("schema mismatch: %s", schema.Name)
			}
		} else {
			log.Printf("file %s adds %s -> %s", fname, schema.Name, stype.String())
			nodes[schema.Name] = stype
		}
	}

	return nodes, nil
}

var (
	nodemap = map[string]parquet.Node{
		"INT8":       parquet.Optional(parquet.Int(8)),
		"INT16":      parquet.Optional(parquet.Int(16)),
		"INT32":      parquet.Optional(parquet.Int(32)),
		"INT64":      parquet.Optional(parquet.Int(64)),
		"DOUBLE":     parquet.Optional(parquet.Leaf(parquet.DoubleType)),
		"BOOLEAN":    parquet.Optional(parquet.Leaf(parquet.BooleanType)),
		"BYTE_ARRAY": parquet.Optional(parquet.Leaf(parquet.ByteArrayType)),
	}
	string_node = parquet.Optional(parquet.String())
)

func schemaTypeToNode(typ, logical string) (parquet.Node, error) {
	if logical == "STRING" {
		return string_node, nil
	}
	if node, ok := nodemap[typ]; ok {
		return node, nil
	}
	return nil, fmt.Errorf("unsupported type: %s, logical %s", typ, logical)
}
