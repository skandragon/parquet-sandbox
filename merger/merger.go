package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"github.com/parquet-go/parquet-go"
)

//
// Example of loading multiple schemas from multiple files and merging them into a single schema,
// then reading records from each file and writing them to a new file, without holding all of the records
// in memory at once, so long as the writer does not hold on to too many unwritten records.
//

var (
	sourcedir     = flag.String("sourcedir", "", "directory containing parquet files to merge")
	outfile       = flag.String("outfile", "", "output file to write merged records to")
	requireFields = flag.String("requireFields", "", "comma separated list of fields that must be present in a file to merge")
)

func main() {
	flag.Parse()

	if *sourcedir == "" {
		log.Fatal("sourcedir is required")
	}

	if *outfile == "" {
		*outfile = "merged.parquet"
	}

	rfields := strings.Split(*requireFields, ",")
	files := findFiles(*sourcedir)

	merge(*outfile, rfields, files)
}

func findFiles(dir string) []string {
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Fatal(err)
	}
	var out []string
	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if strings.HasSuffix(file.Name(), ".parquet") {
			out = append(out, dir+"/"+file.Name())
		}
	}
	return out
}

func merge(outfile string, rfields, files []string) {
	mergedSchema := map[string]parquet.Node{}
	fileSchema := map[string]*parquet.Schema{}
	for _, file := range files {
		nodes, err := getSchemaNodes(file)
		if err != nil {
			log.Fatal(err)
		}
		keep := true
		for _, field := range rfields {
			if _, ok := nodes[field]; !ok {
				keep = false
				break
			}
		}
		if !keep {
			continue
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

	outf, err := os.Create(outfile)
	if err != nil {
		log.Fatal(err)
	}
	wc, err := parquet.NewWriterConfig(schema, parquet.Compression(&parquet.Zstd))
	if err != nil {
		log.Fatalf("error creating writer config: %v", err)
	}
	writer := parquet.NewGenericWriter[map[string]any](outf, wc)
	for file := range fileSchema {
		stat, err := os.Stat(file)
		if err != nil {
			log.Fatal(err)
		}
		inf, err := os.Open(file)
		if err != nil {
			log.Fatal(err)
		}
		if err := copyFromFile(inf, stat.Size(), writer, fileSchema[file]); err != nil {
			inf.Close()
			log.Fatal(err)
		}
		if err := inf.Close(); err != nil {
			log.Fatal(err)
		}
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
		"UINT8":      parquet.Optional(parquet.Uint(8)),
		"UINT16":     parquet.Optional(parquet.Uint(16)),
		"UINT32":     parquet.Optional(parquet.Uint(32)),
		"UINT64":     parquet.Optional(parquet.Uint(64)),
		"FLOAT":      parquet.Optional(parquet.Leaf(parquet.FloatType)),
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
