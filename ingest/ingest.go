package ingest

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/RedisLabs/RediSearchBenchmark/index"
)

// DocumentReader implements parsing a data source and yielding documents
type DocumentReader interface {
	Read(io.Reader) (<-chan index.Document, error)
	LoadScores(string) error
}

type IngestOptions struct {
	ChunkSize   int
	Concurrency int
	Limit       int
}

// IngestDocuments ingests documents into an index using a DocumentReader
func IngestDocuments(fileName string, r DocumentReader, idx index.Index, opts interface{}, iopts IngestOptions) error {

	// open the file
	fp, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer fp.Close()

	// run the reader and let it spawn a goroutine
	ch, err := r.Read(fp)
	if err != nil {
		return err
	}

	chunk := iopts.ChunkSize
	if chunk == 0 {
		chunk = 1000
	}

	docs := make([]index.Document, chunk*2)

	//freqs := map[string]int{}
	st := time.Now()

	i := 0
	n := 0
	dt := 0
	totalDt := 0
	doch := make(chan index.Document, 100)

	for w := 0; w < iopts.Concurrency; w++ {
		go func(doch chan index.Document) {
			for doc := range doch {
				if doc.Id != "" {
					//fmt.Println(doc)
					idx.Index([]index.Document{doc}, opts)
				}
			}
		}(doch)
	}

	for doc := range ch {

		docs[i%chunk] = doc

		if doc.Score == 0 {
			doc.Score = 0.0000001
		}

		for k, v := range doc.Properties {
			switch s := v.(type) {
			case string:
				dt += len(s) + len(k)
				totalDt += len(s) + len(k)
			}
		}

		i++
		n++
		if i%chunk == 0 {
			//var _docs []index.Document
			for _, d := range docs {
				doch <- d
			}

		}

		// print report every CHUNK documents
		if i%chunk == 0 {
			rate := float32(n) / (float32(time.Since(st).Seconds()))
			dtrate := float32(dt) / (float32(time.Since(st).Seconds())) / float32(1024*1024)
			fmt.Println(i, "rate: ", rate, "d/s. data rate: ", dtrate, "MB/s", "total data ingested", float32(totalDt)/float32(1024*1024))
			st = time.Now()
			n = 0
			dt = 0
		}

		if iopts.Limit > 0 && i >= iopts.Limit {
			return nil
		}
	}

	if i%chunk != 0 {
		go idx.Index(docs[:i%chunk], opts)
		return nil
	}
	return nil
}
