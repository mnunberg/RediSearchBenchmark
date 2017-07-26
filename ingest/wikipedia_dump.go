package ingest

import (
	"io"

	"fmt"

	"github.com/RedisLabs/RediSearchBenchmark/index"
	wp "github.com/dustin/go-wikiparse"
)

type wikipediaDumpReader struct {
}

// Dummy
func (wr *wikipediaDumpReader) LoadScores(string) error {
	return nil
}

func readWikiDump(p wp.Parser, ch chan index.Document) {
	var err error
	for err != io.EOF {
		page, err := p.Next()
		if err != nil {
			break
		}
		// fmt.Println("Parsing nexgt document...")
		doc := index.NewDocument(fmt.Sprintf("WP_%d", page.ID), 1.0).
			Set("title", page.Title).
			Set("body", page.Revisions[0].Text).
			Set("url", wp.URLForFile(page.Title))
		ch <- doc
	}

	fmt.Println("Parsing done!")
}

func (wr *wikipediaDumpReader) Read(r io.Reader) (<-chan index.Document, error) {
	parser, err := wp.NewParser(r)
	if err != nil {
		fmt.Println("Couldn't open file!")
		panic(err)
	}

	ch := make(chan index.Document, 1000)
	go readWikiDump(parser, ch)
	return ch, nil
}

// NewWikipediaDumpReader Create new reader for wikipedia article dumps
func NewWikipediaDumpReader() DocumentReader {
	return &wikipediaDumpReader{}
}
