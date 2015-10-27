package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"

	//"github.com/aws/aws-sdk-go/aws"
	//"github.com/aws/aws-sdk-go/service/s3"
	"launchpad.net/gommap"
)

var expectedDelims = flag.Uint("numDelims", 0, "specify the number of times the delimiter is expected to appear in each line")
var filename = flag.String("file", "", "specify the file to open")
var destination = flag.String("destination", "", "specify where to write results")
var badRows = flag.String("badRows", "", "specify where to write bad rows")
var bufferSize = flag.Uint("bufferSize", 8196, "specify the buffer size to use why scanning through files")

func checkFlags() {
	flag.Parse()

	if *expectedDelims == 0 || *filename == "" || *destination == "" || *badRows == "" {
		flag.PrintDefaults()
		return
	}

	if _, err := os.Stat(*filename); os.IsNotExist(err) {
		log.Fatalf("no such file or directory: %s", *filename)
	}
}

func main() {
	checkFlags()

	file, err := os.Open(*filename)
	check(err)

	mmap, err := gommap.Map(file.Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
	check(err)

	numLines, err := countLines(bytes.NewReader(mmap))
	check(err)

	lines := bytes.SplitN(mmap, []byte{'\n'}, numLines)

	lines[numLines-1] = bytes.Trim(lines[numLines-1], "\n")

	sub := [][][]byte{
		lines[:(numLines / 4)],
		lines[(numLines / 4):(numLines / 2)],
		lines[(numLines / 2) : (numLines/2)+(numLines/4)],
		lines[(numLines/2)+(numLines/4) : numLines],
	}

	jobs := make(chan []byte)
	results := make(chan string)

	wg := new(sync.WaitGroup)
	for w := 0; w <= 3; w++ {
		wg.Add(1)
		go normalizeLines(jobs, results, wg)
	}

	go func() {
		for i := 0; i <= 3; i++ {
			jobs <- bytes.Join(sub[i], []byte{'\n'})
		}
		close(jobs)
	}()

	go func() {
		wg.Wait()
		close(results)
	}()

	for v := range results {
		if strings.HasPrefix(v, "trimmed:") {
			//fmt.Fprintf(os.Stdout, "%s\n", v[strings.IndexAny(v, ":")+1:])
		} else if strings.HasPrefix(v, "bad:") {
			fmt.Fprintf(os.Stdout, "%s\n", v[strings.IndexAny(v, ":")+1:])
		} else {
			//fmt.Fprintf(os.Stdout, "%s\n", v)
		}
	}

}

func normalizeLines(jobs <-chan []byte, results chan<- string, wg *sync.WaitGroup) {
	//defer wg.Done()

	j := <-jobs
	scanner := bufio.NewScanner(bytes.NewReader(j))
	for scanner.Scan() {
		line := scanner.Text()

		delimCount := uint(strings.Count(line, "|"))
		values := strings.Split(line, "|")

		if delimCount < *expectedDelims {
			var fixBuf bytes.Buffer

			fixBuf.WriteString("trimmed:")
			fixBuf.WriteString(strings.TrimSpace(line))

			scanner.Scan()
			line2 := scanner.Text()
			fixBuf.WriteString(strings.TrimSpace(line2))

			scanner.Scan()
			line3 := scanner.Text()
			fixBuf.WriteString(strings.TrimSpace(line3))

			results <- fixBuf.String()
		} else if delimCount > *expectedDelims {
			var badBuf bytes.Buffer

			badBuf.WriteString("bad:")
			badBuf.WriteString(line)

			results <- badBuf.String()
		} else {
			var goodBuf bytes.Buffer

			for _, value := range values {
				trimmedValue := strings.TrimSpace(value)

				goodBuf.WriteString(trimmedValue)

				if !strings.Contains(value, "\n") {
					goodBuf.WriteString("\\|")
				} else {
					goodBuf.WriteString("\n")
				}
			}

			results <- goodBuf.String()
		}
	}

	wg.Done()
}

func runReducer() {
	//svc := s3.New(&aws.Config{Region: aws.String("us-west-2")})
}

func increment(group string, counter string) {
	//fmt.Fprintf(os.Stderr, "reporter:counter:%s,%s,1\n", group, counter)
}

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
}

func foundEOF(e error) bool {
	if e == io.EOF {
		return true
	} else if e != nil {
		log.Fatal(e)
	}
	return false
}

func countLines(r io.Reader) (int, error) {
	// play with this buffer size to optimize for speed
	buf := make([]byte, *bufferSize)
	lineCount := 0
	lineSep := []byte{'\n'}

	for {
		c, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return lineCount, err
		}

		lineCount += bytes.Count(buf[:c], lineSep)

		if err == io.EOF {
			break
		}
	}

	return lineCount, nil
}
