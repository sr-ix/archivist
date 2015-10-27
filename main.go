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

var runStage = flag.String("stage", "", "specify the stage to run - can be \"mapper\" or \"reducer\"")
var numDelims = flag.Uint("numDelims", 0, "specify the number of times the delimiter is expected to appear in each line")
var filename = flag.String("file", "", "specify the file to open")

func main() {
	flag.Parse()

	if *runStage == "" || *numDelims == 0 || *filename == "" {
		flag.PrintDefaults()
		return
	}

	if _, err := os.Stat(*filename); os.IsNotExist(err) {
		log.Fatalf("no such file or directory: %s", *filename)
	}

	switch *runStage {
	case "mapper":
		runMapper()
	case "reducer":
		runReducer()
	default:
		log.Fatalln("stage must be either \"mapper\" or \"reducer\"")
	}
}

func runMapper() {
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
		fmt.Fprintf(os.Stderr, "working with: %s\n", v)
	}
}

func normalizeLines(jobs <-chan []byte, results chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()

	for j := range jobs {
		in := bufio.NewReader(bytes.NewReader(j))

		line, err := in.ReadString('\n')

		if checkEOF(err) {
			break
		}

		//delimCount := uint(strings.Count(line, "|"))

		values := strings.Split(line, "|")

		for _, value := range values {
			trimmedValue := strings.TrimSpace(value)
			if !strings.Contains(value, "\n") {
				fmt.Printf("%s\\|", trimmedValue)
			} else {
				fmt.Printf("%s\n", trimmedValue)
			}
		}
		results <- string(j)
	}
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

func checkEOF(e error) bool {
	if e == io.EOF {
		return true
	} else if e != nil {
		log.Fatal(e)
	}
	return false
}

func countLines(r io.Reader) (int, error) {
	// play with this buffer size to optimize for speed
	buf := make([]byte, 8196)
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
