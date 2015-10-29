package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"reflect"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/service/s3"
	"launchpad.net/gommap"
)

var expectedDelims = flag.Uint("numDelims", 0, "specify the number of times the delimiter is expected to appear in each line")
var bufferSize = flag.Uint("bufferSize", 8196, "specify the buffer size to use why scanning through files")
var srcBucket = flag.String("srcBucket", "", "specify the full path the bucket that contains the files needing fixing")
var destBucket = flag.String("destBucket", "", "specify a full path to the bucket where the results will be stored")
var badBucket = flag.String("badBucket", "", "specify a full path to the bucket where the unfixable bad results will be stored")
var objPrefix = flag.String("objPrefix", "", "specify the prefix of attached to the objects in question")

func checkFlags() {
	flag.Parse()

	if *expectedDelims == 0 || *srcBucket == "" || *destBucket == "" || *badBucket == "" || *objPrefix == "" {
		flag.PrintDefaults()
		return
	}
}

func main() {
	// validate & parse the flags sent into the command
	checkFlags()

	// check existence of s3 bucket as src
	objects, _ := getObjectsInBucket()
	fmt.Println(*objects.Contents[0].Key)
	// get array of all object names - use objects.Contents
	// loop a goroutine for each file to:
	//// -stream object to a mmapped file
	//// -fix the lines
	//// -upload bad rows to separate files
	//// -reupload to s3
	// when all are done
}

func getObjectsInBucket() (*s3.ListObjectsOutput, error) {
	defaults.DefaultConfig.Region = aws.String("us-west-2")

	svc := s3.New(nil)

	params := &s3.ListObjectsInput{
		Bucket: aws.String(*srcBucket), // Required
		Prefix: aws.String(*objPrefix),
	}
	resp, err := svc.ListObjects(params)

	if err != nil {
		// Print the error, cast err to awserr.Error to get the Code and
		// Message from an error.
		fmt.Println(err.Error())
		return resp, err
	}

	// Pretty-print the response data.
	fmt.Println(reflect.TypeOf(resp))
	return resp, nil
}

func fixFile() {
	file, err := os.Open("./data/0000.txt")
	check(err)

	mmap, err := gommap.Map(file.Fd(), gommap.PROT_READ, gommap.MAP_PRIVATE)
	check(err)

	numLines, err := countLines(bytes.NewReader(mmap))
	check(err)

	lines := bytes.SplitN(mmap, []byte{'\n'}, numLines)

	lines[numLines-1] = bytes.Trim(lines[numLines-1], "\n")

	// dear lord, fix this
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

func check(e error) {
	if e != nil {
		log.Fatal(e)
	}
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
