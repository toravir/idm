/*
Copyright 2017 Ravi Raju <toravir@yahoo.com>

Redistribution and use in source and binary forms, with or without modification, are
permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this list
of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice, this
list of conditions and the following disclaimer in the documentation and/or other
materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY
EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT
SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED
TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR
BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
SUCH DAMAGE.

*/
package main

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	//"io"
	"bufio"
	"flag"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
)

const numberOfWorkers = 10
const httpHeaderSize = 8 * 1024    // 8K
const maxRequestSize = 1024 * 1024 //1MB

var verboseMode bool
var urlToDownload string
var runInParallel bool
var useHttpPkg bool
var numWorkerThreads uint

func init() {
	flag.BoolVar(&verboseMode, "v", false, "Verbose Mode")
	flag.BoolVar(&verboseMode, "V", false, "Verbose Mode")
	flag.BoolVar(&runInParallel, "p", true, "In Parallel Download Mode")
	flag.BoolVar(&useHttpPkg, "h", false, "Use HTTP package instead of TCP Pkg (developer)")
	flag.UintVar(&numWorkerThreads, "n", 6, "Number of threads performing the download")

	flag.StringVar(&urlToDownload, "u", "", "URL to Fetch (mandatory)")
}

//
// Type to store the one-time info about the URL to download data:
//  - size of the file
//  - fileName of the downloaded file
//
type urlInfoType struct {
	FileSize uint
	FileName string
	urlData  *url.URL
}

func logMsg(a ...interface{}) (n int, err error) {
	if verboseMode {
		return fmt.Println(time.Now(), fmt.Sprint(a...))
	}
	return 0, nil
}

func workerFuncParallel(urlInfo urlInfoType, startOffset uint, endOffset uint, dstFile string, waitGrp *sync.WaitGroup) {
    workerFunc(urlInfo, startOffset, endOffset, dstFile)
	waitGrp.Done()
}

// Worker func downloads and stores a single segment of the file
// written assuming that this function is executed as a go thread
//
// Inputs:
//    url            - the URL to fetch the file
//    startOffset    - the file offset indicating the start of the segment to download
//    endOffset      - the file offset indicating the END of the segment to download (inclusive)
//    dstFile        - local filename to which the segment is to be stored at - uses startOffset
//
func workerFunc(urlInfo urlInfoType, startOffset uint, endOffset uint, dstFile string) {
	var err error
	var conn net.Conn
	var rdr *bufio.Reader
	var f *os.File

	logMsg("Worker to download from:", urlInfo, " from:", startOffset, " to:", endOffset)
	readBuf := make([]byte, maxRequestSize+httpHeaderSize)
	connDown := true
	rspHdrDone := false
	fileIsOpen := false
	resendReq := false
	bytesReadInThisRequest := 0

	if urlInfo.urlData.Scheme == "http" { //HTTP Protocol
		for startOffset < endOffset {
			//time.Sleep(2*time.Second)
			if connDown {
				conn, err = net.Dial("tcp4", urlInfo.urlData.Host)
				logMsg("Setting up new Connection...")
				if err == nil {
					connDown = false
					resendReq = true
				} else {
					logMsg("Connection failed to Establish - trying after 5 sec")
					time.Sleep(5 * time.Second)
					continue
				}
			}
			if resendReq {
				var rangeOfBytes string
				if endOffset-startOffset > maxRequestSize {
					rangeOfBytes = "bytes=" + strconv.Itoa(int(startOffset)) + "-" +
						strconv.Itoa(int(startOffset+maxRequestSize))
				} else {
					rangeOfBytes = "bytes=" + strconv.Itoa(int(startOffset)) + "-" +
						strconv.Itoa(int(endOffset))
				}
				bytesReadInThisRequest = 0
				logMsg("Requesting:", rangeOfBytes)
				fmt.Fprintf(conn, "GET "+urlInfo.urlData.Path+" HTTP/1.1\r\n")
				fmt.Fprintf(conn, "Host: "+urlInfo.urlData.Host+"\r\n")
				fmt.Fprintf(conn, "Range: "+rangeOfBytes+"\r\n")
				fmt.Fprintf(conn, "\r\n")
				resendReq = false
				rspHdrDone = false
				rdr = bufio.NewReaderSize(conn, maxRequestSize+httpHeaderSize)
			}
			rdCnt := 0
			conn.SetReadDeadline(time.Now().Add(20 * time.Second))
			start := time.Now()
			rdCnt, err = rdr.Read(readBuf)
			end := time.Now()
			bytesReadInThisRequest += rdCnt
			logMsg("Read returned ", rdCnt, "/", bytesReadInThisRequest, " bytes")
			elapsed := int64(end.Sub(start))
			if err != nil {
				logMsg("Read returned Error:", err)
				conn.Close()
				connDown = true
				resendReq = true
				rspHdrDone = false
				continue
			}
			if !rspHdrDone {
				hdrs := strings.Split(string(readBuf), "\n")
				byteCnt := 0
				for _, v := range hdrs {
					//logMsg("HDR:", v)
					// Need to parse the Headers to be sure - later
					byteCnt += len(v) + 1
					if v == "\r" {
						rspHdrDone = true
						rdCnt -= byteCnt
						readBuf = readBuf[byteCnt:]
						break
					}
				}
			}
			if rspHdrDone && rdCnt != 0 {
				// rdCnt is number of bytes -> divide by 1024 to get KB
				// elapsed is nano seconds -> multiple by 10^9 to get Seconds
				if elapsed == 0 {
					elapsed = 1
				}
				//speed := int64(rdCnt * 1000 * 1000 * 1000) / (elapsed * 1024)
				//logMsg("Speed: ", speed, " KB/s")
				if !fileIsOpen {
					f, err = os.OpenFile(dstFile, os.O_RDWR, 0644)
					if err != nil {
						//todo handle this later
						panic(err)
					}
					defer f.Close()
					fileIsOpen = true
				}
				if fileIsOpen {
					if _, err := f.WriteAt(readBuf[:rdCnt], int64(startOffset)); err != nil {
						//todo hanndle this later
						panic(err)
					}
				}
				if len(readBuf) != maxRequestSize+httpHeaderSize {
					readBuf = make([]byte, maxRequestSize+httpHeaderSize)
				}
				startOffset += uint(rdCnt)
				logMsg("Read ", rdCnt) //, " bytes: ", string(readBuf))
			} else {
				logMsg("rspHdr NOT Done!...")
			}
		}
	}
}

func workerFuncHttp(url string, startOffset uint, endOffset uint, dstFile string) {
	size := endOffset - startOffset + 1
	rangeOfBytes := "bytes=" + strconv.Itoa(int(startOffset)) + "-" +
		strconv.Itoa(int(startOffset+size))
	req, _ := http.NewRequest("GET", url, nil)
	logMsg("Requesting:", rangeOfBytes)
	req.Header.Add("Range", rangeOfBytes)

	timeout := time.Duration(5 * time.Second)
	cli := &http.Client{Timeout: timeout}
	rsp, err := cli.Do(req)
	if err != nil {
		logMsg("Got errored out from HTTP GET.:", rsp)
		panic(err)
	}
	defer rsp.Body.Close()

	rdr, _ := ioutil.ReadAll(rsp.Body)
	f, err := os.OpenFile(dstFile, os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	if _, err := f.WriteAt([]byte(rdr), int64(startOffset)); err != nil {
		panic(err)
	}
}

// Before we download the contents of the file - lets
// fetch the details of the file to be downloaded first..
// returns the urlInfo and a bool to indicate if it was
// successful or not. When failed, urlInfo is invalid
func fetchUrlInfo(urlStr string) (urlInfoType, bool) {
	var urlInfo urlInfoType
	var err error

	urlInfo.urlData, err = url.Parse(urlStr)
	if err != nil {
		return urlInfo, false
	}
	if urlInfo.urlData.Scheme == "" {
		logMsg("No protocol specified.. assuming http...")
		urlInfo.urlData.Scheme = "http"
	}
	if urlInfo.urlData.Scheme != "http" {
		logMsg("URL:", urlStr)
		logMsg("Only HTTP URLs supported...")
		return urlInfo, false
	}
	if !strings.ContainsAny(urlInfo.urlData.Host, ":") {
		if urlInfo.urlData.Scheme == "http" {
			urlInfo.urlData.Host += ":80"
		} else if urlInfo.urlData.Scheme == "https" {
			urlInfo.urlData.Host += ":443"
		} else {
			logMsg("Dunno default port for Protocol:", urlInfo.urlData.Scheme)
			os.Exit(1)
		}
	}
	if urlInfo.urlData.Fragment != "" {
		logMsg("Ignoring url Fragment:", urlInfo.urlData.Fragment)
	}
	logMsg("URL:", urlStr)
	res, err := http.Head(urlStr)
	if err != nil {
		logMsg("Could not get Header...")
		return urlInfo, false
	}
	//logMsg("Status:", res.Status)
	// May be there are other successful codes ??
	if res.Status != "200 OK" {
		logMsg(res)
		return urlInfo, false
	}
	size := res.Header["Content-Length"]
	fileSize, _ := strconv.Atoi(size[0])
	urlInfo.FileSize = uint(fileSize)
	urlPieces := strings.Split(urlInfo.urlData.Path, "/")
	urlInfo.FileName = urlPieces[len(urlPieces)-1]
	//logMsg(urlInfo)
	return urlInfo, true
}

func main() {
	flag.Parse()
	if len(os.Args) < 2 || urlToDownload == "" {
		flag.PrintDefaults()
		os.Exit(1)
	} else {
		url := urlToDownload
		logMsg("Fetching URL:", url)
		urlInfo, ok := fetchUrlInfo(url)
		if !ok {
			logMsg("Invalid URL..")
		} else {
            var waitGrp sync.WaitGroup
			file, _ := os.Create(urlInfo.FileName)
			file.Close()
			//Currently hardcoded number of workers
			numWorkers := numWorkerThreads
			for i := uint(0); i < numWorkers; i++ {
				logMsg("Worker #", i)
				//idx := numWorkers - i -1
				idx := i
				startOffset := idx * urlInfo.FileSize / numWorkers
				endOffset := (idx+1)*urlInfo.FileSize/numWorkers - 1
				if !useHttpPkg {
					if runInParallel {
                        waitGrp.Add(1)
						go workerFuncParallel(urlInfo, startOffset, endOffset,
							urlInfo.FileName, &waitGrp)
					} else {
						workerFunc(urlInfo, startOffset, endOffset,
							urlInfo.FileName)
					}
				} else {
					if runInParallel {
						go workerFuncHttp(url, startOffset, endOffset,
							urlInfo.FileName)
					} else {
						workerFuncHttp(url, startOffset, endOffset,
							urlInfo.FileName)
					}
				}
			}
            if runInParallel {
                waitGrp.Wait()
            }
		}
	}
}
