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
    "strconv"
    "sync"
    "os"
    "fmt"
    "strings"
    "time"
    "io/ioutil"
    "net/http"
)

var waitGrp sync.WaitGroup

// Worker func downloads and stores a single segment of the file
// written assuming that this function is executed as a go thread
//
// Inputs:
//    url            - the URL to fetch the file
//    startOffset    - the file offset indicating the start of the segment to download
//    size           - size (aka number of bytes to download and store)
//    dstFile        - local filename to which the segment is to be stored at - uses startOffset
//
func workerFunc (url string, startOffset uint, size uint, dstFile string) {
    rangeOfBytes := "bytes=" + strconv.Itoa(int(startOffset)) + "-" +
                    strconv.Itoa(int(startOffset+size))
    req, _ := http.NewRequest("GET", url, nil)
    fmt.Println("Requesting:", rangeOfBytes)
    req.Header.Add("Range", rangeOfBytes)

    timeout := time.Duration(5 * time.Second)
    cli := &http.Client{ Timeout: timeout}
    rsp, err := cli.Do(req)
    if (err != nil) {
        fmt.Println("Got errored out from HTTP GET.:",rsp)
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
    waitGrp.Done()
}

//
// Type to store the one-time info about the URL to download data:
//  - size of the file
//  - fileName of the downloaded file
//
type urlInfoType struct {
    FileSize uint
    FileName string
}

// Before we download the contents of the file - lets 
// fetch the details of the file to be downloaded first..
// returns the urlInfo and a bool to indicate if it was 
// successful or not. When failed, urlInfo is invalid
func fetchUrlInfo (url string) (urlInfoType, bool) {
    var urlInfo urlInfoType
    res, err := http.Head(url)
    if (err != nil) {
        fmt.Println("Could not get Header...")
        return urlInfo, false
    }
    //fmt.Println("Status:", res.Status)
    // May be there are other successful codes ??
    if (res.Status != "200 OK") {
        fmt.Println(res)
        return urlInfo, false
    }
    size := res.Header["Content-Length"]
    fileSize,_ := strconv.Atoi(size[0])
    urlInfo.FileSize = uint(fileSize)
    urlPieces := strings.Split(url, "/")
    urlInfo.FileName = urlPieces[len(urlPieces)-1]
    //fmt.Println(urlPieces)
    return urlInfo, true
}


func main () {
    if len(os.Args) < 2 {
        fmt.Println("Usage: idm <url>")
    } else {
        url := os.Args[1]
        fmt.Println("Fetching URL:", url)
        urlInfo, ok := fetchUrlInfo(url)
        if (!ok) {
            fmt.Println("Invalid URL..")
        } else {
            file, _ := os.Create(urlInfo.FileName)
            file.Close()
            //Currently hardcoded number of workers
            numWorkers := uint(5)
            for i:= uint(0); i < numWorkers; i++ {
                //fmt.Println("Worker #", i)
                waitGrp.Add(1)
                idx := numWorkers - i -1
                go workerFunc(url, idx*urlInfo.FileSize/numWorkers,
                           urlInfo.FileSize/numWorkers,
                           urlInfo.FileName)
            }
            waitGrp.Wait()
        }
    }
}
