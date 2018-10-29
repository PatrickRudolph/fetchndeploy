package main

import "time"
import "flag"
import "fmt"
import "net/http"
import "os"
import "io"
import "errors"
import "strconv"
import "reflect"

import "compress/bzip2"
import "compress/gzip"
import "github.com/ulikunitz/xz"

import "github.com/h2non/filetype"

import "crypto/md5" 


//    import "encoding/hex" 

func min(a, b int64) int64 {
    if a < b {
        return a
    }
    return b
}

func getMimeTyeFromURI(uri string, timeout time.Duration) (string, error) {
        var netClient = &http.Client{
                Timeout: time.Second * timeout,
        }

        head, err := http.Head(uri)
        if err != nil {
		return "unknown", err
        } 

        if head.StatusCode != http.StatusOK {
		return "unknown", errors.New("HTTP did not return 200 OK")
        }

        // the Header "Content-Length" will let us know
        // the total file size to download
        size, _ := strconv.ParseInt(head.Header.Get("Content-Length"), 0, 64)

        resp, err := netClient.Get(uri)
        if err != nil {
                return "unknown", err
        }
        defer resp.Body.Close()

        buffer := make([]byte, min(261, size))

	n, err := io.ReadFull(resp.Body, buffer)
	if err != nil || n == 0 {
		return "unknown", err
	}

	kind, err := filetype.Match(buffer)
	if err != nil {
		return "unknown", err
	}

	return kind.MIME.Value, nil
}

func downloadFile(chunkSize int64, uri string, timeout time.Duration, c chan<- []byte) {
        var netClient = &http.Client{
                Timeout: time.Second * timeout,
        }

        head, err := http.Head(uri)
        if err != nil {
                return
        }

        if head.StatusCode != http.StatusOK {
                return
        }

        // the Header "Content-Length" will let us know
        // the total file size to download
        size, _ := strconv.ParseInt(head.Header.Get("Content-Length"), 0, 64)

        resp, err := netClient.Get(uri)
        if err != nil {
		fmt.Errorf("Failed to get file %s", err)
		return
        }
        defer resp.Body.Close()

        fmt.Println("Starting download ...")
	var i int64 = 0
	for {
                buffer := make([]byte, chunkSize)

        	part, err := io.ReadFull(resp.Body, buffer)
        	if err != nil {
			fmt.Printf("Download failed with %s\n", err)
			break
        	}
		if part == 0 {
			break
		}
		c <- buffer

		fmt.Printf("\rDownloading %d%% ...", 100 * i / size)
		i += int64(len(buffer))
	}

	close(c)
	fmt.Println("Download done")
}

func readFileChunks(f* os.File, chunkSize int, c chan<- []byte) {

	defer close(c)
	defer f.Close()
	for {
        	buffer := make([]byte, chunkSize)

		bytesread, err := f.Read(buffer)
		if err != nil {
			if err != io.EOF {
				fmt.Println(err)
			}   
			break
		}

		if bytesread == 0 {
			break
		}
		c <- buffer
	}
}

func decompress(mime string, co chan<- []byte, ci <-chan []byte) {
        if mime != "application/octet-stream" {
                fmt.Printf("Decompressing %s...\n", mime)
        }

	defer close(co)

        if mime == "application/x-xz" {
                reader, err := xz.NewReader(newChanReader(ci))
                if err != nil {
                        fmt.Println(err)
                        return
                }
                if _, err := io.Copy(newChanWriter(co), reader); err != nil {
                        fmt.Printf("io.Copy error %s\n", err)
                        return
                }
        } else if mime == "application/gzip" {
                reader, err := gzip.NewReader(newChanReader(ci))
                if err != nil {
                        fmt.Println(err)
                        return
                }
                if _, err := io.Copy(newChanWriter(co), reader); err != nil {
                        fmt.Printf("io.Copy error %s\n", err)
                        return
                }
        } else if mime == "application/x-bzip2" {
                reader := bzip2.NewReader(newChanReader(ci))
                if _, err := io.Copy(newChanWriter(co), reader); err != nil {
                        fmt.Printf("io.Copy error %s\n", err)
                        return
                }
        } else if mime == "application/octet-stream" {
                if _, err := io.Copy(newChanWriter(co), newChanReader(ci)); err != nil {
                        fmt.Errorf("io.Copy error %s", err)
                        return
                }
        }
}


type BufferHashed struct {
        buf []byte
        hash [md5.Size]byte
}

func hashChunks(chunkSize int64, co chan<- BufferHashed, ci <-chan []byte) {

        for {
	        buffer := make([]byte, chunkSize)

		b, err := io.ReadFull(newChanReader(ci), buffer)
                if err != nil {
			fmt.Printf("Error hashing data %s\n", err)
                        break
                }
		if b == 0 {
			break
		}
                co <- BufferHashed{buf: buffer, hash: md5.Sum(buffer)}
        }
        close(co)
}


func compareAndWriteChunks(f* os.File, ci1 <-chan BufferHashed, ci2 <-chan BufferHashed) {
	var processed int64 = 0
	var written int64 = 0
	defer f.Sync()
        defer f.Close()
        for {
                b1, ok := <-ci1
                if ok == false {
                        break
                }
		b2, ok := <-ci2
                if ok == false {
                        break
                }
		if len(b1.buf) != len(b2.buf) {
			fmt.Printf("Error buffersize doesn't match")
			break
		}
//		fmt.Printf("%x %x\n", b1.hash, b2.hash)
		if reflect.DeepEqual(b1.hash, b2.hash) == false {

			written += int64(len(b1.buf))
			off, err := f.Seek(processed, os.SEEK_SET)
			if off != processed || err != nil {
				fmt.Printf("Error seeking file\n")
				break
			}
			n, err := f.WriteAt(b1.buf, processed)
			if n != len(b1.buf) || err != nil {
				fmt.Printf("Error writing file\n")
				break
			}
			f.Sync()
			fmt.Printf("Write at %x\n", processed)
		}
                processed += int64(len(b1.buf))
        }
	fmt.Printf("processed %d, written %d\n", processed, written)
}


func main() { os.Exit(mainReturnWithCode()) }

func mainReturnWithCode() int {
	urlPtr := flag.String("url", "", "The download URI")
	timeoutPtr := flag.Int("timeout", 180, "Connection timeout in seconds")
	mimeType := flag.String("mimetype", "application/octet-stream", "MIME type of the file to download")
	destFilePtr := flag.String("dest", "", "Destination path where to store the downloaded file")
	chunkSizePtr := flag.Int("chunksize", 1024, "Size of chunks in KiB")

        flag.Parse()
	*chunkSizePtr *= 1024 

	if *urlPtr == "" {
		fmt.Printf("Must specify an URI using --url\n")
		return 1
	}

        if *destFilePtr == "" {
                fmt.Printf("Must specify a destination path using --dest\n")
                return 1
        }

        mime, err := getMimeTyeFromURI(*urlPtr, time.Duration(*timeoutPtr))
	if err != nil {
		fmt.Println("Failed to automatically detect compression")
		mime = *mimeType	
	}

	fmt.Println("Crrating channels")
        c1 := make(chan []byte)
        go downloadFile(int64(*chunkSizePtr), *urlPtr, time.Duration(*timeoutPtr), c1)
        c2 := make(chan []byte)
	go decompress(mime, c2, c1)

	if _, err := os.Stat(*destFilePtr); os.IsNotExist(err) {
		fw, err := os.Create(*destFilePtr)
		if err != nil {
			fmt.Printf("Failed to create file\n")
			return 1
		}
		defer fw.Close()

		for {
			b, ok := <-c2
			if ok == false {
				break
			}
		        _, err := fw.Write(b)
			if err != nil {
				break
			}
		}
		fw.Sync()
	} else {
		c3 := make(chan BufferHashed)
        	go hashChunks(int64(*chunkSizePtr), c3, c2)

        	c4 := make(chan []byte)
        	fr, err := os.OpenFile(*destFilePtr, os.O_RDONLY, 0644)
        	if err != nil {
			fmt.Printf("Failed to open file\n")
                	return 1
        	}
		go readFileChunks(fr, *chunkSizePtr, c4)
		c5 := make(chan BufferHashed)
		go hashChunks(int64(*chunkSizePtr), c5, c4)

                fw, err := os.OpenFile(*destFilePtr, os.O_WRONLY, 0644)
                if err != nil {
                        fmt.Printf("Failed to open file\n")
                        return 1
                }

		compareAndWriteChunks(fw, c3, c5)
	}

	fmt.Printf("Downloaded %s\n", *urlPtr)
	return 0
}
