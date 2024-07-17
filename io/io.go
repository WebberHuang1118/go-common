package io

/*
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <errno.h>

// Safe_pwrite is a wrapper around pwrite(2) that retries on EINTR.
ssize_t safe_pwrite(int fd, const void *buf, size_t count, off_t offset)
{
        while (count > 0) {
                ssize_t r = pwrite(fd, buf, count, offset);
                if (r < 0) {
                        if (errno == EINTR)
                                continue;
                        return -errno;
                }
                count -= r;
                buf = (char *)buf + r;
                offset += r;
        }
        return 0;
}
ssize_t safe_pread(int fd, void *buf, size_t count, off_t offset)
{
        size_t cnt = 0;
        char *b = (char*)buf;

        while (cnt < count) {
                ssize_t r = pread(fd, b + cnt, count - cnt, offset + cnt);
                if (r <= 0) {
                        if (r == 0) {
                                // EOF
                                return cnt;
                        }
                        if (errno == EINTR)
                                continue;
                        return -errno;
                }
                cnt += r;
        }
        return cnt;
}
ssize_t safe_pread_exact(int fd, void *buf, size_t count, off_t offset)
{
        ssize_t ret = safe_pread(fd, buf, count, offset);
        if (ret < 0)
                return ret;
        if ((size_t)ret != count)
                return -EDOM;
        return 0;
}
// Write data to a file descriptor with O_DIRECT
int directWrite(int fd, void *buf, size_t count, off_t offset) {
    return safe_pwrite(fd, buf, count, offset);
}
// Read data from a file descriptor with O_DIRECT
int directRead(int fd, void *buf, size_t count, off_t offset) {
	return safe_pread(fd, buf, count, offset);
}
*/
import "C"

import (
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"
)

const (
	baseAlignSize = 4096
	maxChunkSize  = 4194304
	producerNum   = 8
	workerNum     = 6
	BLKGETSIZE64  = 0x80081272
)

type Content struct {
	offset uint64
	buf    []byte
}

func Copy(src *os.File, dst *os.File, chunkSize int) error {
	var ioError error
	ioProducer := func(seqID int, src *os.File, chunkNums int, chunkSize int, objs chan<- Content, errChan chan error) {
		for i := seqID; i < chunkNums; i += producerNum {
			buf := make([]byte, chunkSize)
			_, err := PReadExact(src, buf, chunkSize, uint64(i*chunkSize))
			if err != nil {
				fmt.Printf("Error reading data: %v\n", err)
				errChan <- err
				continue
			}
			objs <- Content{offset: uint64(i * chunkSize), buf: buf}
		}
	}

	ioWorker := func(ioQueue chan Content, wg *sync.WaitGroup, errChan chan error) {
		for {
			obj, ok := <-ioQueue
			if !ok {
				return
			}
			_, err := PWrite(dst, obj.buf, len(obj.buf), obj.offset)
			wg.Done()
			if err != nil {
				fmt.Printf("Error writing data: %v\n", err)
				errChan <- err
			}
		}
	}

	srcSize, err := getSourceVolSize(src)
	if err != nil {
		fmt.Printf("Error getting file size: %v\n", err)
		return fmt.Errorf("error getting file size")
	}

	if chunkSize > maxChunkSize {
		return fmt.Errorf("chunk size is too large, max chunk size is %d", maxChunkSize)
	}
	if chunkSize%baseAlignSize != 0 {
		return fmt.Errorf("chunk size must be a multiple of %d", baseAlignSize)
	}

	// Create a channel to receive the results
	ioQ := make(chan Content, producerNum)
	completedChan := make(chan struct{})
	errChan := make(chan error)

	chunkNums := int(srcSize) / chunkSize
	if int(srcSize)%chunkSize != 0 {
		chunkNums++
	}
	var wg sync.WaitGroup
	wg.Add(chunkNums)

	for i := 0; i < producerNum; i++ {
		go ioProducer(i, src, chunkNums, chunkSize, ioQ, errChan)
	}
	for i := 0; i < workerNum; i++ {
		go ioWorker(ioQ, &wg, errChan)
	}

	go func() {
		wg.Wait()
		close(ioQ)
		close(completedChan)
	}()

	select {
	case <-completedChan:
		break
	case errVal := <-errChan:
		ioError = errVal
		break
	}

	close(errChan)

	if ioError != nil {
		fmt.Printf("Error: %v\n", ioError)
		return ioError
	}
	return nil

}

func Write(dst *os.File, data []byte, size int, chunkSize int) error {
	var ioError error
	ioProducer := func(seqID, chunkNums int, objs chan<- Content) {
		for i := seqID; i < chunkNums; i += producerNum {
			start := i * chunkSize
			end := start + chunkSize
			if i == chunkNums-1 {
				end = len(data)
			}
			chunk := data[start:end]
			objs <- Content{offset: uint64(start), buf: chunk}
		}
	}

	ioWorker := func(ioQueue chan Content, wg *sync.WaitGroup, errChan chan error) {
		for {
			obj, ok := <-ioQueue
			if !ok {
				return
			}
			_, err := PWrite(dst, obj.buf, len(obj.buf), obj.offset)
			wg.Done()
			if err != nil {
				fmt.Printf("Error writing data: %v\n", err)
				errChan <- err
			}
		}
	}

	if chunkSize > maxChunkSize {
		return fmt.Errorf("chunk size is too large, max chunk size is %d", maxChunkSize)
	}
	if chunkSize%baseAlignSize != 0 {
		return fmt.Errorf("chunk size must be a multiple of %d", baseAlignSize)
	}

	// Calculate the number of chunks based on the alignment size
	numChunks := size / chunkSize
	if len(data)%chunkSize != 0 {
		numChunks++
	}

	// Create a channel to receive the results
	ioQ := make(chan Content, producerNum)
	completedChan := make(chan struct{})
	errChan := make(chan error)

	var wg sync.WaitGroup
	wg.Add(numChunks)

	for i := 0; i < producerNum; i++ {
		go ioProducer(i, numChunks, ioQ)
	}
	for i := 0; i < workerNum; i++ {
		go ioWorker(ioQ, &wg, errChan)
	}

	go func() {
		wg.Wait()
		close(ioQ)
		close(completedChan)
	}()

	select {
	case <-completedChan:
		break
	case errVal := <-errChan:
		ioError = errVal
		break
	}

	close(errChan)

	if ioError != nil {
		fmt.Printf("Error: %v\n", ioError)
		return ioError
	}
	return nil
}

func PWrite(dst *os.File, data []byte, size int, offset uint64) (int, error) {
	var writeBuffer unsafe.Pointer
	if C.posix_memalign((*unsafe.Pointer)(unsafe.Pointer(&writeBuffer)), C.size_t(baseAlignSize), C.size_t(size)) != 0 {
		fmt.Printf("Error allocating aligned memory\n")
		return 0, fmt.Errorf("error allocating aligned memory")
	}
	defer C.free(unsafe.Pointer(writeBuffer))

	// Copy the Go data into the C buffer
	C.memcpy(writeBuffer, unsafe.Pointer(&data[0]), C.size_t(size))

	// Call the C function to write with O_DIRECT
	ret := C.directWrite(C.int(dst.Fd()), writeBuffer, C.size_t(size), C.off_t(offset))
	if ret < 0 {
		fmt.Printf("Error writing data: %v\n", ret)
		return 0, fmt.Errorf("error writing data")
	}

	return int(ret), nil
}

func PReadExact(src *os.File, buf []byte, count int, offset uint64) (int, error) {
	var readBuffer unsafe.Pointer
	if C.posix_memalign((*unsafe.Pointer)(unsafe.Pointer(&readBuffer)), C.size_t(count), C.size_t(count)) != 0 {
		fmt.Printf("Error allocating aligned memory\n")
		return 0, fmt.Errorf("error allocating aligned memory")
	}
	defer C.free(unsafe.Pointer(readBuffer))

	// Call the C function to read with O_DIRECT
	ret := C.directRead(C.int(src.Fd()), readBuffer, C.size_t(count), C.off_t(offset))
	if ret < 0 {
		fmt.Printf("Error reading data: %v\n", ret)
		return 0, fmt.Errorf("error reading data")
	}

	// Copy the C data into the Go buffer
	C.memcpy(unsafe.Pointer(&buf[0]), readBuffer, C.size_t(count))

	return count, nil
}

func getSourceVolSize(src *os.File) (uint64, error) {

	var srcSize uint64
	srcInfo, err := src.Stat()
	if err != nil {
		return 0, err
	}

	if srcInfo.Mode().IsRegular() {
		// file size should not be negative, directly return as uint64
		return uint64(srcInfo.Size()), nil
	}

	if (srcInfo.Mode() & os.ModeDevice) != 0 {
		_, _, err := syscall.Syscall(
			syscall.SYS_IOCTL,
			src.Fd(),
			BLKGETSIZE64,
			uintptr(unsafe.Pointer(&srcSize)),
		)
		if err != 0 {
			return 0, fmt.Errorf("error getting file size: %v", err)
		}
		return srcSize, nil
	}

	return 0, fmt.Errorf("unsupported file type: %v", srcInfo.Mode())
}
