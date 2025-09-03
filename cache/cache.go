package cache

import (
	"bufio"
	"crypto/sha256"
	"fmt"
	"github.com/msprev/fzf-bibtex/bibtex"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const debug = false

func IsFresh(cacheDir string, subcache string, bibFiles []string) bool {
	for _, bibFile := range bibFiles {
		if !isFresh(cacheDir, subcache, bibFile) {
			return false
		}
	}
	return true
}

func isFresh(cacheDir, subcache, bibFile string) bool {
	cacheFile := cacheName(bibFile)
	if debug {
		fmt.Println(cacheFile)
	}
	lock(cacheDir, cacheFile)
	defer unlock(cacheDir, cacheFile)
	toRead := filepath.Join(cacheDir, cacheFile+"."+subcache+".timestamp")
	file, err := os.Open(toRead)
	if err != nil {
		if debug {
			fmt.Println("cache does not exist yet " + toRead)
		}
		return false
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	scanner.Scan()
	if err := scanner.Err(); err != nil {
		panic(err)
	}
	timestamp, _ := strconv.ParseInt(scanner.Text(), 10, 64)

	fi, err := os.Stat(bibFile)
	check(err)
	if timestamp < fi.ModTime().UnixNano() {
		if debug {
			fmt.Println("cache is out of date for: " + bibFile)
		}
		return false
	}
	if debug {
		fmt.Println("cache is up to date for: " + bibFile)
	}
	return true
}

func RefreshAndDo(cacheDir string, bibFiles []string, subcache string, formatter func(map[string]string) string, doSomething func(string)) {
	for _, bibFile := range bibFiles {
		refreshAndDo(cacheDir, bibFile, subcache, formatter, doSomething)
	}
}

func refreshAndDo(cacheDir, bibFile, subcache string, formatter func(map[string]string) string, doSomething func(string)) {
	cacheFile := cacheName(bibFile)
	lock(cacheDir, cacheFile)
	defer unlock(cacheDir, cacheFile)

	data := ""
	bibtex.Parse(&data, []string{bibFile}, formatter, doSomething)
	write(filepath.Join(cacheDir, cacheFile+"."+subcache), &data)

	timestamp := time.Now().UnixNano()
	f, err := os.Create(filepath.Join(cacheDir, cacheFile+"."+subcache+".timestamp"))
	check(err)
	defer f.Close()
	f.WriteString(strconv.FormatInt(timestamp, 10))
}

func ReadAndDo(cacheDir string, bibFiles []string, subcache string, formatter func(map[string]string) string, doSomething func(string)) {
	for _, bibFile := range bibFiles {
		if !isFresh(cacheDir, subcache, bibFile) {
			refreshAndDo(cacheDir, bibFile, subcache, formatter, doSomething)
			continue
		}
		readCache(cacheDir, bibFile, subcache, doSomething)
	}
}

func readCache(cacheDir, bibFile, subcache string, doSomething func(string)) {
	cacheFile := cacheName(bibFile)
	lock(cacheDir, cacheFile)
	defer unlock(cacheDir, cacheFile)

	toRead := filepath.Join(cacheDir, cacheFile+"."+subcache)
	if debug {
		fmt.Println("opening: " + toRead)
	}
	file, err := os.Open(toRead)
	check(err)
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		doSomething(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		panic(err)
	}
}

func cacheName(bibFile string) string {
	absPath, _ := filepath.Abs(bibFile)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(absPath)))
}

func lock(cacheDir string, cacheFile string) {
	lockFile := filepath.Join(cacheDir, cacheFile+".lock")
	for {
		f, err := os.OpenFile(lockFile, os.O_CREATE|os.O_EXCL, 0600)
		if err == nil {
			f.Close()
			return
		}
		if os.IsExist(err) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		check(err)
	}
}

func unlock(cacheDir string, cacheFile string) {
	lockFile := filepath.Join(cacheDir, cacheFile+".lock")
	if err := os.Remove(lockFile); err != nil && !os.IsNotExist(err) {
		check(err)
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func write(cacheFile string, data *string) {
	if debug {
		fmt.Println("writing " + cacheFile)
		fmt.Println(*data)
	}
	f, err := os.Create(cacheFile)
	check(err)
	defer f.Close()
	f.WriteString(*data)
}
