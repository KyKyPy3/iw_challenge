package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"unsafe"
)

type Stats struct {
	Min   int64
	Max   int64
	Sum   int64
	Count int64
}

// unsafeString converts []byte to string without allocation
func unsafeString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

func parseIntFast(b []byte) (int64, error) {
	n := len(b)
	if n == 0 {
		return 0, fmt.Errorf("empty input")
	}

	var val int64
	for i := range n {
		c := b[i]
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' {
			continue
		}

		if c >= '0' && c <= '9' {
			val = val*10 + int64(c-'0')
		} else {
			return 0, fmt.Errorf("invalid digit: %c", c)
		}
	}

	return val, nil
}

func main() {
	cpuProfile := os.Getenv("CPU_PROFILE")
	if cpuProfile != "" {
		f, err := os.Create("cpu.prof")
		if err != nil {
			fmt.Fprintf(os.Stderr, "error creating CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "error starting CPU profile: %v\n", err)
			os.Exit(1)
		}
		defer pprof.StopCPUProfile()
	}

	var filePath string
	if len(os.Args) > 1 {
		filePath = os.Args[1]
	} else {
		fmt.Println("You need provide file path in first argument")
	}

	numWorkers := runtime.NumCPU()
	runtime.GOMAXPROCS(numWorkers)

	parts, err := splitFile(filePath, numWorkers)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error splitting file: %v\n", err)
		os.Exit(1)
	}

	resultsChan := make(chan map[string]*Stats, numWorkers)

	for _, part := range parts {
		go processPart(filePath, part.offset, part.size, resultsChan)
	}

	totals := make(map[string]*Stats)
	for range parts {
		result := <-resultsChan

		for endpoint, s := range result {
			end, ok := totals[endpoint]
			if !ok {
				totals[endpoint] = &Stats{
					Min:   s.Min,
					Max:   s.Max,
					Sum:   s.Sum,
					Count: s.Count,
				}
				continue
			}

			end.Min = min(end.Min, s.Min)
			end.Max = max(end.Max, s.Max)
			end.Sum += s.Sum
			end.Count += s.Count
			totals[endpoint] = end
		}
	}

	endpoints := make([]string, 0, len(totals))
	for endpoint := range totals {
		endpoints = append(endpoints, endpoint)
	}
	sort.Strings(endpoints)

	fmt.Fprint(os.Stdout, "{\n  \"endpoints\": {\n")
	for i, endpoint := range endpoints {
		if i > 0 {
			fmt.Fprint(os.Stdout, ",\n")
		}
		end := totals[endpoint]
		mean := float64(end.Sum) / float64(end.Count)
		fmt.Fprintf(os.Stdout, "    \"%s\": {\n      \"min_response_time\": %d,\n      \"avg_response_time\": %.1f,\n      \"max_response_time\": %d\n    }",
			endpoint, end.Min, mean, end.Max)
	}
	fmt.Fprint(os.Stdout, "\n  }\n}\n")

	memProfile := os.Getenv("MEM_PROFILE")
	if memProfile != "" {
		f, err := os.Create(memProfile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error creating memory profile: %v\n", err)
			os.Exit(1)
		}
		defer f.Close()

		runtime.GC()
		if err := pprof.WriteHeapProfile(f); err != nil {
			fmt.Fprintf(os.Stderr, "error writing memory profile: %v\n", err)
			os.Exit(1)
		}
	}
}

type part struct {
	offset, size int64
}

func splitFile(filePath string, numParts int) ([]part, error) {
	const maxLineLength = 100

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	st, err := file.Stat()
	if err != nil {
		return nil, err
	}

	fileSize := st.Size()
	chunkSize := fileSize / int64(numParts)

	// Если файл слишком малкий, то нет смысла сплитить его
	if chunkSize < 4096 {
		return []part{{0, fileSize}}, nil
	}

	buf := make([]byte, maxLineLength)

	parts := make([]part, 0, numParts)

	offset := int64(0)

	for i := range numParts {
		if i == numParts-1 {
			if offset < fileSize {
				parts = append(parts, part{offset, fileSize - offset})
			}
			break
		}

		seekOffset := max(offset+chunkSize-maxLineLength, 0)
		_, err := file.Seek(seekOffset, io.SeekStart)
		if err != nil {
			return nil, err
		}
		n, _ := io.ReadFull(file, buf)
		chunk := buf[:n]
		newline := bytes.LastIndexByte(chunk, '\n')
		if newline < 0 {
			return nil, fmt.Errorf("newline not found at offset %d", offset+chunkSize-maxLineLength)
		}
		remaining := len(chunk) - newline - 1
		nextOffset := seekOffset + int64(len(chunk)) - int64(remaining)
		parts = append(parts, part{offset, nextOffset - offset})
		offset = nextOffset
	}

	return parts, nil
}

func processPart(filePath string, fileOffset, fileSize int64, resultsChan chan map[string]*Stats) {
	// Открываем файл
	file, err := os.Open(filePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error opening file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// Перемещаемся на начало нашего куска
	_, err = file.Seek(fileOffset, io.SeekStart)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error seek file: %v]n", err)
		os.Exit(1)
	}

	endpointStats := make(map[string]*Stats)

	stringCache := make(map[string]string)

	// Будем читать пачками по 32Mb
	chunkSize := 32 * 1024 * 1024
	buf := make([]byte, chunkSize)

	// Буфер для неполных строк между пачками
	remainder := make([]byte, 0, 4096)

	// Считаем количество прочитанных байт
	var bytesRead int64 = 0

	for bytesRead < fileSize {
		bytesToRead := min(int64(chunkSize), fileSize-bytesRead)

		// Read a chunk
		n, err := file.Read(buf[:bytesToRead])
		if err != nil && err != io.EOF {
			fmt.Fprintf(os.Stderr, "error reading file: %v\n", err)
			os.Exit(1)
		}

		if n == 0 {
			break // EOF
		}

		bytesRead += int64(n)

		chunk := buf[:n]

		lastNewline := bytes.LastIndexByte(chunk, '\n')

		var processingChunk []byte
		if lastNewline >= 0 {
			if len(remainder) > 0 {
				processingChunk = make([]byte, len(remainder)+lastNewline+1)
				copy(processingChunk, remainder)
				copy(processingChunk[len(remainder):], chunk[:lastNewline+1])
				remainder = remainder[:0]
			} else {
				processingChunk = chunk[:lastNewline+1]
			}

			if lastNewline < n-1 {
				remainder = append(remainder[:0], chunk[lastNewline+1:]...)
			}
		} else {
			// Если не нашли символа новой строки, то это очень странно, но просто добавляем к остатку
			remainder = append(remainder, chunk...)
			continue
		}

		processLines(processingChunk, endpointStats, stringCache)
	}

	if len(remainder) > 0 {
		processLines(remainder, endpointStats, stringCache)
	}

	resultsChan <- endpointStats
}

func processLines(data []byte, stats map[string]*Stats, stringCache map[string]string) error {
	spaceCount := 0

	var pathStart, pathEnd, timeStart int

	for i := 32; i < len(data); i++ {
		if data[i] == ' ' {
			spaceCount++
			switch spaceCount {
			// Встретили начало PATH
			case 2:
				pathStart = i + 1
			// Встретили конец PATH
			case 3:
				pathEnd = i
				i += 5
				timeStart = i
			}
		}

		// Если встречаем перевод строки, то сбрасываем счетчик пробелов
		if data[i] == '\n' {
			pathBytes := data[pathStart:pathEnd]
			unsafeKey := unsafeString(pathBytes)

			var endpointStr string
			if cached, exists := stringCache[unsafeKey]; exists {
				endpointStr = cached
			} else {
				// Создаем новую строку только если её нет в кэше
				endpointStr = string(pathBytes)
				stringCache[endpointStr] = endpointStr
			}

			responseTime, err := parseIntFast(data[timeStart:i])
			if err != nil {
				fmt.Println("Error parsing response time:", err)
				continue
			}

			s := stats[endpointStr]
			if s == nil {
				stats[endpointStr] = &Stats{
					Min:   int64(responseTime),
					Max:   int64(responseTime),
					Sum:   int64(responseTime),
					Count: 1,
				}
			} else {
				s.Min = min(s.Min, int64(responseTime))
				s.Max = max(s.Max, int64(responseTime))
				s.Sum += int64(responseTime)
				s.Count++
			}

			spaceCount = 0
			// Смещаемся, исключая timestamp и IP
			i += 32
		}
	}

	return nil
}
