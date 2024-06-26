package store

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	"bitcaskClone/segment"
	sm "bitcaskClone/segmentManager"
)

type (
	Store struct {
		Stdin  io.Reader
		Stdout io.Writer
		Stderr io.Writer
	}
)

// var ErrNoValue = errors.New("no value found for the given key")

var (
	getOperation    string = "get"
	setOperation    string = "set"
	deleteOperation string = "delete"
)

// var segmentMap map[string]tuple = make(map[string]tuple)
// var currentOffset int64 = io.SeekStart

// func updateHasMap(key string, bytesWritten int64) {
// 	segmentMap[key] = tuple{
// 		Start: currentOffset,
// 		End:   bytesWritten,
// 	}
// 	currentOffset += bytesWritten
// }

// func handleGetOperation(key string) (string, error) {
// 	offset, ok := segmentMap[key]
// 	if !ok {
// 		return "", ErrNoValue
// 	}
// 	file, err := os.OpenFile("currentSegment", os.O_RDONLY, 0644)
// 	if err != nil {
// 		return "", fmt.Errorf("unable to complete get operation. error opening segment file %w", err)
// 	}

// 	file.Seek(offset.Start, io.SeekStart)
// 	var readData []byte = make([]byte, offset.End)
// 	_, err = file.Read(readData)
// 	if err != nil {
// 		return "", fmt.Errorf("unable to complete get operation. error reading data from segment file %w", err)
// 	}

// 	readString := string(readData)
// 	value := strings.Split(readString, " ")[2:]
// 	returnValue := strings.Join(value, " ")
// 	returnValue = strings.TrimSuffix(returnValue, "\n")

// 	return returnValue, nil
// }

// func handleSetOperation(key string, value string) error {
// 	file, err := os.OpenFile("currentSegment", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
// 	if err != nil {
// 		return fmt.Errorf("unable to complete set operation. error opening segment file %w", err)
// 	}
// 	defer file.Close()

// 	stringToWrite := fmt.Sprintf("%s %s %s\n", time.Now().Format(time.RFC3339), key, value)
// 	bytesWritten, err := file.Write([]byte(stringToWrite))
// 	if err != nil {
// 		return fmt.Errorf("unable to complete set operation. error persisting data to file %w", err)
// 	}

// 	updateHasMap(key, int64(bytesWritten))

// 	return nil
// }

// // for now deleting key from hasmap
// func handleDeleteOperation(key string) {
// 	delete(segmentMap, key)
// }

func processInputString(input string, segmentManager *sm.SegmentManager) (*string, error) {
	input = strings.TrimSpace(input)
	input = strings.TrimSuffix(input, "\n")
	splittedInput := strings.Split(input, " ")

	operation := splittedInput[0]

	if (operation == setOperation) && len(splittedInput) != 3 {
		return nil, errors.New("please provide a valid key and value pair")
	}

	switch operation {
	case getOperation:
		data, err := segmentManager.Get(splittedInput[1])
		if errors.Is(err, segment.ErrNoValue) {
			nullString := "nil"
			return &nullString, nil
		}
		return &data, err
	case setOperation:
		return nil, segmentManager.Set(splittedInput[1], splittedInput[2])
	case deleteOperation:
		segmentManager.Delete(splittedInput[1])
		return nil, nil
	default:
		return nil, errors.New("not a valid operation. get|set|delete are valid operations")
	}
}

func (store Store) Start() {
	reader := bufio.NewReader(store.Stdin)
	logger := log.New(store.Stdout, "store ", log.Flags())
	segmentManager := sm.Initialize()

	for {
		data, err := reader.ReadString('\n')
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			logger.Println("error reading next line. But continuing anyway", err)
		}

		value, err := processInputString(data, segmentManager)
		if err != nil {
			logger.Println("error parsing input string: ", err)
		}

		if value != nil {
			fmt.Fprintln(store.Stdout, *value)
		}
	}
}

func Initialize() Store {
	return Store{
		Stdin:  os.Stdin,
		Stdout: os.Stdout,
		Stderr: os.Stderr,
	}
}
