package segment

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"
)

type (
	Offset struct {
		Start  int64
		Length int64
	}

	Segment struct {
		file          *os.File
		HashMap       map[string]Offset
		currentOffset int64
	}
)

var (
	ErrNoValue       error = errors.New("no value found for the given key")
	segmentDirectory       = "./segments"
)

func CreateSegment(fileName string) (*Segment, error) {
	file, err := os.OpenFile(path.Join(segmentDirectory, fileName), os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("error creating segment for file %s %w ", fileName, err)
	}
	s := Segment{
		file:          file,
		HashMap:       make(map[string]Offset),
		currentOffset: io.SeekStart,
	}

	return &s, nil
}

func (s *Segment) Write(key string, value string) error {
	stringToWrite := fmt.Sprintf("%s %s %s\n", time.Now().Format(time.RFC3339), key, value)
	bytesWritten, err := s.file.Write([]byte(stringToWrite))
	if err != nil {
		return fmt.Errorf("unable to complete set operation. error persisting data to file %w", err)
	}

	s.updateHashmap(key, int64(bytesWritten))
	return nil
}

func (s *Segment) Read(key string) (string, error) {
	offset, ok := s.HashMap[key]
	if !ok || offset.Start == -1 {
		return "", ErrNoValue
	}

	s.file.Seek(offset.Start, io.SeekStart)
	var readData []byte = make([]byte, offset.Length)
	_, err := s.file.Read(readData)
	if err != nil {
		return "", fmt.Errorf("unable to complete get operation. error reading data from segment file %w", err)
	}

	readString := string(readData)
	value := strings.Split(readString, " ")[2:]
	returnValue := strings.Join(value, " ")
	returnValue = strings.TrimSuffix(returnValue, "\n")

	return returnValue, nil
}

func (s *Segment) Delete(key string) {
	s.HashMap[key] = Offset{
		Start:  -1,
		Length: -1,
	}
}

func (s *Segment) updateHashmap(key string, bytesWritten int64) {
	s.HashMap[key] = Offset{
		Start:  s.currentOffset,
		Length: bytesWritten,
	}
	s.currentOffset += bytesWritten
}
