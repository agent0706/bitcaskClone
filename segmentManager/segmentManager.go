package segmentManager

import (
	"bitcaskClone/segment"
	"fmt"
	"log"
	"time"
)

var MAX_RECORDS_PER_SEGMENT int = 2
var MAX_SEGMENTS int = 3

type SegmentManager struct {
	currentSegment      segment.Segment
	segments            []segment.Segment
	currentRecordsCount int
}

func createNewSegment() *segment.Segment {
	segmentName := fmt.Sprintf("%s.segment", time.Now().Format(time.RFC3339Nano))
	newSegment, err := segment.CreateSegment(segmentName)
	if err != nil {
		log.Fatalln("error creating segment file", err)
	}
	return newSegment
}

func Initialize() *SegmentManager {
	// create a new segment
	newSegment := createNewSegment()
	sm := &SegmentManager{
		currentSegment:      *newSegment,
		currentRecordsCount: 0,
	}

	return sm

	// start a process that monitors the current segment and when it is full creates new segment and swaps the current segment
	// have a configuration like the number of segment files after which the merge process should start
	// trigger the merge segment process on a seperate thread
	// delete a segment
}

func (s *SegmentManager) Set(key string, value string) error {
	err := s.currentSegment.Write(key, value)
	if err != nil {
		return err
	}

	s.currentRecordsCount += 1

	if s.currentRecordsCount >= MAX_RECORDS_PER_SEGMENT {
		s.segments = append(s.segments, s.currentSegment)
		newSegment := createNewSegment()
		s.currentSegment = *newSegment
		s.currentRecordsCount = 0
	}

	return nil
}

func (s *SegmentManager) Get(key string) (string, error) {
	val, err := s.currentSegment.Read(key)
	if val == "" && err != nil {
		for _, eachSegment := range s.segments {
			offset, ok := eachSegment.HashMap[key]
			if !ok || offset.Start == -1 {
				continue
			}
			val, err := eachSegment.Read(key)
			return val, err
		}
	}
	return val, err
}

func (s *SegmentManager) Delete(key string) {
	_, ok := s.currentSegment.HashMap[key]
	if !ok {
		for _, eachSegment := range s.segments {
			offset, ok := eachSegment.HashMap[key]
			if !ok {
				continue
			}
			if offset.Start == -1 {
				break
			}
			eachSegment.Delete(key)
		}
	} else {
		s.currentSegment.Delete(key)
	}
}
