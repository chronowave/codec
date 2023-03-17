/*
 *  Copyright 2021 Chronowave Authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  Package parser declares an expression parser with support for macro
 *  expansion.
 */

//go:generate protoc --go_out=./ --proto_path=./ codec.proto

package codec

import (
	"errors"
	"fmt"
	"golang.org/x/xerrors"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/buger/jsonparser"

	"github.com/chronowave/fbs/go"
	"github.com/chronowave/ssql/go"
)

type Columnar struct {
	Int8         map[uint32][]int8    // 1
	UInt8        map[uint32][]uint8   // 2
	Int16        map[uint32][]int16   // 3
	UInt16       map[uint32][]uint16  // 4
	Int32        map[uint32][]int32   // 5
	Date32       map[uint32][]int32   // 6
	UInt32       map[uint32][]uint32  // 7
	Int64        map[uint32][]int64   // 8
	Date64       map[uint32][]int64   // 9
	UInt64       map[uint32][]uint64  // 10
	Float32      map[uint32][]float32 // 11
	Float64      map[uint32][]float64 // 12
	Text         map[uint32][][]byte  // 13
	Bool         map[uint32][]bool    // 14
	Null         map[uint32][]uint16  // 15
	ArrayBracket map[uint32][]uint16  // 16
	ArrayElement map[uint32][]uint16  // 17
}

var epoch time.Time

type ColumnedEntity struct {
	SchemaVersion uint64
	Columnar      Columnar
}

func newColumnedEntity(rev uint64) *ColumnedEntity {
	return &ColumnedEntity{
		SchemaVersion: rev,
		Columnar: Columnar{
			Int8:         make(map[uint32][]int8),
			UInt8:        make(map[uint32][]uint8),
			Int16:        make(map[uint32][]int16),
			UInt16:       make(map[uint32][]uint16),
			Int32:        make(map[uint32][]int32),
			UInt32:       make(map[uint32][]uint32),
			Date32:       make(map[uint32][]int32),
			Int64:        make(map[uint32][]int64),
			UInt64:       make(map[uint32][]uint64),
			Date64:       make(map[uint32][]int64),
			Float32:      make(map[uint32][]float32),
			Float64:      make(map[uint32][]float64),
			Text:         make(map[uint32][][]byte),
			Bool:         make(map[uint32][]bool),
			Null:         make(map[uint32][]uint16),
			ArrayBracket: make(map[uint32][]uint16),
			ArrayElement: make(map[uint32][]uint16),
		},
	}
}

const threshold = uint32(^uint16(0))

func (s *Schema) ColumnizeJson(json []byte) ([]*ColumnedEntity, error) {
	var entities []*ColumnedEntity
	for {
		// offset to json buffer, number of object,
		var (
			parseFunc func(*ColumnedEntity, *uint32, map[uint32][]uint16, []string, []byte) (int, error)
			ce        *ColumnedEntity
		)
	open:
		// advancing json bytes to first appearance of either { or [
		for i, b := range json {
			switch b {
			case '{':
				ce = newColumnedEntity(s.revision)
				entities = append(entities, ce)
				parseFunc = s.parseObject
				json = json[i:]
				break open
			case '[':
				parseFunc = func(_ *ColumnedEntity, _ *uint32, _ map[uint32][]uint16, path []string, data []byte) (int, error) {
					var objerr error
					eoa, err := jsonparser.ArrayEach(data,
						func(value []byte, dataType jsonparser.ValueType, offset int, err error) {
							if dataType == jsonparser.Object {
								ce = newColumnedEntity(s.revision)
								nesting := ^uint32(0)
								entities = append(entities, ce)
								if _, err := s.parseObject(ce, &nesting, nil, path, value); err != nil {
									objerr = err
								}
							} else {
								// TODO: log err, note: skip if provided json doc contains only array of scalar elements
							}
						})

					if err == nil {
						err = objerr
					}

					return eoa, err
				}
				json = json[i:]
				break open
			}
		}

		if parseFunc == nil {
			// EOF
			break
		}

		// end of entity, } or ], note: eoe would be at before closing } or ], including white space
		nesting := ^uint32(0)
		eoe, err := parseFunc(ce, &nesting, nil, []string{""}, json)
		if err != nil {
			return nil, err
		}

		ce = nil
		json = json[eoe:]
	}

	return entities, nil
}

// offset in json doc, number of objects, error if any
func (s *Schema) parseObject(f *ColumnedEntity, nesting *uint32, nested map[uint32][]uint16, path []string, data []byte) (int, error) {
	// eoo -> end of object, offset in json doc
	eoo := 0
	callback := func(key []byte, value []byte, valueType jsonparser.ValueType, offset int) error {
		eoo = offset
		return s.parseObjectValue(f, nesting, nested, valueType, append(path, replaceSlashWithUnderscore(key)), value)
	}

	err := jsonparser.ObjectEach(data, callback)
	if err != nil {
		return eoo, err
	}

	return eoo, nil
}

// parse array element
func (s *Schema) parseArrayValue(f *ColumnedEntity, nesting *uint32, path []string, data []byte) (int, error) {
	// increment, overflow to zero from initial value u32::MAX
	*nesting++

	if *nesting >= threshold {
		return 0, fmt.Errorf("the number of array elements in one document exceeds %d", threshold)
	}

	// log [ pos, brackets[0] = the count of brackets
	brackets := []uint16{0, uint16(*nesting)}

	// start path /root/children
	field, ok := s.fields[strings.Join(path, sep)]
	if !ok {
		return 0, fmt.Errorf("JSON document contains path %s isn't defined in schema", strings.Join(path, sep))
	}

	var (
		prevErr error
		aed     = false
	)

	// start object count
	callback := func(value []byte, valueType jsonparser.ValueType, offset int, err error) {
		if err != nil || prevErr != nil {
			return
		}

		if aed {
			//  AED, array element divider, increase by one
			*nesting++
			if *nesting >= threshold {
				prevErr = fmt.Errorf("the number of array elements in one document exceeds %d", threshold)
				return
			}

			brackets = append(brackets, uint16(*nesting))
		}
		err = s.parseObjectValue(f, nesting, f.Columnar.ArrayElement, valueType, append(path, ""), value)
		if err != nil {
			prevErr = err
		}
		//
		aed = true
	}

	eoa, _ := jsonparser.ArrayEach(data, callback)

	// the last closing bracket
	brackets[0] = uint16(len(brackets))

	// log last one
	f.Columnar.ArrayBracket[field.Unicode] = append(f.Columnar.ArrayBracket[field.Unicode], append(brackets, uint16(*nesting)+1)...)

	// end of array
	return eoa, prevErr
}

// https://tools.ietf.org/html/rfc7159#section-3
func (s *Schema) parseObjectValue(f *ColumnedEntity, nesting *uint32, nested map[uint32][]uint16, valueType jsonparser.ValueType, path []string, value []byte) error {
	field, ok := s.fields[strings.Join(path, sep)]
	if !ok {
		return fmt.Errorf("JSON document contains path %s isn't defined in schema", strings.Join(path, sep))
	}

	var (
		isNumber bool
		err      error
	)

	switch valueType {
	case jsonparser.Number:
		isNumber = true
		fallthrough
	case jsonparser.String:
		switch field.Type {
		case ssql.Type_text:
			f.Columnar.Text[field.Unicode] = append(f.Columnar.Text[field.Unicode], value)
		case ssql.Type_int8:
			if v, e := strconv.ParseInt(string(value), 10, 8); e != nil {
				// TODO: log err
				return e
			} else {
				f.Columnar.Int8[field.Unicode] = append(f.Columnar.Int8[field.Unicode], int8(v))
			}
		case ssql.Type_uint8:
			if v, e := strconv.ParseUint(string(value), 10, 8); e != nil {
				// TODO: log err
				return e
			} else {
				f.Columnar.UInt8[field.Unicode] = append(f.Columnar.UInt8[field.Unicode], uint8(v))
			}
		case ssql.Type_int16:
			if v, e := strconv.ParseInt(string(value), 10, 16); e != nil {
				// TODO: log err
				return e
			} else {
				f.Columnar.Int16[field.Unicode] = append(f.Columnar.Int16[field.Unicode], int16(v))
			}
		case ssql.Type_uint16:
			if v, e := strconv.ParseUint(string(value), 10, 16); e != nil {
				// TODO: log err
				return e
			} else {
				f.Columnar.UInt16[field.Unicode] = append(f.Columnar.UInt16[field.Unicode], uint16(v))
			}

		case ssql.Type_int32:
			if v, e := strconv.ParseInt(string(value), 10, 32); e != nil {
				// TODO: log err
				return e
			} else {
				f.Columnar.Int32[field.Unicode] = append(f.Columnar.Int32[field.Unicode], int32(v))
			}
		case ssql.Type_date32:
			var days int32
			if isNumber {
				if t, e := strconv.ParseInt(string(value), 10, 32); e != nil {
					return xerrors.Errorf("unable to parse date32 value %s", string(value))
				} else {
					days = int32(t)
				}
			} else {
				format, ok := s.formats[strings.Join(path, sep)]
				if !ok {
					format = DateFormat{
						Layout: time.RFC3339Nano,
					}
				}

				if tm, err := time.Parse(format.Layout, string(value)); err != nil {
					return xerrors.Errorf("unable to parse path %s date32 value %s with date format %v", strings.Join(path, sep), string(value), format)
				} else {
					days = int32(tm.Sub(epoch).Hours() / 24)
				}
			}

			f.Columnar.Date32[field.Unicode] = append(f.Columnar.Date32[field.Unicode], days)
		case ssql.Type_uint32:
			if v, e := strconv.ParseUint(string(value), 10, 32); e != nil {
				// TODO: log err
				return e
			} else {
				f.Columnar.UInt32[field.Unicode] = append(f.Columnar.UInt32[field.Unicode], uint32(v))
			}
		case ssql.Type_float32:
			if v, e := strconv.ParseFloat(string(value), 32); e != nil {
				// TODO: log err
				return e
			} else {
				f.Columnar.Float32[field.Unicode] = append(f.Columnar.Float32[field.Unicode], float32(v))
			}
		case ssql.Type_int64:
			if v, e := strconv.ParseInt(string(value), 10, 64); e != nil {
				// TODO: log err
				return e
			} else {
				f.Columnar.Int64[field.Unicode] = append(f.Columnar.Int64[field.Unicode], v)
			}
		case ssql.Type_date64:
			var millis int64
			if isNumber {
				if v, e := strconv.ParseInt(string(value), 10, 64); e != nil {
					return xerrors.Errorf("unable to parse path %s date64 value %s", strings.Join(path, sep), string(value))
				} else {
					millis = v
				}
			} else {
				format, ok := s.formats[strings.Join(path, sep)]
				if !ok {
					format = DateFormat{
						Unit:   arrow.Millisecond,
						Layout: time.RFC3339Nano,
					}
				}
				if tm, err := time.Parse(format.Layout, string(value)); err != nil {
					return xerrors.Errorf("unable to parse path %s date64 value %s with date format %v", strings.Join(path, sep), string(value), format)
				} else {
					switch format.Unit {
					case arrow.Second:
						millis = tm.Unix()
					case arrow.Millisecond:
						millis = tm.UnixMilli()
					case arrow.Microsecond:
						fallthrough
					case arrow.Nanosecond:
						millis = tm.UnixMicro()
					}
				}
			}

			f.Columnar.Date64[field.Unicode] = append(f.Columnar.Date64[field.Unicode], millis)
		case ssql.Type_uint64:
			if v, e := strconv.ParseUint(string(value), 10, 64); e != nil {
				// TODO: log err
				return e
			} else {
				f.Columnar.UInt64[field.Unicode] = append(f.Columnar.UInt64[field.Unicode], v)
			}

		case ssql.Type_float64:
			if v, e := strconv.ParseFloat(string(value), 64); e != nil {
				// TODO: log err
				return e
			} else {
				f.Columnar.Float64[field.Unicode] = append(f.Columnar.Float64[field.Unicode], v)
			}
		case ssql.Type_bool:
			if value[0] == 't' || value[0] == 'T' || value[0] == 'y' || value[0] == 'Y' {
				f.Columnar.Bool[field.Unicode] = append(f.Columnar.Bool[field.Unicode], true)
			} else {
				f.Columnar.Bool[field.Unicode] = append(f.Columnar.Bool[field.Unicode], false)
			}
		}
		if nested != nil {
			nested[field.Unicode] = append(nested[field.Unicode], uint16(*nesting))
		}
	case jsonparser.Object:
		_, err = s.parseObject(f, nesting, nested, stripNestedArrayBracket(path), value)
	case jsonparser.Array:
		_, err = s.parseArrayValue(f, nesting, stripNestedArrayBracket(path), value)
	case jsonparser.Boolean:
		if field.Type != ssql.Type_bool {
			return errors.New("this is not boolean")
		}
		if value[0] == 't' || value[0] == 'T' || value[0] == 'y' || value[0] == 'Y' {
			f.Columnar.Bool[field.Unicode] = append(f.Columnar.Bool[field.Unicode], true)
		} else {
			f.Columnar.Bool[field.Unicode] = append(f.Columnar.Bool[field.Unicode], false)
		}
		if nested != nil {
			nested[field.Unicode] = append(nested[field.Unicode], uint16(*nesting))
		}
	case jsonparser.Null:
		// note: null column will contain aee value since null column
		f.Columnar.Null[field.Unicode] = append(f.Columnar.Null[field.Unicode], uint16(*nesting))
	case jsonparser.Unknown:
		err = jsonparser.UnknownValueTypeError
	}

	return err
}

func replaceSlashWithUnderscore(key []byte) string {
	for i, b := range key {
		if b == '/' || b <= byte(fbs.MarkerEOA) {
			key[i] = '_'
		}
	}
	return string(key)
}

func stripNestedArrayBracket(path []string) []string {
	sz := len(path)
	if sz > 1 && len(path[sz-1]) == 0 {
		// note: strip "" for array element
		// [8,4,5] or [{"a1": 1}]
		sz--
	}

	return path[:sz]
}
