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

package codec

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/memory"

	"github.com/chronowave/codec/internal"
	"github.com/chronowave/fbs/go"
	"github.com/chronowave/ssql/go"
)

const (
	DayInMillis = 24 * 60 * 60 * 1000
	sep         = "/"
)

var (
	codePathKey = fbs.EnumNamesMetadataKey[fbs.MetadataKeyCODEPATH]
	dateLayout  = fbs.EnumNamesMetadataKey[fbs.MetadataKeyLAYOUT]
	arrowToSsql = map[arrow.Type]ssql.Type{
		arrow.LIST:      ssql.Type_list,
		arrow.STRUCT:    ssql.Type_struct,
		arrow.INT8:      ssql.Type_int8,
		arrow.UINT8:     ssql.Type_uint8,
		arrow.INT16:     ssql.Type_int16,
		arrow.UINT16:    ssql.Type_uint16,
		arrow.INT32:     ssql.Type_int32,
		arrow.UINT32:    ssql.Type_uint32,
		arrow.DATE32:    ssql.Type_date32,
		arrow.FLOAT32:   ssql.Type_float32,
		arrow.INT64:     ssql.Type_int64,
		arrow.UINT64:    ssql.Type_uint64,
		arrow.TIMESTAMP: ssql.Type_date64,
		arrow.FLOAT64:   ssql.Type_float64,
		arrow.BOOL:      ssql.Type_bool,
		arrow.STRING:    ssql.Type_text,
		arrow.NULL:      ssql.Type_null,
	}

	timestampUnit = map[arrow.TimeUnit]ssql.Timestamp_TimeUnit{
		arrow.Second:      ssql.Timestamp_Second,
		arrow.Millisecond: ssql.Timestamp_Millisecond,
		arrow.Microsecond: ssql.Timestamp_Microsecond,
		arrow.Nanosecond:  ssql.Timestamp_Nanosecond,
	}
)

type DateFormat struct {
	Unit     arrow.TimeUnit
	TimeZone string
	Layout   string
}

type Schema struct {
	revision uint64
	nextCode rune
	fields   map[string]*ssql.Field
	formats  map[string]DateFormat

	arrowSchema *arrow.Schema
}

func (s *Schema) Rev() uint64 {
	return s.revision
}

func (s *Schema) ToArrowFlightStream() ([]byte, error) {
	keys := make([]string, 1)
	values := make([]string, 1)
	keys[0] = fbs.EnumNamesMetadataKey[fbs.MetadataKeyNEXTCODE]
	values[0] = strconv.FormatInt(int64(s.nextCode), 10)

	metadata := arrow.NewMetadata(keys, values)
	arrowSchema := arrow.NewSchema(s.arrowSchema.Fields(), &metadata)
	return flight.SerializeSchema(arrowSchema, memory.DefaultAllocator), nil
}

// FromArrowFlightStream deserialize schema from Arrow Flight Schema binary format (info)
func (s *Schema) FromArrowFlightStream(ver uint64, info []byte) error {
	var err error
	s.arrowSchema, err = flight.DeserializeSchema(info, memory.DefaultAllocator)
	if err != nil {
		return err
	}

	s.revision = ver
	metadata := s.arrowSchema.Metadata()
	next := metadata.FindKey(fbs.EnumNamesMetadataKey[fbs.MetadataKeyNEXTCODE])
	if next < 0 {
		s.revision = 0
		// starts from 32, SPACE
		s.nextCode = ' '

		fields := s.arrowSchema.Fields()
		// for i, field := range fields will invoke copy constructor
		for i := range fields {
			if err = s.setFieldCodePath(&fields[i], nil); err != nil {
				return err
			}
		}
		s.revision = 1
	} else {
		values := metadata.Values()
		for i, key := range metadata.Keys() {
			switch fbs.EnumValuesMetadataKey[key] {
			case fbs.MetadataKeyNEXTCODE:
				var nextCode int64
				if nextCode, err = strconv.ParseInt(values[i], 10, 32); err == nil {
					s.nextCode = rune(nextCode)
				}
			}
			if err != nil {
				return err
			}
		}
	}

	return s.consolidateSSQLFields()
}

func (s *Schema) AddFields(alter *arrow.Schema) (*Schema, error) {
	clone, err := s.clone()
	if err != nil {
		return nil, err
	}

	var merge func(update *arrow.Field, exist []arrow.Field, parent []string) ([]arrow.Field, error)
	merge = func(update *arrow.Field, exist []arrow.Field, parent []string) ([]arrow.Field, error) {
		index := make(map[string]int)
		for i, o := range exist {
			index[o.Name] = i
		}

		if ith, ok := index[update.Name]; !ok {
			if err := clone.setFieldCodePath(update, parent); err != nil {
				return nil, err
			}
			exist = append(exist, *update)
		} else {
			var (
				fields []arrow.Field
				err    error
			)

			if len(exist[ith].Name) > 0 {
				parent = append(parent, exist[ith].Name)
			}

			// existing field
			switch exist[ith].Type.(type) {
			case *arrow.StructType:
				updateStruct, ok := update.Type.(*arrow.StructType)
				if !ok {
					return nil, fmt.Errorf("name %s should be StructType, actual: %v", update.Name, update.Type)
				}
				fields = exist[ith].Type.(*arrow.StructType).Fields()
				for _, u := range updateStruct.Fields() {
					fields, err = merge(&u, fields, parent)
					if err != nil {
						return nil, err
					}
				}
				exist[ith].Type = arrow.StructOf(fields...)
			case *arrow.ListType:
				updateField, ok := update.Type.(*arrow.ListType)
				if !ok {
					return nil, fmt.Errorf("name %s should be ListType, actual: %v", update.Name, update.Type)
				}
				u := updateField.ElemField()
				existField := exist[ith].Type.(*arrow.ListType).ElemField()
				existFieldType, ok := existField.Type.(*arrow.StructType)
				if !ok {
					return nil, fmt.Errorf("list field %s should be Struct Type, actual: %v", exist[ith].Name, exist[ith].Type)
				}
				fields, err = merge(&u, existFieldType.Fields(), parent)
				if err != nil {
					return nil, err
				}
				exist[ith].Type = arrow.ListOfField(arrow.Field{
					Name:     existField.Name,
					Type:     arrow.StructOf(fields...),
					Metadata: existField.Metadata,
				})
			default:
				if update.Type != exist[ith].Type {
					// TODO: update error
					return nil, fmt.Errorf("path exists")
				}
			}
		}

		return exist, nil
	}

	var (
		fields = clone.arrowSchema.Fields()
	)

	for _, u := range alter.Fields() {
		if fields, err = merge(&u, fields, nil); err != nil {
			return nil, err
		}
	}

	metadata := s.arrowSchema.Metadata()
	clone.arrowSchema = arrow.NewSchema(fields, &metadata)
	return clone, nil
}

func (s *Schema) setFieldCodePath(f *arrow.Field, parent []string) error {
	ith := f.Metadata.FindKey(codePathKey)

	key := parent
	if len(f.Name) > 0 {
		key = append(parent, f.Name)
	}

	if ith < 0 {
		// -1 no code path key
		nextRune := internal.NextRune(s.nextCode)
		metadata := f.Metadata
		keys := append(metadata.Keys(), fbs.EnumNamesMetadataKey[fbs.MetadataKeyCODEPATH])
		values := append(metadata.Values(), string(nextRune))
		f.Metadata = arrow.NewMetadata(keys, values)
		s.nextCode = nextRune
	}

	// note: all utf8 encoded
	switch f.Type.(type) {
	case *arrow.StructType:
		nested := f.Type.(*arrow.StructType).Fields()
		for i := range nested {
			if err := s.setFieldCodePath(&nested[i], key); err != nil {
				return err
			}
		}
	case *arrow.ListType:
		nested := f.Type.(*arrow.ListType).ElemField()
		if err := s.setFieldCodePath(&nested, append(key, "[")); err != nil {
			return err
		}
		f.Type = arrow.ListOfField(nested)
	}

	return nil
}

func (s *Schema) consolidateSSQLFields() error {
	fields := s.arrowSchema.Fields()
	s.fields = make(map[string]*ssql.Field)
	s.formats = make(map[string]DateFormat)

	nested := make([]*ssql.Field, len(fields))
	root := &ssql.Field{
		Unicode: uint32(0),
		Name:    "",
		Nested: &ssql.Field_Struct_{
			Struct: &ssql.Field_Struct{
				Fields: nested,
			},
		},
	}

	path := []string{""}
	var err error
	for i := range fields {
		nested[i], err = s.toSSQLField(&fields[i], path)
		if err != nil {
			return err
		}
	}

	s.fields[sep] = root

	return nil
}

func (s *Schema) toSSQLField(field *arrow.Field, path []string) (*ssql.Field, error) {
	metadata := field.Metadata
	ith := metadata.FindKey(codePathKey)

	if ith < 0 {
		return nil, fmt.Errorf("field %s is missing key %s", field.Name, codePathKey)
	}

	typeID := field.Type.ID()

	ssqlField := &ssql.Field{
		Unicode: uint32(metadata.Values()[ith][0]),
		Name:    field.Name,
		Type:    arrowToSsql[typeID],
	}

	key := path
	if len(field.Name) > 0 {
		key = append(path, field.Name)
	}

	pathKey := strings.Join(key, sep)
	if _, ok := s.fields[pathKey]; !ok {
		s.fields[pathKey] = ssqlField
	} else {
		// path exits if and only if ssqlField is an array element
		s.fields[strings.Join(append(key, ""), sep)] = ssqlField
	}

	if typeID == arrow.STRUCT {
		arrowFields := field.Type.(*arrow.StructType).Fields()

		nested := make([]*ssql.Field, len(arrowFields))
		var err error
		for i := range arrowFields {
			nested[i], err = s.toSSQLField(&arrowFields[i], key)
			if err != nil {
				return nil, err
			}
		}
		ssqlField.Nested = &ssql.Field_Struct_{
			Struct: &ssql.Field_Struct{
				Fields: nested,
			},
		}
	} else if typeID == arrow.LIST {
		f := field.Type.(*arrow.ListType).ElemField()
		list, err := s.toSSQLField(&f, key)
		if err != nil {
			return nil, err
		}

		ssqlField.Nested = &ssql.Field_List{
			List: list,
		}
	} else if typeID == arrow.TIMESTAMP {
		tsType := field.Type.(*arrow.TimestampType)
		ssqlField.Timestamp = &ssql.Timestamp{
			Timeunit: timestampUnit[tsType.Unit],
			Tz:       tsType.TimeZone,
		}

		var layout string
		if ith := field.Metadata.FindKey(dateLayout); ith >= 0 {
			layout = field.Metadata.Values()[ith]
		}

		s.formats[pathKey] = DateFormat{
			Unit:     tsType.Unit,
			TimeZone: tsType.TimeZone,
			Layout:   layout,
		}
	}

	return ssqlField, nil
}

func (s *Schema) clone() (*Schema, error) {
	info, err := s.ToArrowFlightStream()
	if err != nil {
		return nil, err
	}
	var clone Schema
	err = clone.FromArrowFlightStream(s.revision, info)
	return &clone, err
}
