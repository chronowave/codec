/*
 *  Copyright 2022 Chronowave Authors
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
	"bytes"
	"encoding/gob"
	"strings"

	"github.com/google/flatbuffers/go"

	"github.com/chronowave/fbs/go"
)

func FlattenClusterQuery(query []byte, hosts []string, local int) []byte {
	builder := flatbuffers.NewBuilder(2048)

	ssqlOff := builder.CreateByteVector(query)

	offsets := make([]flatbuffers.UOffsetT, len(hosts))
	for i, h := range hosts {
		offsets[i] = builder.CreateString(h)
	}

	fbs.ClusterQueryStartHostsVector(builder, len(hosts))

	for i := len(hosts) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(offsets[i])
	}

	hostsOff := builder.EndVector(len(hosts))

	fbs.ClusterQueryStart(builder)
	fbs.ClusterQueryAddSsql(builder, ssqlOff)
	fbs.ClusterQueryAddHosts(builder, hostsOff)
	fbs.ClusterQueryAddLocal(builder, int32(local))
	builder.Finish(fbs.ClusterQueryEnd(builder))
	return builder.FinishedBytes()
}

func SerializeColumnedEntity(entity *ColumnedEntity) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(entity)

	return buf.Bytes(), err
}

func DeserializeColumnedEntity(data []byte) (*ColumnedEntity, error) {
	dec := gob.NewDecoder(bytes.NewReader(data))

	var entity ColumnedEntity
	err := dec.Decode(&entity)

	return &entity, err
}

func FlattenColumnizedEntities(entities []*ColumnedEntity) []byte {
	if len(entities) == 0 {
		return []byte{}
	}

	builder := flatbuffers.NewBuilder(2048)

	text := make(map[string]int)

	offsets := make([]flatbuffers.UOffsetT, len(entities))
	for i := 0; i < len(entities); i++ {
		offsets[i] = flatternColumnizedEntity(builder, entities[i], text)
	}

	fbs.ColumnizedEntitiesStartEntitiesVector(builder, len(entities))
	for i := len(offsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(offsets[i])
	}
	offset := builder.EndVector(len(entities))

	content := make([]string, len(text))
	for k, v := range text {
		content[v-1] = k
	}

	contentOffsets := make([]flatbuffers.UOffsetT, len(content))
	for i := range content {
		contentOffsets[i] = builder.CreateString(content[i])
	}

	fbs.ColumnizedEntitiesStartTextContentVector(builder, len(contentOffsets))
	for i := len(contentOffsets) - 1; i >= 0; i-- {
		builder.PrependUOffsetT(contentOffsets[i])
	}
	contentOffset := builder.EndVector(len(contentOffsets))

	fbs.ColumnizedEntitiesStart(builder)
	fbs.ColumnizedEntitiesAddEntities(builder, offset)
	fbs.ColumnizedEntitiesAddTextContent(builder, contentOffset)
	builder.Finish(fbs.ColumnizedEntitiesEnd(builder))

	return builder.FinishedBytes()
}

func flatternColumnizedEntity(builder *flatbuffers.Builder, entity *ColumnedEntity, content map[string]int) flatbuffers.UOffsetT {
	var offsets []struct {
		name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
		value flatbuffers.UOffsetT
	}

	// 1
	if len(entity.Columnar.UInt8) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddU8,
			value: BuildColumn(builder, fbs.MarkerUINT8, builder.PrependUint8, entity.Columnar.UInt8),
		})
	}

	// 2
	if len(entity.Columnar.Int8) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddI8,
			value: BuildColumn(builder, fbs.MarkerINT8, builder.PrependInt8, entity.Columnar.Int8),
		})
	}

	// 3
	if len(entity.Columnar.UInt16) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddU16,
			value: BuildColumn(builder, fbs.MarkerUINT16, builder.PrependUint16, entity.Columnar.UInt16),
		})
	}

	// 4
	if len(entity.Columnar.Int16) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddI16,
			value: BuildColumn(builder, fbs.MarkerINT16, builder.PrependInt16, entity.Columnar.Int16),
		})
	}

	// 5
	if len(entity.Columnar.UInt32) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddU32,
			value: BuildColumn(builder, fbs.MarkerUINT16, builder.PrependUint32, entity.Columnar.UInt32),
		})
	}

	// 6
	if len(entity.Columnar.Int32) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddI32,
			value: BuildColumn(builder, fbs.MarkerINT32, builder.PrependInt32, entity.Columnar.Int32),
		})
	}

	// 7
	if len(entity.Columnar.Date32) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddD32,
			value: BuildColumn(builder, fbs.MarkerDATE32, builder.PrependInt32, entity.Columnar.Date32),
		})
	}

	// 8
	if len(entity.Columnar.UInt64) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddU64,
			value: BuildColumn(builder, fbs.MarkerUINT64, builder.PrependUint64, entity.Columnar.UInt64),
		})
	}

	// 9
	if len(entity.Columnar.Int64) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddI64,
			value: BuildColumn(builder, fbs.MarkerINT64, builder.PrependInt64, entity.Columnar.Int64),
		})
	}

	// 10
	if len(entity.Columnar.Date64) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddD64,
			value: BuildColumn(builder, fbs.MarkerDATE64, builder.PrependInt64, entity.Columnar.Date64),
		})
	}

	// 11
	if len(entity.Columnar.Float32) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddF32,
			value: BuildColumn(builder, fbs.MarkerFLT32, builder.PrependFloat32, entity.Columnar.Float32),
		})
	}

	// 12
	if len(entity.Columnar.Float64) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddF64,
			value: BuildColumn(builder, fbs.MarkerFLT64, builder.PrependFloat64, entity.Columnar.Float64),
		})
	}

	// 13
	if len(entity.Columnar.Text) > 0 {
		text := consolidateTextColumn(entity.Columnar.Text, content)
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddText,
			value: BuildColumn(builder, fbs.MarkerUINT32, builder.PrependUint32, text),
		})
	}

	// 14
	if len(entity.Columnar.Bool) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddBool,
			value: BuildColumn(builder, fbs.MarkerBOOL, builder.PrependBool, entity.Columnar.Bool),
		})
	}

	// 15
	if len(entity.Columnar.Null) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddNull,
			value: BuildColumn(builder, fbs.MarkerNULL, builder.PrependUint16, entity.Columnar.Null),
		})
	}

	// 16
	if len(entity.Columnar.ArrayBracket) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddArrayBracket,
			value: BuildColumn(builder, fbs.MarkerUINT16, builder.PrependUint16, entity.Columnar.ArrayBracket),
		})
	}

	// 17
	if len(entity.Columnar.ArrayElement) > 0 {
		offsets = append(offsets, struct {
			name  func(*flatbuffers.Builder, flatbuffers.UOffsetT)
			value flatbuffers.UOffsetT
		}{
			name:  fbs.ColumnizedEntityAddArrayElement,
			value: BuildColumn(builder, fbs.MarkerUINT16, builder.PrependUint16, entity.Columnar.ArrayElement),
		})
	}

	fbs.ColumnizedEntityStart(builder)
	for _, pair := range offsets {
		pair.name(builder, pair.value)
	}
	return fbs.ColumnizedEntityEnd(builder)
}

func consolidateTextColumn(text map[uint32][][]byte, content map[string]int) map[uint32][]uint32 {
	consolidated := make(map[uint32][]uint32, len(text))

	for k, v := range text {
		offset := make([]uint32, len(v))

		for i := range v {
			t := strings.TrimSpace(string(v[i]))

			if len(t) == 0 {
				offset[i] = 0
				continue
			}

			if j, ok := content[t]; ok {
				offset[i] = uint32(j)
			} else {
				j = len(content) + 1
				content[t] = j
				offset[i] = uint32(j)
			}
		}

		consolidated[k] = offset
	}

	return consolidated
}
