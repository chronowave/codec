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
	"github.com/google/flatbuffers/go"

	"github.com/chronowave/fbs/go"
)

type ColumnarBuilderFunction struct {
	columnStartVector func(*flatbuffers.Builder, int) flatbuffers.UOffsetT
	columnStart       func(*flatbuffers.Builder)
	columnAddKey      func(*flatbuffers.Builder, uint32)
	columnAddValue    func(*flatbuffers.Builder, flatbuffers.UOffsetT)
	columnEnd         func(*flatbuffers.Builder) flatbuffers.UOffsetT
	entityStartVector func(*flatbuffers.Builder, int) flatbuffers.UOffsetT
}

var (
	specs = map[fbs.Marker]ColumnarBuilderFunction{
		fbs.MarkerUINT8: {
			columnStartVector: fbs.Uint8ColumnStartUint8Vector,
			columnStart:       fbs.Uint8ColumnStart,
			columnAddKey:      fbs.Uint8ColumnAddUnicode,
			columnAddValue:    fbs.Uint8ColumnAddUint8,
			columnEnd:         fbs.Uint8ColumnEnd,
			entityStartVector: fbs.ColumnizedEntityStartU8Vector,
		},
		fbs.MarkerINT8: {
			columnStartVector: fbs.Int8ColumnStartInt8Vector,
			columnStart:       fbs.Int8ColumnStart,
			columnAddKey:      fbs.Int8ColumnAddUnicode,
			columnAddValue:    fbs.Int8ColumnAddInt8,
			columnEnd:         fbs.Int8ColumnEnd,
			entityStartVector: fbs.ColumnizedEntityStartI8Vector,
		},
		fbs.MarkerUINT16: {
			columnStartVector: fbs.Uint16ColumnStartUint16Vector,
			columnStart:       fbs.Uint16ColumnStart,
			columnAddKey:      fbs.Uint16ColumnAddUnicode,
			columnAddValue:    fbs.Uint16ColumnAddUint16,
			columnEnd:         fbs.Uint16ColumnEnd,
			entityStartVector: fbs.ColumnizedEntityStartU16Vector,
		},
		fbs.MarkerINT16: {
			columnStartVector: fbs.Int16ColumnStartInt16Vector,
			columnStart:       fbs.Int16ColumnStart,
			columnAddKey:      fbs.Int16ColumnAddUnicode,
			columnAddValue:    fbs.Int16ColumnAddInt16,
			columnEnd:         fbs.Int16ColumnEnd,
			entityStartVector: fbs.ColumnizedEntityStartI16Vector,
		},
		fbs.MarkerUINT32: {
			columnStartVector: fbs.Uint32ColumnStartUint32Vector,
			columnStart:       fbs.Uint32ColumnStart,
			columnAddKey:      fbs.Uint32ColumnAddUnicode,
			columnAddValue:    fbs.Uint32ColumnAddUint32,
			columnEnd:         fbs.Uint32ColumnEnd,
			entityStartVector: fbs.ColumnizedEntityStartU32Vector,
		},
		fbs.MarkerINT32: {
			columnStartVector: fbs.Int32ColumnStartInt32Vector,
			columnStart:       fbs.Int32ColumnStart,
			columnAddKey:      fbs.Int32ColumnAddUnicode,
			columnAddValue:    fbs.Int32ColumnAddInt32,
			columnEnd:         fbs.Int32ColumnEnd,
			entityStartVector: fbs.ColumnizedEntityStartI32Vector,
		},
		fbs.MarkerDATE32: {
			columnStartVector: fbs.Int32ColumnStartInt32Vector,
			columnStart:       fbs.Int32ColumnStart,
			columnAddKey:      fbs.Int32ColumnAddUnicode,
			columnAddValue:    fbs.Int32ColumnAddInt32,
			columnEnd:         fbs.Int32ColumnEnd,
			entityStartVector: fbs.ColumnizedEntityStartD32Vector,
		},
		fbs.MarkerUINT64: {
			columnStartVector: fbs.Uint64ColumnStartUint64Vector,
			columnStart:       fbs.Uint64ColumnStart,
			columnAddKey:      fbs.Uint64ColumnAddUnicode,
			columnAddValue:    fbs.Uint64ColumnAddUint64,
			columnEnd:         fbs.Uint64ColumnEnd,
			entityStartVector: fbs.ColumnizedEntityStartU64Vector,
		},
		fbs.MarkerINT64: {
			columnStartVector: fbs.Int64ColumnStartInt64Vector,
			columnStart:       fbs.Int64ColumnStart,
			columnAddKey:      fbs.Int64ColumnAddUnicode,
			columnAddValue:    fbs.Int64ColumnAddInt64,
			columnEnd:         fbs.Int64ColumnEnd,
			entityStartVector: fbs.ColumnizedEntityStartI64Vector,
		},
		fbs.MarkerDATE64: {
			columnStartVector: fbs.Int64ColumnStartInt64Vector,
			columnStart:       fbs.Int64ColumnStart,
			columnAddKey:      fbs.Int64ColumnAddUnicode,
			columnAddValue:    fbs.Int64ColumnAddInt64,
			columnEnd:         fbs.Int64ColumnEnd,
			entityStartVector: fbs.ColumnizedEntityStartD64Vector,
		},
		fbs.MarkerFLT32: {
			columnStartVector: fbs.Float32ColumnStartFloatVector,
			columnStart:       fbs.Float32ColumnStart,
			columnAddKey:      fbs.Float32ColumnAddUnicode,
			columnAddValue:    fbs.Float32ColumnAddFloat,
			columnEnd:         fbs.Float32ColumnEnd,
			entityStartVector: fbs.ColumnizedEntityStartF32Vector,
		},
		fbs.MarkerFLT64: {
			columnStartVector: fbs.Float64ColumnStartFloat64Vector,
			columnStart:       fbs.Float64ColumnStart,
			columnAddKey:      fbs.Float64ColumnAddUnicode,
			columnAddValue:    fbs.Float64ColumnAddFloat64,
			columnEnd:         fbs.Float64ColumnEnd,
			entityStartVector: fbs.ColumnizedEntityStartF64Vector,
		},
		fbs.MarkerBOOL: {
			columnStartVector: fbs.BoolColumnStartBoolVector,
			columnStart:       fbs.BoolColumnStart,
			columnAddKey:      fbs.BoolColumnAddUnicode,
			columnAddValue:    fbs.BoolColumnAddBool,
			columnEnd:         fbs.BoolColumnEnd,
			entityStartVector: fbs.ColumnizedEntityStartBoolVector,
		},
		fbs.MarkerNULL: {
			columnStartVector: fbs.NullColumnStartNullVector,
			columnStart:       fbs.NullColumnStart,
			columnAddKey:      fbs.NullColumnAddUnicode,
			columnAddValue:    fbs.NullColumnAddNull,
			columnEnd:         fbs.NullColumnEnd,
			entityStartVector: fbs.ColumnizedEntityStartNullVector,
		},
	}
)

func BuildColumn[T any](builder *flatbuffers.Builder, m fbs.Marker, prepend func(T), column map[uint32][]T) flatbuffers.UOffsetT {
	spec := specs[m]

	sz := len(column)

	offsets := make([]flatbuffers.UOffsetT, sz)

	i := 0
	for k, v := range column {
		asz := len(v)

		//fbs.Uint8ColumnStartUint8Vector(builder, asz)
		spec.columnStartVector(builder, asz)
		for j := asz - 1; j >= 0; j-- {
			//builder.PrependUint8(v[j])
			prepend(v[j])
		}
		voff := builder.EndVector(asz)

		//key := builder.CreateString(k)

		//fbs.Uint8ColumnStart(builder)
		spec.columnStart(builder)
		//fbs.Uint8ColumnAddUnicode(builder, key)
		spec.columnAddKey(builder, k)
		//fbs.Uint8ColumnAddUint8(builder, voff)
		spec.columnAddValue(builder, voff)

		//offsets[i] = fbs.Uint8ColumnEnd(builder)
		offsets[i] = spec.columnEnd(builder)
		i++
	}

	//fbs.ColumnedEntityStartU8Vector(builder, sz)
	spec.entityStartVector(builder, sz)

	for i := sz - 1; i >= 0; i-- {
		builder.PrependUOffsetT(offsets[i])
	}

	return builder.EndVector(sz)
}

/*
func buildNullColumn(builder *flatbuffers.Builder, nulls map[uint32]uint) flatbuffers.UOffsetT {
	sz := len(nulls)

	offsets := make([]flatbuffers.UOffsetT, sz)

	i := 0
	for k, v := range nulls {
		//key := builder.CreateString(k)

		fbs.NullColumnStart(builder)
		fbs.NullColumnAddUnicode(builder, k)
		fbs.NullColumnAddCount(builder, uint32(v))

		offsets[i] = fbs.NullColumnEnd(builder)
		i++
	}

	fbs.ColumnizedEntityStartNullVector(builder, sz)

	for i := sz - 1; i >= 0; i-- {
		builder.PrependUOffsetT(offsets[i])
	}

	return builder.EndVector(sz)
}
*/
