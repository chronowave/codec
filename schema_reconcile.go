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
	"golang.org/x/xerrors"
	"strconv"
	"strings"
	"time"

	"github.com/chronowave/ssql/go"
)

func (s *Schema) ReconcileStatement(stmt *ssql.Statement) (*ssql.Statement, error) {
	attributes, tupleIDs := make(map[string]*ssql.Field), make(map[string]uint32)
	cnt := uint32(0)

	var procExpr func([]string, []*ssql.Expr) error
	procExpr = func(parent []string, expr []*ssql.Expr) error {
		for _, e := range expr {
			switch e.Field.(type) {
			case *ssql.Expr_Tuple:
				tuple := e.Field.(*ssql.Expr_Tuple).Tuple
				key := strings.Join(append(parent, tuple.Path), "")
				field, ok := s.fields[key]
				if !ok {
					return fmt.Errorf("schema doesn't have entry path %s: [%s %s]", key, tuple.Name, tuple.Path)
				} else {
					attributes[tuple.Name] = field
					tuple.Unicode = field.Unicode
					tuple.Id = cnt
					tupleIDs[tuple.Name] = cnt
					cnt++
				}

				if tuple.Predicate == nil {
					tuple.Predicate = &ssql.Tuple_SelectOnly{SelectOnly: field.Type}
				} else {
					switch tuple.Predicate.(type) {
					case *ssql.Tuple_Nested:
						nested := tuple.Predicate.(*ssql.Tuple_Nested)
						if err := procExpr(append(parent, tuple.Path), nested.Nested.Expr); err != nil {
							return err
						}
					case *ssql.Tuple_Binary:
						tupleBinary := tuple.Predicate.(*ssql.Tuple_Binary)
						if err := reconcileBinary(tuple.Path, field, tupleBinary.Binary); err != nil {
							return err
						}
					case *ssql.Tuple_Unary:
						unary := tuple.Predicate.(*ssql.Tuple_Unary)
						if unary.Unary.Op != ssql.Operator_ISNULL && unary.Unary.Op != ssql.Operator_EXIST {
							if err := reconcileUnary(tuple.Path, field, unary.Unary.Operand); err != nil {
								return err
							}
						}
					case *ssql.Tuple_List:
						list := tuple.Predicate.(*ssql.Tuple_List)
						if err := reconcileList(field, list.List); err != nil {
							return err
						}
					case *ssql.Tuple_SelectOnly:
						tuple.Predicate = &ssql.Tuple_SelectOnly{SelectOnly: field.Type}
					}
				}
			case *ssql.Expr_Or:
				or := e.Field.(*ssql.Expr_Or).Or
				if err := procExpr(parent, or.Expr); err != nil {
					return err
				}
			}
		}
		return nil
	}

	if err := procExpr(nil, stmt.Where); err != nil {
		return nil, err
	}

	for _, attr := range stmt.Find {
		if attr == nil {
			continue
		}

		var ok bool
		attr.Arrow, ok = attributes[attr.Name]
		if !ok {
			return stmt, fmt.Errorf("attributes name %v is not part of where clause", attr.Name)
		}
		attr.TupleId = tupleIDs[attr.Name]
	}

	stmt.Aux.TupleCount = cnt

	return stmt, nil
}

func reconcileUnary(path string, field *ssql.Field, operand *ssql.Operand) error {
	switch field.Type {
	case ssql.Type_int8:
		if v, err := strconv.ParseInt(operand.Content, 10, 8); err != nil {
			return err
		} else {
			operand.Value = &ssql.Operand_Int8{Int8: int32(v)}
		}
	case ssql.Type_uint8:
		if v, err := strconv.ParseUint(operand.Content, 10, 8); err != nil {
			return err
		} else {
			operand.Value = &ssql.Operand_Uint8{Uint8: uint32(v)}
		}
	case ssql.Type_int16:
		if v, err := strconv.ParseInt(operand.Content, 10, 16); err != nil {
			return err
		} else {
			operand.Value = &ssql.Operand_Int16{Int16: int32(v)}
		}
	case ssql.Type_uint16:
		if v, err := strconv.ParseUint(operand.Content, 10, 16); err != nil {
			return err
		} else {
			operand.Value = &ssql.Operand_Uint16{Uint16: uint32(v)}
		}
	case ssql.Type_int32:
		if v, err := strconv.ParseInt(operand.Content, 10, 32); err != nil {
			return err
		} else {
			operand.Value = &ssql.Operand_Int32{Int32: int32(v)}
		}
	case ssql.Type_date32:
		if t, err := tryConvertDateToUnixTime(operand.Content, ssql.Timestamp_Second); err != nil {
			return err
		} else {
			operand.Value = &ssql.Operand_Date32{Date32: int32(t / (24 * 60 * 60))}
		}
	case ssql.Type_uint32:
		if v, err := strconv.ParseUint(operand.Content, 10, 32); err != nil {
			return err
		} else {
			operand.Value = &ssql.Operand_Uint32{Uint32: uint32(v)}
		}
	case ssql.Type_float32:
		if v, err := strconv.ParseFloat(operand.Content, 32); err != nil {
			return err
		} else {
			operand.Value = &ssql.Operand_Float32{Float32: float32(v)}
		}
	case ssql.Type_int64:
		if v, err := strconv.ParseInt(operand.Content, 10, 64); err != nil {
			return err
		} else {
			operand.Value = &ssql.Operand_Int64{Int64: v}
		}
	case ssql.Type_date64:
		if t, err := tryConvertDateToUnixTime(operand.Content, field.Timestamp.Timeunit); err != nil {
			return err
		} else {
			operand.Value = &ssql.Operand_Date64{Date64: t}
		}
	case ssql.Type_uint64:
		if v, err := strconv.ParseUint(operand.Content, 10, 64); err != nil {
			return err
		} else {
			operand.Value = &ssql.Operand_Uint64{Uint64: v}
		}
	case ssql.Type_float64:
		if v, err := strconv.ParseFloat(operand.Content, 64); err != nil {
			return err
		} else {
			operand.Value = &ssql.Operand_Float64{Float64: v}
		}
	case ssql.Type_bool:
		// do nothing
	case ssql.Type_text:
		operand.Value = &ssql.Operand_Text{Text: true}
	default:
		return fmt.Errorf("path %s is not a scalar data field", path)
	}
	return nil
}

func reconcileBinary(path string, field *ssql.Field, binary *ssql.Binary) error {
	switch field.Type {
	case ssql.Type_int8:
		if first, err := strconv.ParseInt(binary.First.Content, 10, 8); err != nil {
			return err
		} else if second, err := strconv.ParseInt(binary.Second.Content, 10, 8); err != nil {
			return err
		} else {
			binary.First.Value = &ssql.Operand_Int8{Int8: int32(first)}
			binary.Second.Value = &ssql.Operand_Int8{Int8: int32(second)}
		}
	case ssql.Type_uint8:
		if first, err := strconv.ParseUint(binary.First.Content, 10, 8); err != nil {
			return err
		} else if second, err := strconv.ParseUint(binary.Second.Content, 10, 8); err != nil {
			return err
		} else {
			binary.First.Value = &ssql.Operand_Uint8{Uint8: uint32(first)}
			binary.Second.Value = &ssql.Operand_Uint8{Uint8: uint32(second)}
		}
	case ssql.Type_int16:
		if first, err := strconv.ParseInt(binary.First.Content, 10, 16); err != nil {
			return err
		} else if second, err := strconv.ParseInt(binary.Second.Content, 10, 16); err != nil {
			return err
		} else {
			binary.First.Value = &ssql.Operand_Int16{Int16: int32(first)}
			binary.Second.Value = &ssql.Operand_Int16{Int16: int32(second)}
		}
	case ssql.Type_uint16:
		if first, err := strconv.ParseUint(binary.First.Content, 10, 16); err != nil {
			return err
		} else if second, err := strconv.ParseUint(binary.Second.Content, 10, 16); err != nil {
			return err
		} else {
			binary.First.Value = &ssql.Operand_Uint16{Uint16: uint32(first)}
			binary.Second.Value = &ssql.Operand_Uint16{Uint16: uint32(second)}
		}
	case ssql.Type_int32:
		if first, err := strconv.ParseInt(binary.First.Content, 10, 32); err != nil {
			return err
		} else if second, err := strconv.ParseInt(binary.Second.Content, 10, 32); err != nil {
			return err
		} else {
			binary.First.Value = &ssql.Operand_Int32{Int32: int32(first)}
			binary.Second.Value = &ssql.Operand_Int32{Int32: int32(second)}
		}
	case ssql.Type_date32:
		if first, err := tryConvertDateToUnixTime(binary.First.Content, ssql.Timestamp_Second); err != nil {
			return err
		} else if second, err := tryConvertDateToUnixTime(binary.Second.Content, ssql.Timestamp_Second); err != nil {
			return err
		} else {
			binary.First.Value = &ssql.Operand_Date32{Date32: int32(first / DayInMillis)}
			binary.Second.Value = &ssql.Operand_Date32{Date32: int32(second / DayInMillis)}
		}
	case ssql.Type_uint32:
		if first, err := strconv.ParseUint(binary.First.Content, 10, 32); err != nil {
			return err
		} else if second, err := strconv.ParseUint(binary.Second.Content, 10, 32); err != nil {
			return err
		} else {
			binary.First.Value = &ssql.Operand_Uint32{Uint32: uint32(first)}
			binary.Second.Value = &ssql.Operand_Uint32{Uint32: uint32(second)}
		}
	case ssql.Type_float32:
		if first, err := strconv.ParseFloat(binary.First.Content, 32); err != nil {
			return err
		} else if second, err := strconv.ParseFloat(binary.Second.Content, 32); err != nil {
			return err
		} else {
			binary.First.Value = &ssql.Operand_Float32{Float32: float32(first)}
			binary.Second.Value = &ssql.Operand_Float32{Float32: float32(second)}
		}
	case ssql.Type_int64:
		if first, err := strconv.ParseInt(binary.First.Content, 10, 64); err != nil {
			return err
		} else if second, err := strconv.ParseInt(binary.Second.Content, 10, 64); err != nil {
			return err
		} else {
			binary.First.Value = &ssql.Operand_Int64{Int64: first}
			binary.Second.Value = &ssql.Operand_Int64{Int64: second}
		}
	case ssql.Type_date64:
		unit := field.Timestamp.Timeunit
		if first, err := tryConvertDateToUnixTime(binary.First.Content, unit); err != nil {
			return err
		} else if second, err := tryConvertDateToUnixTime(binary.Second.Content, unit); err != nil {
			return err
		} else {
			binary.First.Value = &ssql.Operand_Date64{Date64: first}
			binary.Second.Value = &ssql.Operand_Date64{Date64: second}
		}
	case ssql.Type_uint64:
		if first, err := strconv.ParseUint(binary.First.Content, 10, 64); err != nil {
			return err
		} else if second, err := strconv.ParseUint(binary.Second.Content, 10, 64); err != nil {
			return err
		} else {
			binary.First.Value = &ssql.Operand_Uint64{Uint64: first}
			binary.Second.Value = &ssql.Operand_Uint64{Uint64: second}
		}
	case ssql.Type_float64:
		if first, err := strconv.ParseFloat(binary.First.Content, 64); err != nil {
			return err
		} else if second, err := strconv.ParseFloat(binary.Second.Content, 64); err != nil {
			return err
		} else {
			binary.First.Value = &ssql.Operand_Float64{Float64: first}
			binary.Second.Value = &ssql.Operand_Float64{Float64: second}
		}
	case ssql.Type_bool:
		fallthrough
	case ssql.Type_text:
		return fmt.Errorf("invalid between predicate for bool or text column")
	default:
		return fmt.Errorf("path %s is not a scalar data field", path)
	}
	return nil
}

func reconcileList(field *ssql.Field, operand *ssql.List) error {
	switch field.Type {
	case ssql.Type_int8:
		operand.Type = ssql.Type_int8
		for _, operand := range operand.Operands {
			if v, err := strconv.ParseInt(operand.Content, 10, 8); err != nil {
				return err
			} else {
				operand.Value = &ssql.Operand_Int8{Int8: int32(v)}
			}
		}
	case ssql.Type_uint8:
		operand.Type = ssql.Type_uint8
		for _, operand := range operand.Operands {
			if v, err := strconv.ParseUint(operand.Content, 10, 8); err != nil {
				return err
			} else {
				operand.Value = &ssql.Operand_Uint8{Uint8: uint32(v)}
			}
		}
	case ssql.Type_int16:
		operand.Type = ssql.Type_int16
		for _, operand := range operand.Operands {
			if v, err := strconv.ParseInt(operand.Content, 10, 16); err != nil {
				return err
			} else {
				operand.Value = &ssql.Operand_Int16{Int16: int32(v)}
			}
		}
	case ssql.Type_uint16:
		operand.Type = ssql.Type_uint16
		for _, operand := range operand.Operands {
			if v, err := strconv.ParseUint(operand.Content, 10, 16); err != nil {
				return err
			} else {
				operand.Value = &ssql.Operand_Uint16{Uint16: uint32(v)}
			}
		}
	case ssql.Type_int32:
		operand.Type = ssql.Type_int32
		for _, operand := range operand.Operands {
			if v, err := strconv.ParseInt(operand.Content, 10, 32); err != nil {
				return err
			} else {
				operand.Value = &ssql.Operand_Int32{Int32: int32(v)}
			}
		}
	case ssql.Type_uint32:
		operand.Type = ssql.Type_uint32
		for _, operand := range operand.Operands {
			if v, err := strconv.ParseUint(operand.Content, 10, 32); err != nil {
				return err
			} else {
				operand.Value = &ssql.Operand_Uint32{Uint32: uint32(v)}
			}
		}
	case ssql.Type_float32:
		operand.Type = ssql.Type_float32
		for _, operand := range operand.Operands {
			if v, err := strconv.ParseFloat(operand.Content, 32); err != nil {
				return err
			} else {
				operand.Value = &ssql.Operand_Float32{Float32: float32(v)}
			}
		}
	case ssql.Type_int64:
		operand.Type = ssql.Type_int64
		for _, operand := range operand.Operands {
			if v, err := strconv.ParseInt(operand.Content, 10, 64); err != nil {
				return err
			} else {
				operand.Value = &ssql.Operand_Int64{Int64: v}
			}
		}
	case ssql.Type_uint64:
		operand.Type = ssql.Type_uint64
		for _, operand := range operand.Operands {
			if v, err := strconv.ParseUint(operand.Content, 10, 64); err != nil {
				return err
			} else {
				operand.Value = &ssql.Operand_Uint64{Uint64: v}
			}
		}
	case ssql.Type_float64:
		operand.Type = ssql.Type_float64
		for _, operand := range operand.Operands {
			if v, err := strconv.ParseFloat(operand.Content, 64); err != nil {
				return err
			} else {
				operand.Value = &ssql.Operand_Float64{Float64: v}
			}
		}
	case ssql.Type_bool:
		return fmt.Errorf("invalid between predicate for bool or text column")
	case ssql.Type_text:
		operand.Type = ssql.Type_text
		for _, operand := range operand.Operands {
			operand.Value = &ssql.Operand_Text{Text: true}
		}
	}

	return nil
}

func tryConvertDateToUnixTime(date string, unit ssql.Timestamp_TimeUnit) (int64, error) {
	if v, err := strconv.ParseInt(date, 10, 64); err == nil {
		return v, nil
	}

	for _, layout := range []string{
		time.RFC3339Nano,
		"2006-01-02T15:04:05.999999Z07:00",
		"2006-01-02T15:04:05.999Z07:00",
		time.RFC3339,
		"2006-01-02T15:04:05.999Z",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05",
		"2006-01-02",
	} {
		if t, err := time.Parse(layout, date); err == nil {
			switch unit {
			case ssql.Timestamp_Second:
				return t.Unix(), nil
			case ssql.Timestamp_Millisecond:
				return t.UnixMilli(), nil
			case ssql.Timestamp_Microsecond:
				fallthrough
			case ssql.Timestamp_Nanosecond:
				return t.UnixMicro(), nil
			}
		}
	}

	return 0, xerrors.Errorf("unable to parse date string: %s", date)
}
