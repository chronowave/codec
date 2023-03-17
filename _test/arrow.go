package _test

import (
	"fmt"
	"os"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/flight"
	"github.com/apache/arrow/go/v10/arrow/memory"

	"github.com/chronowave/fbs/go"
)

// / Load Schema in JSON format and convert to arrow schema in serialized bytes
func LoadJsonSchema(path string) ([]byte, error) {
	var schema _Schema
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	err = schema.UnmarshalJSON(data)
	if err != nil {
		return nil, err
	}

	return SerializeAsArrowSchema(&schema)
}

func SerializeAsArrowSchema(schema *_Schema) ([]byte, error) {
	fields := make([]arrow.Field, len(schema.Fields))
	for i, wrapper := range schema.Fields {
		fields[i] = ToArrowField(wrapper)
	}

	arrowSchema := arrow.NewSchema(fields, nil)
	return flight.SerializeSchema(arrowSchema, memory.DefaultAllocator), nil
}

func ToArrowField(f FieldWrapper) arrow.Field {
	dataType := f.arrowType
	if len(f.Children) > 0 {
		fields := make([]arrow.Field, len(f.Children))
		for i, wrapper := range f.Children {
			fields[i] = ToArrowField(wrapper)
		}

		switch f.Marker {
		case fbs.MarkerSOO:
			dataType = arrow.StructOf(fields...)
		case fbs.MarkerSOA:
			dataType = arrow.ListOfField(fields[0])
		default:
			panic(fmt.Sprintf("unsupported field type %v, must be [STRUCT, LIST]", f.arrowType))
		}
	}

	//f.Children
	return arrow.Field{
		Name:     f.Name,
		Type:     dataType,
		Nullable: f.Nullable,
		Metadata: f.arrowMeta,
	}
}

func FromArrowField(arrowField arrow.Field) FieldWrapper {
	var marker fbs.Marker
	var children []FieldWrapper
	switch arrowField.Type.(type) {
	case *arrow.NullType:
		marker = fbs.MarkerNULL
	case *arrow.BooleanType:
		marker = fbs.MarkerBOOL
	case *arrow.Int8Type:
		marker = fbs.MarkerINT8
	case *arrow.Int16Type:
		marker = fbs.MarkerINT16
	case *arrow.Int32Type:
		marker = fbs.MarkerINT32
	case *arrow.Int64Type:
		marker = fbs.MarkerINT64
	case *arrow.Uint8Type:
		marker = fbs.MarkerUINT8
	case *arrow.Uint16Type:
		marker = fbs.MarkerUINT16
	case *arrow.Uint32Type:
		marker = fbs.MarkerUINT32
	case *arrow.Uint64Type:
		marker = fbs.MarkerUINT64
	case *arrow.Float16Type:
		marker = fbs.MarkerFLT32
	case *arrow.Float32Type:
		marker = fbs.MarkerFLT32
	case *arrow.Float64Type:
		marker = fbs.MarkerFLT64
	case *arrow.StringType:
		marker = fbs.MarkerTEXT
	case *arrow.Date32Type:
		marker = fbs.MarkerDATE32
	case *arrow.Date64Type:
		marker = fbs.MarkerDATE64
	case *arrow.ListType:
		marker = fbs.MarkerSOA
		children = []FieldWrapper{
			FromArrowField(arrowField.Type.(*arrow.ListType).ElemField()),
		}
	case *arrow.MapType:
	case *arrow.StructType:
		marker = fbs.MarkerSOA
		structType := arrowField.Type.(*arrow.StructType)
		fields := structType.Fields()
		children = make([]FieldWrapper, len(fields))
		for i, field := range fields {
			children[i] = FromArrowField(field)
		}
	case *arrow.FixedSizeListType:
		marker = fbs.MarkerSOA
		children = []FieldWrapper{
			FromArrowField(arrowField.Type.(*arrow.FixedSizeListType).ElemField()),
		}
	}

	return FieldWrapper{
		Field{
			Marker:    marker,
			Name:      arrowField.Name,
			arrowType: arrowField.Type,
			Type:      marker.String(),
			Nullable:  arrowField.Nullable,
			Children:  children,
		},
	}
}
