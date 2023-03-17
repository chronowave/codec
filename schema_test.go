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
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v10/arrow"
	"google.golang.org/protobuf/proto"

	"github.com/chronowave/codec/_test"
	"github.com/chronowave/ssql/go"
)

func TestSchemaFromBytes(t *testing.T) {
	firstVersion, err := LoadCodecSchema()
	if err != nil {
		t.Errorf("failed to load firstVersion from _testdata: %v", err)
		return
	}

	info, err := firstVersion.ToArrowFlightStream()
	if err != nil {
		t.Errorf("failed to serialize firstVersion: %v", err)
		return
	}

	var secondVersion Schema
	err = secondVersion.FromArrowFlightStream(firstVersion.revision, info)
	if err != nil {
		t.Errorf("failed to deserialize secondVersion from _testdata: %v", err)
		return
	}

	if !reflect.DeepEqual(firstVersion, &secondVersion) {
		t.Errorf("schema doesn't match")
	}
}

func TestSchemaAddFields(t *testing.T) {
	firstVersion, err := LoadCodecSchema()
	if err != nil {
		t.Errorf("failed to load firstVersion from _testdata: %v", err)
		return
	}

	alter := arrow.NewSchema([]arrow.Field{
		{
			Name: "field4",
			Type: arrow.ListOfField(arrow.Field{
				Name: "field5",
				Type: arrow.PrimitiveTypes.Float64,
			}),
		},
	}, nil)

	alteredSchema, err := firstVersion.AddFields(alter)
	if err != nil {
		t.Errorf("failed to update schema %v", err)
		return
	}

	data, err := alteredSchema.ToArrowFlightStream()
	if err != nil {
		t.Errorf("failed to convert schema to Arrow Flight format %v", err)
		return
	}

	var schema Schema
	err = schema.FromArrowFlightStream(2, data)
	if err != nil {
		t.Errorf("failed to deserialize Arrow Flight format: %v", err)
		return
	}

	if schema.nextCode-1 != firstVersion.nextCode {
		t.Errorf("schema doesn't match upgraded=%v, prev=%v", schema, firstVersion)
	}
}

func TestReconcileStatement(t *testing.T) {
	schema, err := LoadCodecSchema()
	if err != nil {
		t.Errorf("failed to load schema")
		return
	}

	//stmt, errs := ssql.Parse(`find group-by($c), count($c)
	stmt, errs := ssql.Parse(`find $c, $a
from flight_test
where
[$c /field3/field1 between(2, 80)]
[$a /field4 [/field1 isnull]]`)
	//[$b /field4 [$c /field1 eq(15)] [/field2 eq(81)]]`,
	//[$b /field5 [eq(4)]]`,
	//[$b /field4/field3 contain("b")]`,
	if len(errs) > 0 {
		t.Errorf("SSQL err %v", errs)
		return
	}

	if stmt, err = schema.ReconcileStatement(stmt); err != nil {
		t.Errorf("reconcile statement with schema err %v", err)
		return
	}

	data, err := proto.Marshal(stmt)
	if err != nil {
		t.Errorf("failed to marshal statement protobuf: %v", err)
		return
	}

	err = os.WriteFile("/tmp/stmt", data, os.ModePerm)
	if err != nil {
		t.Errorf("error to save statement: %v", err)
	}
}

func LoadCodecSchema() (*Schema, error) {
	info, err := _test.LoadJsonSchema(filepath.Join("_testdata", "schema.json"))
	if err != nil {
		return nil, err
	}

	var firstVersion Schema
	err = firstVersion.FromArrowFlightStream(1, info)
	if err != nil {
		return nil, err
	}

	return firstVersion.clone()
}
