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
	"testing"

	"github.com/chronowave/fbs/go"
)

func TestSchemaColumnizeJson(t *testing.T) {
	doc, err := os.ReadFile("_testdata/data.json")
	if err != nil {
		t.Error(err)
		return
	}

	schema, err := LoadCodecSchema()
	if err != nil {
		t.Error(err)
		return
	}

	columnized, err := schema.ColumnizeJson(doc)
	if err != nil {
		t.Error(err)
		return
	}

	var entities []*ColumnedEntity
	for _, entity := range columnized {
		data, err := SerializeColumnedEntity(entity)
		if err != nil {
			t.Error(err)
			return
		}
		newEntity, err := DeserializeColumnedEntity(data)
		if err != nil {
			t.Error(err)
			return
		}

		entities = append(entities, newEntity)
	}

	buf := FlattenColumnizedEntities(entities)

	_ = os.WriteFile("/tmp/ssss", buf, os.ModePerm)

	deserialized := fbs.GetRootAsColumnizedEntities(buf, 0)

	//if deserialized.TextContentLength() != len(columnized) {
	//t.Errorf("serialized text content doesn't equal")
	//return
	//}

	if deserialized.EntitiesLength() != len(columnized) {
		t.Errorf("serialized entities doesn't equal")
		return
	}
}

func TestJaegerSchema(t *testing.T) {
	/*
			GenerateTestData(t, &TestData{
				SchemaPath: "jaeger/jaeger_test.schema",
				DataPath:   "jaeger/data.json",
				UniqueDoc:  "jaeger/unique.json",
				SSQL: `find $a from span where
		                     [$a /logs [/timestamp gt(1969-10-01)]
		                               [/fields [/v_type eq(4)]]
		                     ]`,
			}, 65536)
	*/

	GenerateTestData(t, &TestData{
		SchemaPath: "jaeger/jaeger_test.schema",
		DataPath:   "jaeger/data.json",
		//UniqueDoc:  "jaeger/unique.json",
		SSQL: `find $a from span where [$a /operation_name]`,
		/*
					SSQL: `find $a from span where
			                     [$a /]
			                     [$st /start_time BETWEEN(0, 1672781330598)]
			                     [/tags [/key CONTAIN('key_str')] [/v_str CONTAIN('key_string')]]`,

		*/
	}, 20)
}

func TestGenData(t *testing.T) {
	queries := []string{
		`find $a from span where [$a /] [$st /start_time BETWEEN(0, 1672865020661963)] [/tags [/key CONTAIN('span.kind')] [/v_str CONTAIN('client')]] limit 1`,
		`FIND $ref, $sid, $svc FROM span WHERE [$ref /references][$sid /span_id][$svc /process/service_name][/start_time BETWEEN(1672942772508, 1673547572508)]`,
	}
	LoadTestDataSet(t, &TestData{
		SchemaPath: "/home/rleiwang/workspace/chronowave/opentelemetry/jaeger/testdata/span.schema",
		DataPath:   "/home/rleiwang/workspace/chronowave/opentelemetry/jaeger/testdata",
		SSQL:       queries[1],
	}, 2555)
}
