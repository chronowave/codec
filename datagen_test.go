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
	"io/fs"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/chronowave/fbs/go"
	"github.com/chronowave/ssql/go"
)

//find $a, $b from jaeger_test_9 where [$a /] [$b /trace_id]

type TestData struct {
	SchemaPath     string
	JsonSchemaPath string
	DataPath       string
	UniqueDoc      string
	SSQL           string
}

func LoadTestDataSet(t *testing.T, data *TestData, num int) {
	var (
		buf []byte
		err error
	)

	if len(data.SchemaPath) > 0 {
		buf, err = os.ReadFile(data.SchemaPath)
		if err != nil {
			t.Error("failed to load arrow binary schema", err)
			return
		}
	}

	var schema Schema
	err = schema.FromArrowFlightStream(1, buf)
	if err != nil {
		t.Errorf("failed to deserialize schema: %v", err)
		return
	}

	var entities []*ColumnedEntity
	err = filepath.Walk(data.DataPath, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() || !strings.HasSuffix(path, ".json") {
			return nil
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		docs, err := schema.ColumnizeJson(data)
		if err != nil {
			return err
		}

		entities = append(entities, docs...)

		return nil
	})

	if num < len(entities) {
		entities = entities[:num]
	}

	GenData(t, entities, len(entities))
	GenStmt(t, &schema, data.SSQL)
}

func GenerateTestData(t *testing.T, data *TestData, num int) {
	var (
		buf []byte
		err error
	)
	if len(data.SchemaPath) > 0 {
		buf, err = os.ReadFile("_testdata/" + data.SchemaPath)
		if err != nil {
			t.Error("failed to load arrow binary schema", err)
			return
		}
	}

	var schema Schema
	err = schema.FromArrowFlightStream(1, buf)
	if err != nil {
		t.Errorf("failed to deserialize schema: %v", err)
		return
	}

	doc, err := os.ReadFile("_testdata/" + data.DataPath)
	if err != nil {
		t.Error("failed to load test data json", err)
		return
	}

	var uniqueEntity []*ColumnedEntity
	if len(data.UniqueDoc) > 0 {
		unique, err := os.ReadFile("_testdata/" + data.UniqueDoc)
		if err != nil {
			t.Error("failed to load test data json", err)
			return
		}

		uniqueEntity, err = schema.ColumnizeJson(unique)
		if err != nil {
			t.Error(err)
			return
		}
	}

	columnized, err := schema.ColumnizeJson(doc)
	if err != nil {
		t.Error(err)
		return
	}

	var template []*ColumnedEntity
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
		template = append(template, newEntity)
	}

	var entities []*ColumnedEntity
	target_len := len(columnized)
	if num > 0 {
		target_len = target_len * (num / target_len)
		unique := rand.Intn(num / len(template))
		for i := 0; i < num/len(template); i++ {
			if unique == i && len(uniqueEntity) > 0 {
				entities = append(entities, uniqueEntity...)
				for j := len(uniqueEntity); j < len(template); j++ {
					entities = append(entities, template[j])
				}
				fmt.Printf("appended unique doc to %v\n", unique)
			} else {
				entities = append(entities, template...)
			}
		}
	} else {
		entities = template
	}

	GenData(t, entities, target_len)
	GenStmt(t, &schema, data.SSQL)
}

func GenData(t *testing.T, entities []*ColumnedEntity, target_len int) {
	buf := FlattenColumnizedEntities(entities)

	_ = os.WriteFile("/tmp/ssss", buf, os.ModePerm)

	deserialized := fbs.GetRootAsColumnizedEntities(buf, 0)
	fmt.Printf("gen data: number of entitites: %v\n", deserialized.EntitiesLength())
	if deserialized.EntitiesLength() != target_len {
		t.Errorf("serialized entities doesn't equal")
		return
	}
}

func GenStmt(t *testing.T, schema *Schema, query string) {
	var err error
	//stmt, errs := ssql.Parse(`find group-by($c), count($c)
	stmt, errs := ssql.Parse(query)
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
