// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"context"
	"fmt"
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

func fmtPrintlnStr(element string) {
	fmt.Println(element)
}

func fmtPrintlnNum(element int) {
	fmt.Println(element)
}

func main() {
	register.Function1x0(fmtPrintlnStr)
	register.Function1x0(fmtPrintlnNum)

	// beam.Init() is an initialization hook that must be called
	// near the beginning of main(), before creating a pipeline.
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	words := beam.Create(s, "Hello", "world", "it`s", "Beam")
	output(s, words, reflect.String)

	//Create a numerical PCollection
	numbers := beam.Create(s, 2, 3, 5, 7, 11)
	output(s, numbers, reflect.Int)

	err := beamx.Run(context.Background(), p)
	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

func output(s beam.Scope, input beam.PCollection, t reflect.Kind) {
	switch t.String() {
	case reflect.Int.String():
		beam.ParDo0(s, fmtPrintlnNum, input)
	case reflect.String.String():
		beam.ParDo0(s, fmtPrintlnStr, input)
	}
}
