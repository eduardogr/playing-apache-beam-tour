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
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/debug"
)

const (
	STRATEGY_NUMBERS = "numbers"
	STRATEGY_STRINGS = "strings"
)

func isOdd(element int) bool {
	return element%2 == 1
}

func notBeginsWithLetterA(word string) bool {
	return !strings.HasPrefix(strings.ToUpper(word), "A")
}

func main() {
	register.Function1x1(isOdd)
	register.Function1x1(notBeginsWithLetterA)

	ctx := context.Background()

	// beam.Init() is an initialization hook that must be called
	// near the beginning of main(), before creating a pipeline.
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	strategy := "strings"

	switch strategy {
	case "numbers":
		numbersPipeline(ctx, p, s)
	case "strings":
		stringsPipeline(ctx, p, s)
	}
}

func numbersPipeline(ctx context.Context, p *beam.Pipeline, s beam.Scope) {
	// List of elements
	input := beam.Create(s, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)

	// The [input] filtered with the applyTransform()
	output := applyTransformGetEvenNumbers(s, input)

	debug.Printf(s, "PCollection filtered value: %v", output)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

func stringsPipeline(ctx context.Context, p *beam.Pipeline, s beam.Scope) {
	// List of elements
	str := "To be, or not to be: that is the question: Whether 'tis nobler in the mind to suffer The slings and arrows of outrageous fortune, Or to take arms against a sea of troubles, And by opposing end them. To die: to sleep"

	input := beam.CreateList(s, strings.Split(str, " "))

	// The [input] filtered with the applyTransform()
	output := applyTransformBeginWithLetterA(s, input)

	debug.Printf(s, "PCollection filtered value: %v", output)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// It filters out all the elements of PCollection that don't satisfy the predicate
// The method filters the collection so that the numbers are even
func applyTransformGetEvenNumbers(s beam.Scope, input beam.PCollection) beam.PCollection {
	return filter.Exclude(s, input, isOdd)
}

// It filters out all the elements of PCollection that don't satisfy the predicate
// The method filters the collection so that the strings start with the letter 'a'
func applyTransformBeginWithLetterA(s beam.Scope, input beam.PCollection) beam.PCollection {
	return filter.Include(s, input, notBeginsWithLetterA)
}
