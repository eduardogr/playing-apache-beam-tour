/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/top"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

const (
	INDEX_PASSENGER_COUNT = 3
	INDEX_TRIP_DISTANCE   = 4
	INDEX_TIP_AMOUNT      = 13
	INDEX_TOTAL_AMOUNT    = 16
)

func less(a, b float64) bool {
	return a < b
}

func tripDistanceTransform(line string) float64 {
	taxi := strings.Split(strings.TrimSpace(line), ",")
	if len(taxi) > INDEX_TRIP_DISTANCE {
		cost, _ := strconv.ParseFloat(taxi[INDEX_TRIP_DISTANCE], 64)
		return cost
	}
	return 0.0
}

func tipAmountTransform(line string) float64 {
	taxi := strings.Split(strings.TrimSpace(line), ",")
	if len(taxi) > INDEX_TIP_AMOUNT {
		cost, _ := strconv.ParseFloat(taxi[INDEX_TIP_AMOUNT], 64)
		return cost
	}
	return 0.0
}

func totalAmountTransform(line string) float64 {
	taxi := strings.Split(strings.TrimSpace(line), ",")
	if len(taxi) > INDEX_TOTAL_AMOUNT {
		cost, _ := strconv.ParseFloat(taxi[INDEX_TOTAL_AMOUNT], 64)
		return cost
	}
	return 0.0
}

func printTripDistance(elements []float64) {
	for _, element := range elements {
		fmt.Println("Trip distance: ", element)
	}
}

func printTipAmount(elements []float64) {
	for _, element := range elements {
		fmt.Println("Tip amount: ", element)
	}
}

func printTotalAmount(elements []float64) {
	for _, element := range elements {
		fmt.Println("Total amount: ", element)
	}
}

func main() {
	register.Function2x1(less)
	register.Function1x1(totalAmountTransform)
	register.Function1x1(tipAmountTransform)
	register.Function1x1(tripDistanceTransform)
	register.Function1x0(printTripDistance)
	register.Function1x0(printTipAmount)
	register.Function1x0(printTotalAmount)

	// beam.Init() is an initialization hook that must be called
	// near the beginning of main(), before creating a pipeline.
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	input := Read(s, "gs://apache-beam-samples/nyc_taxi/misc/sample1000.csv")

	cost := applyTransform(s, input, tipAmountTransform)

	fixedSizeElements := top.Largest(s, cost, 10, less)

	output(s, fixedSizeElements, printTipAmount)

	err := beamx.Run(context.Background(), p)
	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// Read reads from fiename(s) specified by a glob string and a returns a PCollection<string>.
func Read(s beam.Scope, glob string) beam.PCollection {
	return textio.Read(s, glob)
}

// ApplyTransform converts to uppercase all elements in a PCollection<string>.
func applyTransform(s beam.Scope, input beam.PCollection, transformFunc func(line string) float64) beam.PCollection {
	return beam.ParDo(s, transformFunc, input)
}

func output(s beam.Scope, input beam.PCollection, printFunc func(elements []float64)) {
	beam.ParDo0(s, printFunc, input)
}
