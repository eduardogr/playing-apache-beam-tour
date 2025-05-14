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
/*
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
	"regexp"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/filter"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/transforms/top"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	wordRE = regexp.MustCompile(`[a-zA-Z]+('[a-z])?`)
)

func less(a, b string) bool {
	return len(a) < len(b)
}

func findAllStringAndEmit(line string, emit func(string)) {
	for _, word := range wordRE.FindAllString(line, -1) {
		emit(word)
	}
}

func printElementsLines(elements []string) {
	for _, element := range elements {
		fmt.Println("Lines: ", element)
	}
}

func printElementsWords(elements []string) {
	for _, element := range elements {
		fmt.Println("Words: ", element)
	}
}

func isNotEmpty(element string) bool {
	return element != ""
}

func main() {
	register.Function2x0(findAllStringAndEmit)
	register.Function1x0(printElementsLines)
	register.Function1x0(printElementsWords)
	register.Function2x1(less)
	register.Function1x1(isNotEmpty)

	// beam.Init() is an initialization hook that must be called
	// near the beginning of main(), before creating a pipeline.
	beam.Init()

	p, s := beam.NewPipelineWithRoot()

	input := Read(s, "gs://apache-beam-samples/shakespeare/kinglear.txt")

	lines := filterLines(s, input)
	fixedSizeLines := top.Largest(s, lines, 10, less)
	output(s, printElementsLines, fixedSizeLines)

	words := getWords(s, lines)
	fixedSizeWords := top.Largest(s, words, 10, less)
	output(s, printElementsWords, fixedSizeWords)

	err := beamx.Run(context.Background(), p)
	if err != nil {
		log.Exitf(context.Background(), "Failed to execute job: %v", err)
	}
}

// Read reads from filename(s) specified by a glob string and a returns a PCollection<string>.
func Read(s beam.Scope, glob string) beam.PCollection {
	return textio.Read(s, glob)
}

// Read text file content line by line.
// resulting PCollection contains elements, where each element contains a single line of text from the input file.
func filterLines(s beam.Scope, input beam.PCollection) beam.PCollection {
	return filter.Include(s, input, isNotEmpty)
}

// getWords read text lines and split into PCollection of words.
func getWords(s beam.Scope, input beam.PCollection) beam.PCollection {
	return beam.ParDo(s, findAllStringAndEmit, input)
}

func output(s beam.Scope, printFunc func([]string), input beam.PCollection) {
	beam.ParDo0(s, printFunc, input)
}
