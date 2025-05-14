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
	"flag"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/x/beamx"
)

var (
	// Set this required option to specify where to read the input.
	input = flag.String("input", "", "File(s) to read.")

	// Set this required option to specify where to write the output.
	output = flag.String("output", "", "Output file (required).")

	// By default, this example reads from a public dataset containing the text of
	// King Lear. Set this option to choose a different input file or glob.
	inputType = flag.String("input_type", "shakespeare", "Output file (required).")
)

const (
	INPUT_SHAKESPEARE = "gs://apache-beam-samples/shakespeare/kinglear.txt"
	INPUT_NYC_TAXI    = "gs://apache-beam-samples/nyc_taxi/misc/sample1000.csv"
)

// Execution: go run main.go -output=./output/output.csv -input_type=nyc_taxi
// Execution: go run main.go -output=./output/output.txt -input_type=shakespeare
func main() {
	// If beamx or Go flags are used, flags must be parsed first.
	// before beam.Init() is called.
	flag.Parse() // --<option>=<value>

	ctx := context.Background()

	// Input validation is done as usual. Note that it must be after Init().
	if *output == "" {
		log.Exitf(ctx, "No output provided")
	}

	// beam.Init() is an initialization hook that must be called
	// near the beginning of main(), before creating a pipeline.
	beam.Init()

	p := beam.NewPipeline()
	s := p.Root()

	// Read from option input file
	var input_to_read string
	if *input == "" {
		switch *inputType {
		case "shakespeare":
			input_to_read = INPUT_SHAKESPEARE
		case "nyc_taxi":
			input_to_read = INPUT_NYC_TAXI
		default:
			log.Exitf(ctx, "No input_type provided")
		}
	} else {
		input_to_read = *input
	}
	lines := textio.Read(s, input_to_read)

	// Write to option output file
	textio.Write(s, *output, lines)

	err := beamx.Run(ctx, p)

	if err != nil {
		log.Exitf(ctx, "Failed to execute job: %v", err)
	}
}
