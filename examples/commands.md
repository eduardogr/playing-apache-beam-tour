
# Introduction

## Beam Concepts

### Runners

#### Locally (DirectRunner)
```bash
$ go install github.com/apache/beam/sdks/v2/go/examples/wordcount
$ wordcount --input <PATH_TO_INPUT_FILE> --output counts
```

#### Using Google DataFlow Runner

```bash
wordcount --input gs://dataflow-samples/shakespeare/kinglear.txt \
--output gs://<your-gcs-bucket>/counts \
--runner dataflow \
--project your-gcp-project \
--region your-gcp-region \
--temp_location gs://<your-gcs-bucket>/tmp/ \
--staging_location gs://<your-gcs-bucket>/binaries/ \
--worker_harness_container_image=apache/beam_go_sdk:latest
```
