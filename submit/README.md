# GAE-Data-Flow

## How to run
Firstly, go to the virtual environment.

To run it direclty in terminal

Project ID: chen-zeyu-wine-dataflow

BUCKET: gs://chen-zeyu-wine-dataflow

source_file_name: main.py

Input file: SubmissionDataset.csv

Variety: Chardonnay

To set bucket
```
export BUCKET=gs://chen-zeyu-wine-dataflow
```

To run
```
python run_all_pipelines.py --input $BUCKET/SubmissionDataset.csv --temp_location $BUCKET/tmp/ --project chen-zeyu-wine-dataflow --variety Chardonnay --bucket $BUCKET --source_file_name main.py
```
