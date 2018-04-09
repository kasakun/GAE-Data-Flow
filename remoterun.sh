#!/bin/bash

INPUT_PATH="gs://chen-zeyu-wine-dataflow/input/filtered_output_dataset.csv"
OUTPUT_PATH="gs://chen-zeyu-wine-dataflow/Chardonnay/"
RUNNER="DataflowRunner"
PROJECT_ID="chen-zeyu-wine-dataflow"
TEMP="gs://chen-zeyu-wine-dataflow/tmp/"

VARIETY="Chardonnay" #test Red Blend Chardonnay

MODE1="--bottles_sold"
MODE2="--dollars_sold"
MODE3="--winery_bottles_sold"
MODE4="--winery_dollars_sold"

MODE5="--purchased_together"

# Command Line
python main.py \
--input ${INPUT_PATH} \
--output ${OUTPUT_PATH} \
--runner ${RUNNER} \
--project ${PROJECT_ID} \
--temp_location ${TEMP} \
${MODE1}


# Command Line
python main.py \
--input ${INPUT_PATH} \
--output ${OUTPUT_PATH} \
--runner ${RUNNER} \
--project ${PROJECT_ID} \
--temp_location ${TEMP} \
${MODE2}


# Command Line
python main.py \
--input ${INPUT_PATH} \
--output ${OUTPUT_PATH} \
--runner ${RUNNER} \
--project ${PROJECT_ID} \
--temp_location ${TEMP} \
${MODE3}

# Command Line
python main.py \
--input ${INPUT_PATH} \
--output ${OUTPUT_PATH} \
--runner ${RUNNER} \
--project ${PROJECT_ID} \
--temp_location ${TEMP} \
${MODE4}


# Command Line
python main.py \
--input ${INPUT_PATH} \
--output ${OUTPUT_PATH} \
--runner ${RUNNER} \
--project ${PROJECT_ID} \
--temp_location ${TEMP} \
${MODE1} \
--variety "${VARIETY}"

# Command Line
python main.py \
--input ${INPUT_PATH} \
--output ${OUTPUT_PATH} \
--runner ${RUNNER} \
--project ${PROJECT_ID} \
--temp_location ${TEMP} \
${MODE2} \
--variety "${VARIETY}"

# Command Line
python main.py \
--input ${INPUT_PATH} \
--output ${OUTPUT_PATH} \
--runner ${RUNNER} \
--project ${PROJECT_ID} \
--temp_location ${TEMP} \
${MODE3} \
--variety "${VARIETY}"

# Command Line
python main.py \
--input ${INPUT_PATH} \
--output ${OUTPUT_PATH} \
--runner ${RUNNER} \
--project ${PROJECT_ID} \
--temp_location ${TEMP} \
${MODE4} \
--variety "${VARIETY}"

# Command Line
python main.py \
--input ${INPUT_PATH} \
--output ${OUTPUT_PATH} \
--runner ${RUNNER} \
--project ${PROJECT_ID} \
--temp_location ${TEMP} \
${MODE5}
