#!/bin/bash
INPUT_PATH="test.csv"
OUTPUT_PATH="output/direct/"
RUNNER="Direct"
PROJECT_ID="chen-zeyu-wine-dataflow"
TEMP="."

MODE1="--bottles_sold"
MODE2="--dollars_sold"
MODE3="--winery_bottles_sold"
MODE4="--winery_dollars_sold"
MODE5="--bottles_sold --variety Chardonnay"
MODE6="--dollars_sold --variety Chardonnay"
MODE7="--winery_bottles_sold --variety Chardonnay"
MODE8="--winery_dollars_sold --variety Chardonnay"

MODE9="--purchased_together"

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
${MODE5}

# Command Line
python main.py \
--input ${INPUT_PATH} \
--output ${OUTPUT_PATH} \
--runner ${RUNNER} \
--project ${PROJECT_ID} \
--temp_location ${TEMP} \
${MODE6}

# Command Line
python main.py \
--input ${INPUT_PATH} \
--output ${OUTPUT_PATH} \
--runner ${RUNNER} \
--project ${PROJECT_ID} \
--temp_location ${TEMP} \
${MODE7}

# Command Line
python main.py \
--input ${INPUT_PATH} \
--output ${OUTPUT_PATH} \
--runner ${RUNNER} \
--project ${PROJECT_ID} \
--temp_location ${TEMP} \
${MODE8}

# Command Line
python main.py \
--input ${INPUT_PATH} \
--output ${OUTPUT_PATH} \
--runner ${RUNNER} \
--project ${PROJECT_ID} \
--temp_location ${TEMP} \
${MODE9}
