#!/bin/bash

usage() {
  echo -e "Usage: $0 [-i <input-file>] [-o <path>]\n"\
       "where\n"\
       "-i defines an input file\n"\
       "-o defines an output path\n"\
       "-e defines an executor: hadoop or yarn, yarn but default\n"\
       "\n"\
        1>&2
  exit 1
}


while getopts ":i:o:e:" opt; do
    case "$opt" in
        i)  INPUT_FILE=${OPTARG} ;;
        o)  OUTPUT_PATH=${OPTARG} ;;
        e)  EXECUTOR=${OPTARG} ;;
        *)  usage ;;
    esac
done

if [[ -z "$INPUT_PATH" ]];
then
  INPUT_PATH="/bdpc/hadoop_mr/flights-processor/input"
fi

if [[ -z "$OUTPUT_PATH" ]];
then
  OUTPUT_PATH="/bdpc/hadoop_mr/flights-processor/output"
fi

if [[ -z "$EXECUTOR" ]];
then
  EXECUTOR="yarn"
fi

THIS_FILE=$(readlink -f "$0")
THIS_PATH=$(dirname "$THIS_FILE")
BASE_PATH=$(readlink -f "$THIS_PATH/../")
APP_PATH="$THIS_PATH/flights-processor-1.0-SNAPSHOT-jar-with-dependencies.jar"

hadoop fs -rm -R $INPUT_PATH $OUTPUT_PATH
hadoop fs -mkdir -p $INPUT_PATH
echo "copy $INPUT_FILE to $INPUT_PATH"
hadoop fs -cp $INPUT_FILE $INPUT_PATH

echo ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
echo "THIS_FILE = $THIS_FILE"
echo "THIS_PATH = $THIS_PATH"
echo "BASE_PATH = $BASE_PATH"
echo "APP_PATH = $APP_PATH"
echo "-------------------------------------"
echo "INPUT_PATH = $INPUT_PATH"
echo "OUTPUT_PATH = $OUTPUT_PATH"
echo "-------------------------------------"

mapReduceArguments=(
  "$APP_PATH"
  "com.globallogic.hadoop.mr.flights.Processor"
  "$INPUT_PATH"
  "$OUTPUT_PATH"
)

SUBMIT_CMD="${EXECUTOR} jar ${mapReduceArguments[@]}"
echo "$SUBMIT_CMD"
${SUBMIT_CMD}

echo "<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<"
