#!/usr/bin/env python2.7
###################################################################################################################################################
# Wordcount Example written by David Cabinian
# dhcabinian@gatech.edu
# Written for python 2.7
# Run python wordcount.py --help for information.
###################################################################################################################################################
import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
###################################################################################################################################################

def run(args, pipeline_args):

    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

        lines = p | ReadFromText(args.input)

        def SplitIntoWords(line):
            import re
            return re.findall(r'[A-Za-z\']+', line)

        def PairWithOne(word):
            return (word, 1)

        def FormatResult(word_count):
            (word, count) = word_count
            return '%s\t%s' % (word, count)

        output = (
            lines
            | 'Split' >> beam.FlatMap(SplitIntoWords)
            | 'PairWithOne' >> beam.Map(PairWithOne)
            | 'GroupAndSum' >> beam.CombinePerKey(sum)
            | 'Format' >> beam.Map(FormatResult)
        )

        output | WriteToText(args.output, 'csv', num_shards=1)

###################################################################################################################################################
# DO NOT MODIFY BELOW THIS LINE
###################################################################################################################################################
if __name__ == '__main__':
    # Try wordcount.py --help for more information
    # View https://docs.python.org/3/library/argparse.html for more information on how it works
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description="ECE 6102 Assignment 3 Wordcount Example", epilog="Example Usages:\npython wordcount.py --input textfile.txt --output wordcount.csv --runner Direct\npython wordcount.py --input $BUCKET/input_files/textfile.txt --output $BUCKET/wordcount.csv --runner DataflowRunner --project $PROJECT --temp_location $BUCKET/tmp/")
    parser.add_argument('--input', help="Input file to process.", required=True)
    parser.add_argument('--output', help="Output file to write results to.", required=True)
    parser.add_argument('--project', help="Your Google Cloud Project ID.")
    parser.add_argument('--runner', help="The runner you would like to use for the map reduce.", choices=['Direct', 'DataflowRunner'], required=True)
    parser.add_argument('--temp_location', help="Location where temporary files should be stored.")
    args = parser.parse_args()

    # Separating Pipeline options from IO options
    # HINT: pipeline args go nicely into: options=PipelineOptions(pipeline_args)
    if args.runner  == "DataflowRunner":
        if None in [args.project, args.temp_location]:
            raise Exception("Missing some pipeline options.")
        pipeline_args = []
        pipeline_args.append("--runner")
        pipeline_args.append(args.runner)
        pipeline_args.append("--project")
        pipeline_args.append(args.project)
        pipeline_args.append("--temp_location")
        pipeline_args.append(args.temp_location)
    else:
        pipeline_args = []

    run(args, pipeline_args)
