#!/usr/bin/env python2.7
###################################################################################################################################################
# Template written by David Cabinian
# dhcabinian@gatech.edu
# Written for python 2.7
# Run python template.py --help for information.
###################################################################################################################################################
# DO NOT MODIFY THESE IMPORTS / DO NOT ADD IMPORTS IN THIS NAMESPACE
# Importing a filesystem library such as ['sys', 'os', 'shutil'] will result in loss of all homework points.
###################################################################################################################################################
import argparse
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
###################################################################################################################################################

def run(args, pipeline_args):
    # INSERT YOUR CODE HERE
    with beam.Pipeline(options = PipelineOptions(pipeline_args)) as p:
        lines = p | ReadFromText(args.input)
    pass

###################################################################################################################################################
# DO NOT MODIFY BELOW THIS LINE
###################################################################################################################################################
if __name__ == '__main__':
    # This function will parse the required arguments for you.
    # Try template.py --help for more information
    # View https://docs.python.org/3/library/argparse.html for more information on how it works
    parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter, description="ECE 6102 Assignment 3", epilog="Example Usages:\npython test.py --input small_dataset.csv --output bottles.csv --runner Direct --bottles_sold\npython wine_test.py --input $BUCKET/input_files/small_dataset.csv --output $BUCKET/bottles.csv --runner DataflowRunner --project $PROJECT --temp_location $BUCKET/tmp/ --bottles_sold")
    parser.add_argument('--input', help="Input file to process.", required=True)
    parser.add_argument('--output', help="Output file to write results to.", required=True)
    parser.add_argument('--project', help="Your Google Cloud Project ID.")
    parser.add_argument('--runner', help="The runner you would like to use for the map reduce.", choices=['Direct', 'DataflowRunner'], required=True)
    parser.add_argument('--temp_location', help="Location where temporary files should be stored.")
    parser.add_argument('--num_workers', help="Set the number of workers for Google Cloud Dataflow to allocate (instead of autoallocation). Default value = 0 uses autoallocation.", default="0")
    pipelines = parser.add_mutually_exclusive_group(required=True)
    pipelines.add_argument('--bottles_sold', help="Count the total number of bottles sold for each wine that has been purchased at least once and order the final result from largest to smallest count.", action='store_true')
    pipelines.add_argument('--dollars_sold', help="Calculate the total dollar amount of sales for each wine and order the final result from largest to smallest amount.", action='store_true')
    pipelines.add_argument('--winery_bottles_sold', help="Count the total number of bottles sold for each winery that has had at least one bottle purchased and order the final result from largest to smallest count.", action='store_true')
    pipelines.add_argument('--winery_dollars_sold', help="Calculate the total dollar amount of sales for each winery and order the final result from largest to smallest amount.", action='store_true')
    pipelines.add_argument('--purchased_together', help="For each wine that was purchased at least once, find the other wine that was purchased most often at the same time and count how many times the two wines were purchased together.", action='store_true')
    parser.add_argument('--variety', help="Use the variety whose first letter is the closest to the first letter of your last name. NOTE: THE CHOICE \"Red Blend\" IS GIVEN FOR COMPARISON AGAINST THE GIVEN SOLUTION. DO NOT USE \"Red Blend\" FOR YOUR ASSIGNMENT SUBMISSION. To use \"Red Blend\" as a script argument place it in double quotes.", choices=["Chardonnay", "Malbec", "Pinot Noir", "Red Blend", "Riesling", "Sauvignon Blanc", "Zinfandel"])
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
        if args.num_workers != "0":
            # This disables the autoscaling if you have specified a number of workers
            pipeline_args.append("--num_workers")
            pipeline_args.append(args.num_workers)
            pipeline_args.append("--autoscaling_algorithm")
            pipeline_args.append("NONE")
    else:
        pipeline_args = []


    run(args, pipeline_args)
