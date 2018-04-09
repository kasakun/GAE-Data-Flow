import os
import subprocess
import argparse
import shutil



def run(args):
    pipelines = ["bottles_sold", "dollars_sold", "winery_bottles_sold", "winery_dollars_sold"]
    output_dir = args.bucket + "/script_output/"
    temp_output_dir = args.bucket + "/script_temp_output/"
    temp_dir = args.bucket + "/script_temp/"
    processes = []
    output_variety = args.variety.replace(" ", "_").lower()
    print("Running parts 1-4")
    for pipeline in pipelines:
        process = subprocess.Popen(["python", args.source_file_name,"--input", args.input, "--output", temp_output_dir + pipeline, "--runner", "DataflowRunner", "--project", args.project, "--temp_location", temp_dir, "--" + pipeline])
        processes.append(process)

    
    print("Running parts 5-8")
    for pipeline in pipelines:
        process = subprocess.Popen(["python", args.source_file_name,"--input", args.input, "--output", temp_output_dir + output_variety + "_" + pipeline, "--runner", "DataflowRunner", "--project", args.project, "--temp_location", temp_dir, "--" + pipeline, "--variety", args.variety])
        processes.append(process)

    
    

    for counter, process in enumerate(processes):
        process.wait()
        pipeline = pipelines[counter % 4]
        stdout = subprocess.check_output(["gsutil", "ls", temp_output_dir])
        files_dirs = stdout.split("\n")
        shards = []
        for shard in files_dirs:
            if counter >= 4:    #Variety runs
                if pipeline == "bottles_sold" or pipeline == "dollars_sold":
                    if (pipeline in shard) and not ("winery" in shard) and output_variety in shard:
                        shards.append(shard) 
                elif pipeline == "winery_dollars_sold" or pipeline == "winery_bottles_sold":
                    if pipeline in shard and output_variety in shard:
                        shards.append(shard)
            else:
                if pipeline == "bottles_sold" or pipeline == "dollars_sold":
                    if (pipeline in shard) and not ("winery" in shard) and not (output_variety in shard):
                        shards.append(shard) 
                elif pipeline == "winery_dollars_sold" or pipeline == "winery_bottles_sold":
                    if pipeline in shard and not (output_variety in shard):
                        shards.append(shard)
        output_file = subprocess.check_output(["gsutil", "cat"] + shards)
        if counter >= 4:
            output_file_name = output_variety + "_" + pipeline + ".csv"
        else:
            output_file_name = pipeline + ".csv"
        with open(output_file_name, 'w') as fh:
            fh.write(output_file)
        for shard in shards:
            subprocess.Popen(["gsutil", "mv", shard, output_dir])
        subprocess.Popen(["gsutil", "cp", output_file_name, output_dir])

    print("Running part 2")
    p_purchased_together = subprocess.Popen(["python", args.source_file_name,"--input", args.input, "--output", temp_output_dir + "purchased_together", "--runner", "DataflowRunner", "--project", args.project, "--temp_location", temp_dir, "--" + "purchased_together"])
    p_purchased_together.wait()
    stdout = subprocess.check_output(["gsutil", "ls", temp_output_dir])
    files_dirs = stdout.split("\n")
    shards = []
    for shard in files_dirs:
        if "purchased_together" in shard:
            shards.append(shard)
    output_file = subprocess.check_output(["gsutil", "cat"] + shards)
    output_file_name = "purchased_together" + ".csv"
    with open(output_file_name, 'w') as fh:
        fh.write(output_file)
    for shard in shards:
        subprocess.Popen(["gsutil", "mv", shard, output_dir])
    subprocess.Popen(["gsutil", "cp", output_file_name, output_dir])



if __name__ == "__main__":
    # This function will parse the required arguments for you.
    # Try template.py --help for more information
    # View https://docs.python.org/3/library/argparse.html for more information on how it works
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', help="Input file to process.", required=True)
    parser.add_argument('--project', help="Your Google Cloud Project ID.", required=True)
    parser.add_argument('--temp_location', help="Location where temporary files should be stored.", required=True)
    parser.add_argument('--num_workers', help="Set the number of workers for Google Cloud Dataflow to allocate (instead of autoallocation). Default value = 0 uses autoallocation.", default="0")
    parser.add_argument('--variety', help="Use the variety whose first letter is the closest to the first letter of your last name. NOTE: THE CHOICE \"Red Blend\" IS GIVEN FOR COMPARISON AGAINST THE GIVEN SOLUTION. DO NOT USE \"Red Blend\" FOR YOUR ASSIGNMENT SUBMISSION. To use \"Red Blend\" as a script argument place it in double quotes.", choices=["Chardonnay", "Malbec", "Pinot Noir", "Red Blend", "Riesling", "Sauvignon Blanc", "Zinfandel"], required=True)
    parser.add_argument('--bucket', help="The name of your bucket. Ex. gs://testbucket", required=True)
    parser.add_argument('--source_file_name', help="The name of your source file. Ex. template.py", required=True)
    args = parser.parse_args()

    run(args)