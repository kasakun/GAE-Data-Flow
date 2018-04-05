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
# Edited by Zeyu Chen, variety = Chardonnay
def run(args, pipeline_args):

    with beam.Pipeline(options=PipelineOptions(pipeline_args)) as p:

        lines = p | ReadFromText(args.input)
        ########################################################################
        # Part1 Basic Data Mining
        ########################################################################
        import re
        ##ignore the , in quote
        rx = re.compile(r',(?![^"]*"(?:(?:[^"]*"){2})*[^"]*$)')

        # def VarietyFilter(line):
        #     temp = rx.split(line)
        #
        #     if temp[9].lower() == "chardonnay" :
        def SplitLine(line):
            res = []
            temp = rx.split(line)
            res.append(temp)
            return res

        def GetIndex(line):
            print line
            index = rx.split(line)
            return index[0:1]

        def vGetIndex(lineSplit):
            return lineSplit[0:1]

        def GetWinery(line):
            winery = rx.split(line)
            return winery[10:11]

        def vGetWinery(lineSplit):
            return lineSplit[10:11]

        def GetIndexPrice(line):
            ## The structure to ouput:
            # [[index, price]]
            result = []
            cap = []

            lineSplit = rx.split(line)

            index = lineSplit[0]
            price = lineSplit[5]

            result.append(index)
            result.append(price)

            cap.append(result)
            return cap

        def vGetIndexPrice(lineSplit):
            ## The structure to ouput:
            # [[index, price]]
            result = []
            cap = []

            index = lineSplit[0]
            price = lineSplit[5]

            result.append(index)
            result.append(price)

            cap.append(result)
            return cap

        def GetWineryPrice(line):
            ## The structure to ouput:
            # [[index, price]]
            result = []
            cap = []

            lineSplit = rx.split(line)

            winery = lineSplit[10]
            price = lineSplit[5]

            result.append(winery)
            result.append(price)

            cap.append(result)
            return cap

        def vGetWineryPrice(lineSplit):
            ## The structure to ouput:
            # [[index, price]]
            result = []
            cap = []

            winery = lineSplit[10]
            price = lineSplit[5]

            result.append(winery)
            result.append(price)

            cap.append(result)
            return cap

        def PairWithOne(index):
            return (index, 1)

        def FormatResult(res):
            (index, result) = res
            return '%s\t%s' % (index, result)

        def PairWithPrice(result):
            return (result[0], int(result[1]))



        # ## output index count
        # output1 = (
        #     lines
        #     | 'GetIndexofWine' >> beam.FlatMap(GetIndex)
        #     | 'IndexPairWithOne' >> beam.Map(PairWithOne)
        #     | 'IndexGroupAndSum' >> beam.CombinePerKey(sum)
        #     | 'IndexFormat' >> beam.Map(FormatResult)
        # )
        #
        # output1 | 'IndexCount' >> WriteToText(args.output + "index", 'csv', num_shards=1)
        #
        # ## output index price count
        # output2 = (
        #     lines
        #     | 'GetIndexPriceofWine' >> beam.FlatMap(GetIndexPrice)
        #     | 'IndexPairWithPrice' >> beam.Map(PairWithPrice)
        #     | 'IndexGroupAndSumPrice' >> beam.CombinePerKey(sum)
        #     | 'IndexPriceFormat' >> beam.Map(FormatResult)
        # )
        # output2 | 'IndexPrice' >> WriteToText(args.output + "indexprice", 'csv', num_shards=1)
        #
        # ## output index price count
        # output3 = (
        #     lines
        #     | 'GetWineryofWine' >> beam.FlatMap(GetWinery)
        #     | 'WineryPairWithOne' >> beam.Map(PairWithOne)
        #     | 'WineryGroupAndSum' >> beam.CombinePerKey(sum)
        #     | 'WineryFormat' >> beam.Map(FormatResult)
        # )
        # output3 | 'WineryCount' >> WriteToText(args.output + "winery", 'csv', num_shards=1)
        #
        # ## output index price count
        # output4 = (
        #     lines
        #     | 'GetWineryPriceofWine' >> beam.FlatMap(GetWineryPrice)
        #     | 'WineryPairWithPrice' >> beam.Map(PairWithPrice)
        #     | 'WineryGroupAndSumPrice' >> beam.CombinePerKey(sum)
        #     | 'WineryPriceFormat' >> beam.Map(FormatResult)
        # )
        # output4 | 'WineryPrice' >> WriteToText(args.output + "wineryprice", 'csv', num_shards=1)
        #
        # ## output index count
        # output5 = (
        #     lines
        #     | 'vIndexSplit' >> beam.FlatMap(SplitLine)
        #     | 'vIndexFilter' >> beam.Filter (lambda element: element[9].lower() == "chardonnay")
        #     | 'vGetIndexofWine' >> beam.FlatMap(vGetIndex)
        #     | 'vIndexPairWithOne' >> beam.Map(PairWithOne)
        #     | 'vIndexGroupAndSum' >> beam.CombinePerKey(sum)
        #     | 'vIndexFormat' >> beam.Map(FormatResult)
        # )
        #
        # output5 | 'vIndexCount' >> WriteToText(args.output + "vindex", 'csv', num_shards=1)
        #
        # output6 = (
        #     lines
        #     | 'vIndexPriceSplit' >> beam.FlatMap(SplitLine)
        #     | 'vIndexPriceFilter' >> beam.Filter (lambda element: element[9].lower() == "chardonnay")
        #     | 'vGetIndexPriceofWine' >> beam.FlatMap(vGetIndexPrice)
        #     | 'vIndexPairWithPrice' >> beam.Map(PairWithPrice)
        #     | 'vIndexGroupAndSumPrice' >> beam.CombinePerKey(sum)
        #     | 'vIndexPriceFormat' >> beam.Map(FormatResult)
        # )
        # output6 | 'vIndexPrice' >> WriteToText(args.output + "vindexprice", 'csv', num_shards=1)
        #
        #
        # ## output index price count
        # output7 = (
        #     lines
        #     | 'vWinerySplit' >> beam.FlatMap(SplitLine)
        #     | 'vWineryFilter' >> beam.Filter (lambda element: element[9].lower() == "chardonnay")
        #     | 'vGetWineryofWine' >> beam.FlatMap(vGetWinery)
        #     | 'vWineryPairWithOne' >> beam.Map(PairWithOne)
        #     | 'vWineryGroupAndSum' >> beam.CombinePerKey(sum)
        #     | 'vWineryFormat' >> beam.Map(FormatResult)
        # )
        # output7 | 'vWineryCount' >> WriteToText(args.output + "vwinery", 'csv', num_shards=1)
        #
        ## output index price count
        # output8 = (
        #     lines
        #     | 'vWineryPriceSplit' >> beam.FlatMap(SplitLine)
        #     | 'vWineryPriceFilter' >> beam.Filter (lambda element: element[9].lower() == "chardonnay")
        #     | 'vGetWineryPriceofWine' >> beam.FlatMap(vGetWineryPrice)
        #     | 'vWineryPairWithPrice' >> beam.Map(PairWithPrice)
        #     | 'vWineryGroupAndSumPrice' >> beam.CombinePerKey(sum)
        #     | 'vWineryPriceFormat' >> beam.Map(FormatResult)
        # )
        # output8 | 'vWineryPrice' >> WriteToText(args.output + "vwineryprice", 'csv', num_shards=1)

        ########################################################################
        # Part2 Most Bought Wine
        ########################################################################
        def GetIndexDate(line):
            # Input in string
            result = []
            key = []
            cap = []

            lineSplit = rx.split(line)

            index = lineSplit[0]
            date = lineSplit[13]
            user = lineSplit[12]

            # construct a combined key
            key.append(user)
            key.append(date)

            # construct a
            result.append(key)
            result.append(index)

            # put data into a capsule
            cap.append(result)
            ## The structure to ouput:
            # print cap
            # [[user, date], index]
            return cap

        def RePair(incap):
            # Input from GroupByKey
            # ([user, date], index)
            newPairList = []
            ## This is to fix the bug of Dataflow....
            SameTimeUserPurchase = list(incap[1])

            count = len(SameTimeUserPurchase)

            if count == 1:
                newPair = []
                newPair.append(SameTimeUserPurchase[0])
                newPair.append(None)
                newPairList.append(newPair)
            else:
                for x in SameTimeUserPurchase:
                    for y in SameTimeUserPurchase:
                        if (y == x):
                            continue
                        else:
                            newPair = []
                            newPair.append(x)
                            newPair.append(y)
                            # print newPair
                        newPairList.append(newPair)

            # output one time purchase pair
            # format [[this wine, other wine], [this wine, other wine], ....]
            # example 1, w1, w2, w3 is purchased once purchased together
            # The format is [[w1, w2], [w1, w3], [w2, w1], [w2, w3], [w3, w1], [w3, w2]]
            # example 2, only w1 is purchased
            # The format is [w1, None]
            # print newPairList
            return newPairList
        # def DePairWithOne(inlist):
        #     for x in inlist:
        #         (x, 1) = 1
        def newPairWithOne(cap):
            # print (cap, [1])
            return (cap, [1])
        def Acheive(incap):
            # Input by GroupByKey, initial merge by user-date
            # example 1 ([w1, w2], [1, 1, ..., 1, 1])
            #              pair     number of pairs
            # example 2 ([[w1, None], [1, 1, ..., 1, 1])
            #             no pair          number
            # print incap[0]
            cap = []
            newPair = []
            res = []

            pair = list(incap[0])
            num = list(incap[1])

            # [otherwine, num]
            newPair.append(pair[1])
            newPair.append(len(num))

            res.append(pair[0])
            res.append(newPair)

            cap.append(res)
            # output
            # print cap
            # [[thiswine, [otherwine1, 1]]]
            return cap

        def sortandmax(incap)
            # Input from GroupByKey
            # After Grouping, new format represents all other wines ans num pair with thiswine
            # [thiswine, [[otherwine1, num], [otherwine2, num], [otherwine3, num], ...]]
            cap = []
            List = [] # final list
            temp = [] # other wine index list
            maxList = [] # pairs with same max number in list
            ## This is to fix the bug of dataflow
            thiswine = incap[0]
            others = list(incap[1])

            List.append(int(thiswine))
            if len(others) > 1:
                for x in others:
                    if x[0] == None:
                        others.remove(x)
            sortedList =  sorted(others, key = lambda ele: ele[1], reverse = True)
            maxvalue = sortedList[0][1]

            if len(sortedList) > 1:
                for x in sortedList:
                    if x[1] == maxvalue:
                        maxList.append(x)
            # print maxList
            if len(sortedList) == 1 and sortedList[0][0] == None:
                sortedList[0][1] = 0

            for x in maxList:
                temp.append(int(x[0]))

            # ascending indexes after wine index 1
            temp = sorted(temp, key = lambda ele: ele)

            # merge thiswine+otherwines
            List = List + temp
            # merge thiswine+otherwines+maxvalue
            List.append(maxvalue)

            cap.append(List)
            return cap

        def mostFormat(cap):
            result = "\t".join(str(x) for x in cap)
            return result

        output = (
            lines
            | 'GetIndexDateofWine' >> beam.FlatMap(GetIndexDate)
            | 'MergeUserDate' >> beam.GroupByKey()
            | 'test' >> beam.FlatMap(RePair)
            # | 'Mapping' >> beam.Map(MMap)
            | 'pairwithone' >> beam.Map(newPairWithOne)
            | 'Sum' >> beam.GroupByKey()
            | 'achieve' >> beam.ParDo(Acheive)
            | 'Groupagain' >> beam.GroupByKey()
            | 'SortandMax' >> beam.ParDo(sortandmax)
            | 'FormatMostPair' >> beam.Map(mostFormat)
        )
        output | 'MostPair' >> WriteToText(args.output + "most", 'csv', num_shards=1)

        # lines = p | ReadFromText(args.input)
        #
        # def SplitIntoWords(line):
        #     import re
        #     index = re.split(r',', line)
        #     index = index[0:1, 5:6]
        #     # print index[0:1]
        #     #print index[0]
        #     return index
        #     # return re.split(r',', line)
        #     #return re.findall(r'[A-Za-z\']+', line)
        #
        # def GetElement(line):
        #     return line
        #
        # def PairWithOne(word):
        #     print word
        #     return (word, 1)
        #
        # def FormatResult(word_count):
        #     (word, count) = word_count
        #     return '%s\t%s' % (word, count)
        #
        # output = (
        #     lines
        #     | 'Split' >> beam.FlatMap(SplitIntoWords)
        #     | 'PairWithOne' >> beam.Map(PairWithOne)
        #     | 'GroupAndSum' >> beam.CombinePerKey(sum)
        #     | 'Format' >> beam.Map(FormatResult)
        # )
        # output | WriteToText(args.output, 'csv', num_shards=1)




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
