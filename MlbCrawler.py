import datetime
import time

import luigi
import argparse


from visualization_tasks.PlotAllCalls import PlotAllCalls
from visualization_tasks.PlotB2S1Histogram import PlotB2S1Histogram
from visualization_tasks.PlotB3S0Histogram import PlotB3S0Histogram
from visualization_tasks.PlotColumnEqualsDifferenceHistogram import PlotColumnEqualsDifferenceHistogram
from visualization_tasks.PlotColumnEqualsHistogram import PlotColumnEqualsHistogram
from visualization_tasks.PlotMissedCalls import PlotMissedCalls
from visualization_tasks.ProbabilityOfStrikes import ProbabilityOfStrikes
from data_tasks.GetSummaryStatistics import GetSummaryStatistics
from data_tasks.GroupBy import GroupBy

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("year", help="year to parse data for", type=int)
    args = parser.parse_args()

    year = datetime.date(args.year, 1, 1)  # TODO: make this a date range
    start = time.time()
    luigi.build([
        ProbabilityOfStrikes(year),
        GetSummaryStatistics(year),
        GroupBy(year,"inning_home_team"),

    ], local_scheduler=True)
    print("Time elapsed: " + str(time.time() - start))
