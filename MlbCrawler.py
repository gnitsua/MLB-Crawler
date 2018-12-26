import datetime
import time

import luigi

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
    year = datetime.date(2015, 1, 1)  # TODO: make this a date range
    start = time.time()
    luigi.build([
        PlotAllCalls(year),
        PlotMissedCalls(year),
        ProbabilityOfStrikes(year),
        PlotColumnEqualsHistogram(year, "atbat_s", 0),
        PlotColumnEqualsHistogram(year, "atbat_s", 1),
        PlotColumnEqualsDifferenceHistogram(year, "atbat_s", 1),
        PlotColumnEqualsHistogram(year, "atbat_s", 2),
        PlotColumnEqualsHistogram(year, "atbat_s", 3),
        PlotColumnEqualsHistogram(year, "atbat_stand", "L"),
        PlotColumnEqualsHistogram(year, "atbat_stand", "R"),
        PlotColumnEqualsDifferenceHistogram(year, "atbat_stand", "L"),
        PlotColumnEqualsHistogram(year, "inning_num", 1),
        PlotColumnEqualsHistogram(year, "inning_num", 9),
        PlotColumnEqualsDifferenceHistogram(year, 'home_plate_umpire', "Angel Hernandez"),
        PlotColumnEqualsDifferenceHistogram(year, 'home_plate_umpire', "Joe West"),
        PlotColumnEqualsHistogram(year, 'home_plate_umpire', "Joe West"),
        PlotB2S1Histogram(year),
        PlotB3S0Histogram(year),
        GetSummaryStatistics(year),
        GroupBy(year,"home_plate_umpire"),
        GroupBy(year,"inning_home_team"),

    ], local_scheduler=True)
    print("Time elapsed: " + str(time.time() - start))
