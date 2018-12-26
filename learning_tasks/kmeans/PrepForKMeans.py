import luigi as luigi
import pandas as pd

from data_tasks.DownloadGameData import DownloadGameData
from PitchStatParser import PitchStatParser
from data_tasks.GetMissedCallsData import GetMissedCallsData


class PrepForKMeans(luigi.Task):
    year = luigi.YearParameter()

    def requires(self):
        return GetMissedCallsData(self.year)

    def output(self):
        path = "{date:%Y}_missed_call_clusters.csv".format(date=self.date)
        return luigi.LocalTarget(path)

    def run(self):
        with self.input().open('r') as yearData:
            year_dataframe = pd.read_csv(yearData)


        reduced_dimension_data = year_dataframe[""]
            strikes = year_dataframe.where(
                (year_dataframe["pitch_type"] == "S"))
            balls = year_dataframe.where(
                (year_dataframe["pitch_type"] == "B"))

