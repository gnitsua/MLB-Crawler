import luigi as luigi
import pandas as pd
import numpy as np

from visualization_tasks.PlotColumnEqualsData import PlotColumnEqualsData
from visualization_tasks.Visualizer import Visualizer
from data_tasks.MergeRetroSheetData import MergeRetroSheetData

class GroupBy(luigi.Task):
    year = luigi.YearParameter()
    column = luigi.Parameter()

    def requires(self):
        return  MergeRetroSheetData(self.year)

    def output(self):
        return luigi.LocalTarget(
            '{year:%Y}/images/{column}/groupby.csv'.format(year=self.year, column=self.column))

    def run(self):
        with self.input().open('r') as yearData:
            year_dataframe = pd.read_csv(yearData)

        year_strikes = year_dataframe.where((year_dataframe["pitch_type"] == "S"))
        year_balls = year_dataframe.where((year_dataframe["pitch_type"] == "B")
                                          )
        value_groups = year_dataframe.groupby(self.column)
        v = Visualizer()
        output_dataframe = pd.DataFrame(columns=["value","count","magnitude"])
        total_length = len(year_dataframe)
        for value, group in value_groups:
            value_strikes = group.where(
                (group["pitch_type"] == "S"))
            value_balls = group.where(
                (group["pitch_type"] == "B"))

            _, _, _, magnitude = v.get_differential_2dhist(value_strikes, value_balls, year_strikes, year_balls)

            output_dataframe = output_dataframe.append({"value":value, "count":len(group) / total_length, "magnitude":magnitude},sort=False, ignore_index=True)

        with self.output().open("w") as outputfile:
            output_dataframe.to_csv(outputfile)
