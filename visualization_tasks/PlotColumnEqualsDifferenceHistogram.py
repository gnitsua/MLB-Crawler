import luigi as luigi
import pandas as pd
import numpy as np

from visualization_tasks.PlotColumnEqualsData import PlotColumnEqualsData
from Visualizer import Visualizer
from data_tasks.MergeRetroSheetData import MergeRetroSheetData

class PlotColumnEqualsDifferenceHistogram(luigi.Task):
    year = luigi.YearParameter()
    column = luigi.Parameter()
    value = luigi.Parameter()

    def requires(self):
        return [PlotColumnEqualsData(self.year, self.column, self.value), MergeRetroSheetData(self.year)]

    def output(self):
        return luigi.LocalTarget(
            '{year:%Y}/images/{column}/{value}_difference.png'.format(year=self.year, column=self.column,
                                                                      value=self.value),
            format=luigi.format.Nop)

    def run(self):
        with self.input()[0].open('r') as yearDataForValue:
            year_dataframe1 = pd.read_csv(yearDataForValue)
            value1_strikes = year_dataframe1.where(
                (year_dataframe1["pitch_type"] == "S"))
            value1_balls = year_dataframe1.where(
                (year_dataframe1["pitch_type"] == "B"))
        with self.input()[1].open('r') as yearDataForValue:
            year_dataframe2 = pd.read_csv(yearDataForValue)
            strikes = year_dataframe2.where(
                (year_dataframe2["pitch_type"] == "S"))
            balls = year_dataframe2.where(
                (year_dataframe2["pitch_type"] == "B"))

        with self.output().open("w") as outputfile:
            v = Visualizer()
            #_,_,_,distance = v.get_differential_2dhist(value1_strikes, value1_balls, strikes, balls)
            v.show_differential_probability(value1_strikes, value1_balls, strikes, balls, outputfile,
                                            title="%s: Increase in probability for %s"%(self.column,self.value))
