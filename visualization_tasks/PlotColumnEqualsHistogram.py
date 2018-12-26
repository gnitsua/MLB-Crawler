import luigi as luigi
import pandas as pd

from Visualizer import Visualizer
from visualization_tasks.PlotColumnEqualsData import PlotColumnEqualsData


class PlotColumnEqualsHistogram(luigi.Task):
    year = luigi.YearParameter()
    column = luigi.Parameter()
    value = luigi.Parameter()

    def requires(self):
        return PlotColumnEqualsData(self.year, self.column, self.value)

    def output(self):
        return luigi.LocalTarget(
            '{year:%Y}/images/{column}/{value}.png'.format(year=self.year, column=self.column, value=self.value),
            format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as yearData:
            year_dataframe = pd.read_csv(yearData)
            strikes = year_dataframe.where(
                (year_dataframe["pitch_type"] == "S"))
            balls = year_dataframe.where(
                (year_dataframe["pitch_type"] == "B"))

        with self.output().open("w") as outputfile:
            v = Visualizer()
            v.show_probability(strikes, balls, outputfile, title="Probability Distrubution for %s equals %s"%(self.column,self.value))
