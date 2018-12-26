import luigi as luigi
import pandas as pd

from data_tasks.GenerateYearData import GenerateYearData
from Visualizer import Visualizer


class PlotB3S0Histogram(luigi.Task):
    year = luigi.YearParameter()

    def requires(self):
        return GenerateYearData(self.year)

    def output(self):
        return luigi.LocalTarget(
            '{year:%Y}/images/B3S0_histogram.png'.format(year=self.year),
            format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as yearData:
            year_dataframe = pd.read_csv(yearData)
            strikes = year_dataframe.where(
                (year_dataframe["pitch_type"] == "S") & (year_dataframe['atbat_s'] == 0) & (year_dataframe['atbat_b'] == 3))
            balls = year_dataframe.where(
                (year_dataframe["pitch_type"] == "B") & (year_dataframe['atbat_s'] == 0) & (year_dataframe['atbat_b'] == 3))

        with self.output().open("w") as outputfile:
            v = Visualizer()
            v.show_probability(strikes, balls, outputfile)
