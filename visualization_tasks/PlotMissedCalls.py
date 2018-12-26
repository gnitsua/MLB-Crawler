import luigi as luigi
import pandas as pd

from Visualizer import Visualizer
from data_tasks.GetMissedCallsData import GetMissedCallsData

class PlotMissedCalls(luigi.Task):
    year = luigi.YearParameter()

    def requires(self):
        return GetMissedCallsData(self.year)

    def output(self):
        return luigi.LocalTarget('{year:%Y}/images/{year:%Y}_missed_calls.png'.format(year=self.year),
                                 format=luigi.format.Nop)

    def run(self):
        with self.input().open('r') as yearData:
            year_dataframe = pd.read_csv(yearData)
            missed_strikes = year_dataframe.where(year_dataframe["pitch_type"] == "S")
            missed_balls = year_dataframe.where(year_dataframe["pitch_type"] == "B")

        with self.output().open("w") as outputfile:
            v = Visualizer()
            v.show_pitches(missed_strikes, missed_balls, outputfile)
