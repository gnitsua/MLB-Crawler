import luigi as luigi
import pandas as pd

from data_tasks.GenerateYearData import GenerateYearData


class GetMissedCallsData(luigi.Task):
    year = luigi.YearParameter()

    def requires(self):
        return GenerateYearData(self.year)

    def output(self):
        return luigi.LocalTarget('{year:%Y}/images/{year:%Y}_missed_calls.csv'.format(year=self.year))

    def run(self):
        with self.input().open('r') as yearData:
            year_dataframe = pd.read_csv(yearData)
            missed_calls = year_dataframe[
                ((year_dataframe["in_strike_zone"] == False) & (year_dataframe["pitch_type"] == "S"))|
                ((year_dataframe["in_strike_zone"] == True) & (year_dataframe["pitch_type"] == "B"))]

        with self.output().open("w") as outputfile:
            missed_calls.to_csv(outputfile)
