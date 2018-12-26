import luigi as luigi
import pandas as pd

from data_tasks.MergeRetroSheetData import MergeRetroSheetData


class PlotColumnEqualsData(luigi.Task):
    year = luigi.YearParameter()
    column = luigi.Parameter()
    value = luigi.Parameter()

    def requires(self):
        return MergeRetroSheetData(self.year)

    def output(self):
        return luigi.LocalTarget('{year:%Y}/images/{column}/{value}.csv'.format(year=self.year, column=self.column, value=self.value))
            

    def run(self):
        with self.input().open('r') as yearData:
            year_dataframe = pd.read_csv(yearData)
            year_dataframe = year_dataframe[(year_dataframe[self.column] == self.value)]

        with self.output().open("w") as outputfile:
            year_dataframe.to_csv(outputfile)