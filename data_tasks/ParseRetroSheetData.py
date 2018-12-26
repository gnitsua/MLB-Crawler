import luigi as luigi
import pandas as pd

from data_tasks.DownloadRetroSheetData import DownloadRetroSheetData


class ParseRetroSheetData(luigi.Task):
    date = luigi.DateParameter()

    def requires(self):
        return DownloadRetroSheetData(self.date)

    def output(self):
        templated_path = "{date:%Y}/GL{date:%Y}.csv"
        instantiated_path = templated_path.format(date=self.date)
        return luigi.LocalTarget(instantiated_path)

    def run(self):
        with self.input().open('r') as inputfile:
            dataframe = pd.read_csv(inputfile, header=None, usecols=[0, 3, 6, 78, 80, 82, 84, 86, 88])
            dataframe[0] = dataframe[0].apply(pd.to_datetime, errors="coerce", format="%Y%m%d")
            dataframe[3] = dataframe[3].str.lower()
            dataframe[6] = dataframe[6].str.lower()
            dataframe.rename(columns={
                0: "date",
                3: "away_team",
                6: "home_team",
                78: "home_plate_umpire",
                80: "1B_umpire",
                82: "2B_umpire",
                84: "3B_umpire",
                86: "LF_umpire",
                88: "RF_umpire",
            }, inplace=True)

            with self.output().open('w') as outputfile:
                dataframe.to_csv(outputfile)
