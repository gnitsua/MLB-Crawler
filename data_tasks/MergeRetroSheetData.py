import luigi as luigi
import pandas as pd

from data_tasks.GenerateYearData import GenerateYearData
from data_tasks.ParseRetroSheetData import ParseRetroSheetData


class MergeRetroSheetData(luigi.Task):
    year = luigi.YearParameter()

    def requires(self):
        return [GenerateYearData(self.year), ParseRetroSheetData(self.year)]

    def output(self):
        return luigi.LocalTarget('{year:%Y}/{year:%Y}_with_umpires.csv'.format(year=self.year))

    def run(self):
        with self.input()[0].open('r') as yearData, self.input()[1].open('r') as retrosheetInput:
            retrosheet_df = pd.read_csv(retrosheetInput,dtype={
                "home_plate_umpire":str,
                "1B_umpire": str,
                "2B_umpire": str,
                "3B_umpire": str,
                "LF_umpire": str,
                "RF_umpire": str,
            })
            year_dataframe = pd.read_csv(yearData)
            year_dataframe = pd.merge(year_dataframe, retrosheet_df, how="left",
                                      left_on=['date', 'inning_home_team', 'inning_away_team'],
                                      right_on=['date', "home_team", "away_team"])
            year_dataframe.drop(columns=["Unnamed: 0_y", "away_team", "home_team"], inplace=True)
            year_dataframe[["home_plate_umpire", "1B_umpire", "2B_umpire", "3B_umpire", "LF_umpire", "RF_umpire"]] = \
                year_dataframe[["home_plate_umpire","1B_umpire","2B_umpire","3B_umpire","LF_umpire","RF_umpire"]].fillna(value="(none)")

        with self.output().open("w") as outputfile:
            year_dataframe.to_csv(outputfile)
