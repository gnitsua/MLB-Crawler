import luigi as luigi
import pandas as pd

from data_tasks.ParseGameData import ParseGameData
from data_tasks.ParseRetroSheetData import ParseRetroSheetData
from data_tasks.ParseSchedules import ParseSchedules


class GenerateYearData(luigi.Task):
    year = luigi.YearParameter()

    def requires(self):
        return ParseSchedules(self.year)

    def output(self):
        return luigi.LocalTarget('{year:%Y}/{year:%Y}.csv'.format(year=self.year))

    def run(self):
        year_dataframe = pd.DataFrame()
        with self.input().open('r') as inputfile:
            schedule = pd.read_csv(inputfile, parse_dates=["date"])

            parsed_game_data = []
            counter = 0
            for index, game in schedule.iterrows():
                parsed_game_data.append(ParseGameData(game["date"], game["home_team"].lower(),
                                                      game["away_team"].lower(),
                                                      1))  # TODO: double headers
            yield parsed_game_data

            year_dataframe = pd.concat([pd.read_csv(f.output().open('r')) for f in parsed_game_data], sort=False)
        with self.output().open("w") as outputfile:
            year_dataframe.to_csv(outputfile)

    # def game_date_parser(self, string):
    #     return dateparser.parse(string, date_formats=['%Y%m%d'])
