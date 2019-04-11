import luigi as luigi
import pandas as pd

from data_tasks.ParseGameData import ParseGameData
from data_tasks.ParseSchedules import ParseSchedules


class GenerateYearData(luigi.Task):
    year = luigi.YearParameter()

    def requires(self):
        return ParseSchedules(self.year)

    def output(self):
        return luigi.LocalTarget('{year:%Y}/{year:%Y}.csv'.format(year=self.year))

    def run(self):
        with self.input().open('r') as inputfile:
            schedule = pd.read_csv(inputfile, parse_dates=["date"])

            parsed_games_data_files = []
            for index, game in schedule.iterrows():
                parsed_game_data_task = ParseGameData(game["date"], game["home_team"].lower(),
                                                            game["away_team"].lower(), 1)
                parsed_games_data_files.append(parsed_game_data_task)
            yield parsed_games_data_files #execute the array of tasks

            #check to make sure the tasks completed
            completed_tasks = []
            for result in parsed_games_data_files:
                try:
                    result.output().open('r')  # just check to see if it exists
                    completed_tasks.append(result)
                except FileNotFoundError:
                    pass

            year_dataframe = pd.concat([pd.read_csv(f.output().open('r')) for f in completed_tasks], sort=False)
        with self.output().open("w") as outputfile:
            year_dataframe.to_csv(outputfile)

    # def game_date_parser(self, string):
    #     return dateparser.parse(string, date_formats=['%Y%m%d'])
