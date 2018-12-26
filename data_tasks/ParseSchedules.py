import dateparser
import luigi as luigi
import pandas as pd

from data_tasks.DownloadSchedules import DownloadSchedules


class ParseSchedules(luigi.Task):
    year = luigi.YearParameter()

    def requires(self):
        return DownloadSchedules(self.year)

    def output(self):
        return luigi.LocalTarget('{year:%Y}/{year:%Y}SKED.csv'.format(year=self.year))

    def run(self):
        try:
            parsed = pd.read_csv(self.input().open('r'),
                                 names=["date", "unknown", "DOW", "home_team", "home_league", "unknown2",
                                        "away_team", "away_league", "unknown3", "day/night", "comments",
                                        "rain_date"], parse_dates=[0], date_parser=self.game_date_parser, skipfooter=1)
            parsed = parsed[parsed["comments"].isnull()]  # filter out games that didn't happen
            with self.output().open('w') as outputfile:
                parsed.to_csv(outputfile)
        except Exception as e:
            raise e

    def game_date_parser(self, string):
        return dateparser.parse(string, date_formats=['%Y%m%d'])
