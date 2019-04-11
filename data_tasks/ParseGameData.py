import luigi as luigi
from luigi_soft_failures import softly_failing

from data_tasks.DownloadGameData import DownloadGameData
from PitchStatParser import PitchStatParser


@softly_failing(catch_all=True, propagate=True)
class ParseGameData(luigi.Task):
    date = luigi.DateParameter()
    home_team = luigi.Parameter()
    away_team = luigi.Parameter()
    game_number = luigi.Parameter()  # only used for double headers
    parser = PitchStatParser()

    def requires(self):
        return DownloadGameData(self.date, self.home_team, self.away_team, self.game_number)

    def output(self):
        templated_path = "{date:%Y}/{parser_version}/gid_{date:%Y_%m_%d}_{home_team}mlb_{away_team}mlb_{game_number}.csv"
        instantiated_path = templated_path.format(date=self.date, home_team=self.home_team, away_team=self.away_team,
                                                  game_number=self.game_number,parser_version=self.parser.__version__)
        return luigi.LocalTarget(instantiated_path)

    def run(self):
        with self.input().open('r') as inputfile:
            dataframe = self.parser.parse(inputfile)
            with self.output().open('w') as outputfile:
                dataframe.to_csv(outputfile)
