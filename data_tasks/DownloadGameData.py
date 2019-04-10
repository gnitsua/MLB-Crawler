import time
# random forest with information gain
# rerusive feature elimination
# should use training/validation/testing datasets
# svm rbm kernel
# descriminit classifier
import luigi as luigi

from data_tasks import FileDownloader

class DownloadGameData(luigi.Task):
    date = luigi.DateParameter()
    home_team = luigi.Parameter()
    away_team = luigi.Parameter()
    game_number = luigi.Parameter()  # only used for double headers

    #
    # def requires(self):
    #     return ParseSchedules(self.date.year)

    def output(self):
        templated_path = "{date:%Y}/raw_data/gid_{date:%Y_%m_%d}_{home_team}mlb_{away_team}mlb_{game_number}.xml"
        instantiated_path = templated_path.format(date=self.date, home_team=self.home_team, away_team=self.away_team,
                                                  game_number=self.game_number)
        return luigi.LocalTarget(instantiated_path)

    def run(self):
        url = self.build_url()
        with self.output().open('w') as file:
            # TODO: rate limit
            time.sleep(0.25)
            datafile = FileDownloader.download_file(url)
            file.write(datafile.read().decode("utf-8"))

    def build_url(self):
        templated_path = "http://gd2.mlb.com/components/game/mlb/{date:year_%Y/month_%m/day_%d}/" \
                         "gid_{date:%Y_%m_%d}_{home_team}mlb_{away_team}mlb_{game_number}/inning/inning_all.xml"
        return templated_path.format(date=self.date, home_team=self.home_team, away_team=self.away_team,
                                     game_number=self.game_number)
