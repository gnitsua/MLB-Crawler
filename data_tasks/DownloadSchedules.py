import zipfile

import luigi as luigi

from data_tasks import FileDownloader


class DownloadSchedules(luigi.Task):
    year = luigi.YearParameter()

    #
    # def requires(self):
    #     return GenerateWords()

    def output(self):
        return luigi.LocalTarget('{year:%Y}/{year:%Y}SKED.TXT'.format(year=self.year))

    def run(self):
        schedule_url = "https://www.retrosheet.org/schedule/{year:%Y}SKED.ZIP".format(year=self.year)
        try:
            with zipfile.ZipFile(FileDownloader.download_file(schedule_url)) as thezip:
                for zipinfo in thezip.infolist():
                    with thezip.open(zipinfo) as thefile:
                        if (zipinfo.filename == "{year:%Y}SKED.TXT".format(year=self.year)):
                            outputfile = self.output().open('w')
                            outputfile.write(thefile.read().decode("utf-8"))
                            outputfile.close()
        except ConnectionError as e:
            raise e
