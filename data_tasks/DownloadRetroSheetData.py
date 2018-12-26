import zipfile

import luigi as luigi

from data_tasks import FileDownloader


class DownloadRetroSheetData(luigi.Task):
    date = luigi.DateParameter()

    def output(self):
        templated_path = "{date:%Y}/GL{date:%Y}.txt"
        instantiated_path = templated_path.format(date=self.date)
        return luigi.LocalTarget(instantiated_path)

    def run(self):
        schedule_url = "https://www.retrosheet.org/gamelogs/gl{date:%Y}.zip".format(date=self.date)
        try:
            with zipfile.ZipFile(FileDownloader.download_file(schedule_url)) as thezip:
                for zipinfo in thezip.infolist():
                    with thezip.open(zipinfo) as thefile:
                        if (zipinfo.filename == "GL{year:%Y}.TXT".format(year=self.date)):
                            outputfile = self.output().open('w')
                            outputfile.write(thefile.read().decode("utf-8"))
                            outputfile.close()
        except ConnectionError as e:
            raise e
