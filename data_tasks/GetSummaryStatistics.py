import luigi as luigi
import pandas as pd

from data_tasks.MergeRetroSheetData import MergeRetroSheetData


class GetSummaryStatistics(luigi.Task):
    year = luigi.YearParameter()

    def requires(self):
        return MergeRetroSheetData(self.year)

    def output(self):
        return luigi.LocalTarget('{year:%Y}/{year:%Y}_summary.txt'.format(year=self.year))
            

    def run(self):
        with self.input().open('r') as yearData:
            year_dataframe = pd.read_csv(yearData)

            num_pitches = len(year_dataframe)
            num_strikes = len(year_dataframe[year_dataframe["pitch_type"] == "S"])
            num_balls = len(year_dataframe[year_dataframe["pitch_type"] == "B"])
            num_missed_strikes = len(year_dataframe[((year_dataframe["in_strike_zone"] == False) & (year_dataframe["pitch_type"] == "S"))])
            num_missed_balls = len(year_dataframe[((year_dataframe["in_strike_zone"] == True) & (year_dataframe["pitch_type"] == "B"))])
            num_balls_with_same_position = year_dataframe[["pitch_px","pitch_pz"]].drop_duplicates().shape[0]

        with self.output().open("w") as outputfile:
            outputfile.write("Summary Statistics for the {year:%Y} season\n".format(year=self.year))
            outputfile.write("Number of pitches: %i\n"%(num_pitches))
            outputfile.write("Number of strikes: %i\n"%(num_strikes))
            outputfile.write("Number of balls: %i\n"%(num_balls))
            outputfile.write("Number of balls that were called strikes: %i (%f)\n"%(num_missed_strikes,(num_missed_strikes/num_strikes)*100))
            outputfile.write("Number of strikes that were called balls: %i (%f)\n"%(num_missed_balls,(num_missed_balls/num_balls)*100))
            outputfile.write("Probability of two balls having the same position: %f\n"%((1-num_balls_with_same_position/num_pitches)*100))