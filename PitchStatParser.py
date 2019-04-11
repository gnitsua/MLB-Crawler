###################################
# Parsing baseball stats XML file
###################################
import hashlib
import inspect
import traceback
import warnings
import xml.etree.ElementTree as ET

import numpy as np
import pandas as pd
from versioned_classes import versioned_class


# todo: this doesn't need to be a class
@versioned_class
class PitchStatParser:
    def __init__(self):
        pass

    def parse(self, fileName):
        tree = ET.parse(fileName)
        game = tree.getroot()
        pitchStats = []
        game.attrib = self.changeKeys(game.attrib, game.tag)
        for inning in game:
            inning.attrib = self.changeKeys(inning.attrib, inning.tag)
            for half in inning:
                half.attrib = self.changeKeys(half.attrib, half.tag)
                for action_atbat in half:
                    if action_atbat.tag == "atbat":
                        action_atbat.attrib = self.changeKeys(action_atbat.attrib, action_atbat.tag)
                        for pitch_runner in action_atbat:
                            if pitch_runner.tag == "pitch":
                                pitch_runner.attrib = self.changeKeys(pitch_runner.attrib, pitch_runner.tag)
                                pitch_dict = {}
                                pitch_dict.update(pitch_runner.attrib)
                                pitch_dict.update(action_atbat.attrib)
                                pitch_dict.update(half.attrib)
                                pitch_dict.update(inning.attrib)
                                pitch_dict.update(game.attrib)
                                pitchStats.append(pitch_dict)
        self.pitchStats = pitchStats
        self.length = len(pitchStats)
        self.fileName = fileName
        self.pitchDataFrame = pd.DataFrame(pitchStats)
        try:
            self.clean_data()
        except Exception as e:
            warnings.warn("Invalid input data, skipping file (%s)"%e, Warning)
            traceback.print_exc()
            self.pitchDataFrame = pd.DataFrame()
            column_list = "atbat_away_team_runs","atbat_b","atbat_b_height","atbat_batter","atbat_end_tfs_zulu","atbat_home_team_runs","atbat_num","atbat_o","atbat_p_throws","atbat_pitcher","atbat_s","atbat_score","atbat_stand","atbat_start_tfs_zulu","game_atBat","inning_away_team","inning_home_team","inning_num","pitch_ax","pitch_ay","pitch_az","pitch_break_angle","pitch_break_length","pitch_break_y","pitch_cc","pitch_code","pitch_end_speed","pitch_nasty","pitch_on_1b","pitch_on_2b","pitch_on_3b","pitch_pfx_x","pitch_pfx_z","pitch_pitch_type","pitch_px","pitch_pz","pitch_spin_dir","pitch_spin_rate","pitch_start_speed","pitch_sz_bot","pitch_sz_top","pitch_tfs","pitch_tfs_zulu","pitch_type","pitch_type_confidence","pitch_vx0","pitch_vy0","pitch_vz0","pitch_x","pitch_x0","pitch_y","pitch_y0","pitch_z0","pitch_zone","date","since_game_start","since_at_bat_start","normalized_pitch_pz","normalized_pitch_px","in_strike_zone"
            for column in column_list:
                self.pitchDataFrame[column] = ""
        return self.pitchDataFrame

    def clean_data(self):
        # self.removeColumns()
        self.stringToFloats()
        self.stringToDates(["atbat_end_tfs_zulu", "atbat_start_tfs_zulu", "pitch_tfs_zulu"])
        self.stringToHeight()
        self.addGameDateColumn()
        self.addPitchTimeSinceGameStartColumn()
        self.addPitchTimeSinceAtBatStartColumn()
        self.notNanToBoolean(["atbat_score", "pitch_on_1b", "pitch_on_2b", "pitch_on_3b"])
        # self.atbatScoreRemoveNan()
        self.removeDataWithoutPxPz()
        self.removeDataWithoutSxSz()
        self.addNormalizedPitchZColumn()
        self.addNormalizedPitchXColumn()
        # self.removeHits()
        # self.onlyCalledStrikes()
        self.addCorrectCallColumn()

    def removeColumns(self):
        self.pitchDataFrame.drop(errors="ignore", axis="columns",
            columns=["atbat_des", "atbat_des_es", "atbat_event", "atbat_event_es", "atbat_event_num", "atbat_play_guid",
                     "atbat_start_tfs", "game_deck", "game_hole", "game_ind", "inning_next", "pitch_des",
                     "pitch_des_es","pitch_event_num", "pitch_id", "pitch_mt", "pitch_play_guid", "pitch_sv_id"], inplace=True)

    def stringToFloats(self):
        self.pitchDataFrame = self.pitchDataFrame.apply(pd.to_numeric, errors="ignore")

    def stringToDates(self, columns):
        for column in columns:
            if(column in self.pitchDataFrame):
                self.pitchDataFrame[column] = self.pitchDataFrame[column].apply(pd.to_datetime, errors="coerce",
                                                                          format="%Y-%m-%d %H:%M:%S")

    def stringToHeight(self):
        if(len(self.pitchDataFrame) > 0):
            self.pitchDataFrame["atbat_b_height"] = self.pitchDataFrame.apply(
                lambda row: heightStringToFeet(row["atbat_b_height"]), axis=1)  # heightStringToFeet(row["atbat_b_height"]

    # def atbatScoreRemoveNan(self):
    #     self.pitchDataFrame.loc[self.pitchDataFrame["atbat_score"] == "nan"] = 0

    def notNanToBoolean(self, columns):
        for column in columns:
            if(column in self.pitchDataFrame):
                self.pitchDataFrame[column].fillna(False, inplace=True)
                self.pitchDataFrame.loc[self.pitchDataFrame[column] != False, column] = True

    def addPitchTimeSinceGameStartColumn(self):
        try:
            self.pitchDataFrame["since_game_start"] = self.pitchDataFrame["pitch_tfs_zulu"] - self.pitchDataFrame[
            "atbat_start_tfs_zulu"].min()
        except Exception as e:
            warnings.warn(str(e),Warning)
            self.pitchDataFrame["since_game_start"] = ""

    def addPitchTimeSinceAtBatStartColumn(self):
        self.pitchDataFrame["since_at_bat_start"] = self.pitchDataFrame["pitch_tfs_zulu"] - self.pitchDataFrame[
            "atbat_start_tfs_zulu"]
        self.pitchDataFrame.loc[
            self.pitchDataFrame["since_at_bat_start"] < pd.Timedelta(0), "since_at_bat_start"] = pd.Timedelta(0)

    def removeDataWithoutPxPz(self):
        self.pitchDataFrame = self.pitchDataFrame.replace('nan', np.nan) \
            .dropna(subset=["pitch_px", "pitch_pz"])

    def removeDataWithoutSxSz(self):
        self.pitchDataFrame = self.pitchDataFrame[
            (self.pitchDataFrame["pitch_sz_top"] - self.pitchDataFrame["pitch_sz_bot"]) != 0]

    def removeHits(self):
        self.pitchDataFrame = self.pitchDataFrame[self.pitchDataFrame.pitch_type != "X"]

    def onlyCalledStrikes(self):
        if("pitch_code" in self.pitchDataFrame):
            self.pitchDataFrame = self.pitchDataFrame[
            ~((self.pitchDataFrame.pitch_type == "S") & (self.pitchDataFrame.pitch_code != "C"))]
        else:
            warnings.warn("No pitch code information, strikes are not necessarily only called strikes",Warning)

    def addNormalizedPitchZColumn(self):
        self.pitchDataFrame["normalized_pitch_pz"] = self.pitchDataFrame.apply(
            lambda row: (row["pitch_pz"] - row["pitch_sz_bot"]) / (row["pitch_sz_top"] - row["pitch_sz_bot"]), axis=1)

    def addNormalizedPitchXColumn(self):
        self.pitchDataFrame["normalized_pitch_px"] = self.pitchDataFrame.apply(
            lambda row: (row["pitch_px"] + (1.42 / 2)) / 1.42 , axis=1)

    def addCorrectCallColumn(self):
        ball_radius = 0.1729 / 2
        sz_left = 0 - ball_radius
        sz_right = 1 + ball_radius
        sz_bottom = 0 - ball_radius
        sz_top = 1 + ball_radius

        self.pitchDataFrame["in_strike_zone"] = False
        self.pitchDataFrame.loc[
            (self.pitchDataFrame["normalized_pitch_px"] > sz_left)
            & (self.pitchDataFrame["normalized_pitch_px"] < sz_right)
            & (self.pitchDataFrame["normalized_pitch_pz"] > sz_bottom)
            & (self.pitchDataFrame["normalized_pitch_pz"] < sz_top), "in_strike_zone"] = True

    def addGameDateColumn(self):
        self.pitchDataFrame["data"] = ""
        if(len(self.pitchDataFrame) > 0):
            self.pitchDataFrame["date"] = self.pitchDataFrame["atbat_start_tfs_zulu"].dt.date

    def changeKeys(self, d, keyword):
        result = {}
        for key in d.keys():
            new_key = keyword + "_" + key
            result[new_key] = d[key]
        return result


def heightStringToFeet(heightString):
    try:
        numbers = heightString.split("-")
        assert (len(numbers) == 2)
        return int(numbers[0]) + float(numbers[1]) / 12
    except AssertionError:
        return heightString  # on failure, return the string
