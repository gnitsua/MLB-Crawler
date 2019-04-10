import matplotlib.pyplot as plt
import numpy as np
from matplotlib.collections import PatchCollection
from matplotlib.patches import Circle


class Visualizer:
    def set_up_sz_plot(self, title):
        f = plt.figure()
        ax = f.gca()
        ax.axis('equal')
        self.plot_sz(ax)
        ax.set(xlim=[-2, 3], ylim=[-2, 3])
        ax.set_xlabel("Distance (ft)")
        ax.set_ylabel("Distance (ft)")
        ax.set_title(title)

        return f, ax

    def plot_sz(self, ax):
        ext_x = [0, 1, 1, 0, 0, 0.01, 0.01, 0.99, 0.99, 0.01]
        ext_y = [0, 0, 1, 1, 0, 0.01, 0.99, 0.99, 0.01, 0.01]
        ax.fill(ext_x, ext_y, color='k')
        # ax.add_patch(
        #     Rectangle((0, 0), 1, 1, color=(0, 1, 0, 0.25)))  # 17 inches = 1.42 feet

    def show_pitches(self, strikes, balls, file=None, title="Missed Pitch Calls"):
        fig, ax = self.set_up_sz_plot(title)
        # strikes = self.data[self.data["pitch_type"] == "S"]
        # balls = self.data[self.data["pitch_type"] == "B"]

        ball_circles = [Circle((xi, yi), radius=0.1729 / 2, edgecolor="k") for xi, yi in
                        zip(balls["normalized_pitch_px"], balls["normalized_pitch_pz"])]
        bc = PatchCollection(ball_circles, facecolors="b", edgecolors="k")
        ax.add_collection(bc)

        strike_circles = [Circle((xi, yi), radius=0.1729 / 2) for xi, yi in
                          zip(strikes["normalized_pitch_px"], strikes["normalized_pitch_pz"])]
        sc = PatchCollection(strike_circles, facecolors="r", edgecolors="k")
        # sc = PatchCollection(strike_circles, facecolors="r")
        ax.add_collection(sc)

        ax.legend(handles=[Circle((0, 0), radius=0.1205, color='r', label="Called Strikes"),
                           Circle((0, 0), radius=0.1205, color='b', label="Called Balls")])

        if (file == None):
            plt.show()
        else:
            plt.savefig(file, format="png")

    def get_2dhist(self, strikes, balls):
        ball_hist, xedges, yedges = np.histogram2d(balls["normalized_pitch_pz"], balls["normalized_pitch_px"],
                                                   range=[[-1, 2], [-1, 2]], bins=40, density=True)
        strike_hist, _, _ = np.histogram2d(strikes["normalized_pitch_pz"], strikes["normalized_pitch_px"],
                                           range=[[-1, 2], [-1, 2]], bins=40, density=True)
        return strike_hist - ball_hist, xedges, yedges

    def show_probability(self, strikes, balls, file=None, title="Probability Distribution of Pitches"):
        fig, ax = self.set_up_sz_plot(title)

        probability_hist, xedges, yedges = self.get_2dhist(strikes, balls)

        farthest_distance_from_zero = np.maximum(abs(np.min(probability_hist)), abs(np.max(probability_hist)))
        ax.imshow(probability_hist, interpolation='nearest', origin='low',
                  extent=[yedges[0], yedges[-1], xedges[0], xedges[-1]], cmap="bwr", vmin=-farthest_distance_from_zero,
                  vmax=farthest_distance_from_zero)

        if (file == None):
            plt.show()
        else:
            plt.savefig(file, format="png")

    def get_differential_2dhist(self, strikes1, balls1, strikes2, balls2):
        strikes, xedges, yedges = self.get_2dhist(strikes1, strikes2)
        balls, _, _ = self.get_2dhist(balls1, balls2)
        combined = strikes + balls
        magnitude = np.sum(np.abs(combined))/(40*40)*100
        return combined, xedges, yedges, magnitude


    def show_differential_probability(self, strikes1, balls1, strikes2, balls2, file=None,
                                      title="Difference between the Probability Distribution of Pitches"):
        fig, ax = self.set_up_sz_plot(title)


        combined, xedges, yedges, magnitude = self.get_differential_2dhist(strikes1,balls1,strikes2,balls2)

        ax.imshow(combined, interpolation='nearest', origin='low',
                  extent=[xedges[0], xedges[-1], yedges[0], yedges[-1]],cmap="bwr", vmin=-1, vmax=1)

        ax.set_title(title+" (%0.2f%%)"%(magnitude))

        if (file == None):
            plt.show()
        else:
            plt.savefig(file, format="png")
