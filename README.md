# MLB-Crawler

The goal of this project is to create a simple tool for downloading and parsing data from the PitchF/X dataset provided by the MLB.
This repository contains a variety of helpful Luigi tasks for generating both `.csv` data and visualizations.
Data analysis funcitons are heavily focused on pitching data, but it is possible to modify the parser to fit your needs.

<img src="https://github.com/gnitsua/MLB-Crawler/blob/master/doc/2015_histogram.png" width="350">

# Installation

This project uses Anaconda for installation. You can get it here: https://www.anaconda.com/

After installing Anaconda run the following command:

`
conda install --name myenv --file envoriment.yml
`

Be sure to activate your enviroment before running

# Execution

To run the parser use the following command (replace "2015" with the year you wish to parse):

`python MlbCrawler.py 2015`

# Task architecture

<img src="https://github.com/gnitsua/MLB-Crawler/blob/master/doc/Untitled%20Diagram.png" width="550">

# Game downloads

Schedule data is downloaded automatically for a given year from https://www.retrosheet.org/ . Using this list of games, the parser attempts to download all data from a given year and places it in the folder. *Note*: if any games fail to download (if they aren't on the site for some reason, or something else is off) the game simply won't be included.

# Data cleaning

Data cleaning takes place in `PitchStatParser.py`. Here, the data goes through several steps of cleaning that can be disabled simply by commenting out lines in `clean_data()`. If for some reason a dataset cannot be cleaned, it will be skipped.

# Data visualization

Visualization is provided in the repo primarily as an example of what can be done. Visualization tasks are stored in the `visualization_tasks`, and a variety of examples are provided.

## TODO:

Add support for rain dates

Add support for double headers
