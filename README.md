# Parallel Distributed Processing Assignments
Assignments that I made for the subject "Parallel Distributed Processing" in my Big Data &amp; AI minor

## Assignment 1
This Python script made with MRJob takes a data file containing user ratings for movies and counts how many ratings each movie received. It then sorts the list by the count of the ratings.

### Usage
Run this with all available nodes within Hadoop as follows:  
`sudo python ass1.py -r hadoop --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar u.data`

or run this locally for testing:  
`python ass1.py u.data`

## Assignment 2
These Apache Pig script take a data file from the playdiplomacy.com dataset and does (1) list how many times "Holland" was a target for an order from any location and (2) lists how many times each country won a game.

To use these scripts you need to:

1. Upload the .csv files players.csv and orders.csv (not included due to size) in the Files View in Ambari.
2. In Ambari, go to the Pig View and create new scripts containing the code from these scripts.
3. Replacing the .csv file location in the second line.
4. Running the script with Tez (for fastest results).