# emberDB
This is a rudimentary noSQL style database that I created from scratch in python.

# Before Launching Ember DB:
Remove all existing .txt files in working directory (from previous program runs)

Should import .csv files you will be using into working directory

To launch emberDB, execute the following: “python3 emberDB.py”

# Assumptions:
*make assumes there is a header included in csv and skips the first line

Each table needs to be created (*make) before inserting data into that table

When you create a table, the rows should be in the exact order as in the csv - though you can change the names of the columns if you want (ie. change “TDS Value (ppm)” to “tds”)

You can exit the program at any time by typing exit as your query

# Example of Execution (submit each query one at a time):
*make:plant_chemistry-ID,Date,TDS,ph

*make:plant_climate-ID,Date,Temp,Humidity

*make:house-rooms,beds,baths

*showTables:house,plant_chemistry,plant_climate

*addInto:plant_chemistry*csv:plant_chemistry.csv

*addInto:plant_climate*csv:plant_climate.csv

*addInto:plant_chemistry*values-1,8/4/23,500,7

*replace:plant_chemistry*set:tds=500,ph=7*filterBy:ph=6.8

*remove:house

*remove:tds*inTable:plant_chemistry

*remove:plant_chemistry*filterBy:ph=7

*groupBy:ph*inTable:plant_chemistry

*groupBy:ph*inTable:plant_chemistry*aggregateBy:ID*function:cnt

*orderBy:plant_chemistry*column:ph

*connect:plant_chemistry,plant_climate*using:ID

*find:id,ph*inTable:plant_chemistry*orderby:ph

*find:id,ph*inTable:plant_chemistry*filterBy:ph<6.5*orderBy:ph

exit

