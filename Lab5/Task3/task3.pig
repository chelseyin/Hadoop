REGISTER 'file:////home/yinchelsey/hadoop/Lab5/Task3/Lab5Task3-1.0-SNAPSHOT.jar'
DEFINE FILTERUDF edu.rosehulman.yinc.Filter();
data = LOAD 'input/tempInput.txt' using PigStorage('\t') AS (year:chararray, temp:int, number:int);
fdata = FILTER data by FILTERUDF(number);
gdata = GROUP fdata by year;
tempInfo = FOREACH gdata GENERATE group, MAX(fdata.temp) AS MaxTemp, MIN(fdata.temp) AS MinTemp, AVG(fdata.temp) AS AvgTemp;
STORE tempInfo into 'output/' using PigStorage(',');