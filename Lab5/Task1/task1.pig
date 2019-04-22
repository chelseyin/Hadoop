data = LOAD '/tempInput.txt' using PigStorage('\t') AS (year:chararray, temp:int, number:int);
fdata = FILTER data by (number == 0 OR number ==1);
gdata = GROUP fdata by year;
tempInfo = FOREACH gdata GENERATE group, MAX(fdata.temp) AS MaxTemp, MIN(fdata.temp) AS MinTemp, AVG(fdata.temp) AS AvgTemp;
STORE tempInfo into '/Output' using PigStorage(',');
