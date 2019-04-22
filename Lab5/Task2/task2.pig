REGISTER 'file:////home/yinchelsey/hadoop/Lab5/Task2/Lab5Task2-1.0-SNAPSHOT.jar'
DEFINE UPPER edu.rosehulman.yinc.Upper();
lines = LOAD 'input/testFile.txt' AS (line:chararray);
words = FOREACH lines GENERATE FLATTEN(TOKENIZE(line)) as word;
grouped = GROUP words BY UPPER(word);
wordcount = FOREACH grouped GENERATE group, COUNT(words);
STORE wordcount into 'output/' using PigStorage(',');