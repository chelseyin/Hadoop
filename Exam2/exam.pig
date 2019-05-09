REGISTER 'hdfs://hadoop-m1.us-central1-f.c.halogen-choir-235521.internal:8020/user/root/tmp/Exams/yincUDF.jar'
DEFINE CONVERT edu.rosehulman.yinc.Convert();
DEFINE CONCATENATE edu.rosehulman.yinc.Concatenate();
grade = LOAD '$gradeInput' using PigStorage(',') AS  (fName:chararray, lName:chararray, CourseNumber:chararray, Score:int);
course = LOAD '$courseInput' using PigStorage(',') AS  (CourseNumber:chararray, CourseName:chararray);
fgrade = FILTER grade by Score<=90;
total = JOIN fgrade by CourseNumber, course by CourseNumber;
result = FOREACH total GENERATE CONCATENATE($0,$1) as name,$2,$5,CONVERT($3) as grade;
STORE result into '$pigOutput/$username' using PigStorage('\t');