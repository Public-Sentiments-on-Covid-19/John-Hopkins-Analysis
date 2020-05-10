# url of the data being parsed
URL="https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
CSVNAME="data.csv"
FILENAME="confirmed_cases.csv"
TYPE="confirmed"

# clean up any remaining files that exist
rm *.class
rm *.jar
rm $FILENAME
hdfs dfs -rm -r -f data 

# get the data and store inside HDFS
hdfs dfs -mkdir data 
hdfs dfs -mkdir data/input
hdfs dfs -mkdir data/cleaned
hdfs dfs -put $CSVNAME data/input
hdfs dfs -ls data/input

# compile the code and run it on the hadoop cluster
javac -classpath `yarn classpath` -d . CleanMapper.java
javac -classpath `yarn classpath` -d . CleanReducer.java
javac -classpath `yarn classpath`:. -d . Clean.java
jar -cvf Clean.jar *.class
hadoop jar Clean.jar Clean /user/dec415/data/input/$CSVNAME /user/dec415/data/output $TYPE

# rename the output
hdfs dfs -mv data/output/$TYPE-r-00000 data/cleaned/$FILENAME
hdfs dfs -rm -r -f data/output

# compile the code to count daily increases
javac -classpath `yarn classpath` -d . DailyChangesMapper.java
javac -classpath `yarn classpath` -d . DailyChangesReducer.java
javac -classpath `yarn classpath`:. -d . DailyChanges.java
jar -cvf DailyChanges.jar *.class
hadoop jar DailyChanges.jar DailyChanges /user/dec415/data/cleaned/$FILENAME /user/dec415/data/output $TYPE

# store the output
hdfs dfs -mv data/output/$TYPE-r-00000 $FILENAME 
