myDf.createOrReplaceTempView("stats") 
sqlContext.sql("create table covidStats as select * from stats")

