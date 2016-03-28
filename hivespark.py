from pyspark.sql import HiveContext,Row
sqlcontext = HiveContext(sc)
sqlcontext.sql("show tables")
RDD_text = sc.textFile("/user/cloudera/age.txt")
RDD_people = RDD_textsplit.map(
    lambda x: Row(name=x[0],age=int(x[1])))
df_people = sqlContext.createDataFrame(RDD_people)
df_people.registerTempTable("people")
gpnames = sqlContext.sql("select  names, count(*) from people")
gpnames = sqlContext.sql("select  names, count(*) from people group by names")
