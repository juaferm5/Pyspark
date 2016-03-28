# Vamos a realizar un Script para practicar cosas con Dataframes de Spark.
RDD_text = sc.textFile("/user/cloudera/age.txt")
RDD_text.collect()
RDD_textsplit = RDD_text.map(lambda x: x.split(','))
# Hay que importar el módulo Row de Pyspark para asignar números de filas
from pyspark.sql import Row
# Map para asignar rows
RDD_people = RDD_textsplit.map(
    lambda x: Row(name=x[0],age=int(x[1])))
df_people = sqlContext.createDataFrame(RDD_people)
df_people.registerTempTable("people")
names = sqlContext.sql(
    "select name from people where age>=13 AND age<=22")
# Como resultado se puede remostar como RDD ordenado.
RDD_names = names.map(lambda x: "Name: "+p.name)
for i in RDD_names.collect():
    print(i)
''' stout:
    Name: Juan
    Name: Luis
    Name: Juan
    Name: Carlos
    Name: Margarita
    Name: Maria
'''
