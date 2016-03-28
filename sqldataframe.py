# Creación de Dataframe en 3 pasos cuando se necesita un parseo.
schemastring= "name age"
# Crear schema y luego dataframe a partir de estos datos
fields = [StructField(field_name, StringType(), True) for field_name in schemaString.split()]
schema = StructType(fields)

# Apply the schema to the RDD.
schemaPeople = sqlContext.createDataFrame(people, schema)
