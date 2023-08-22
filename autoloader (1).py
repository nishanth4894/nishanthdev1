# Databricks notebook source
Ingesting the data into your lakehouse
1. Autoloader(Streaming)
2. COPY INTO (SQL)

# COMMAND ----------

# Syntax
spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", <source_format>)
.load('/path/to/files’)
.writeStream
.option("checkpointLocation", <checkpoint_directory>)
.table(<table_name>)

# COMMAND ----------

input_file="dbfs:/mnt/databrickshexaware/raw/input_stream"

# COMMAND ----------

output_path="dbfs:/mnt/databrickshexaware/raw/output_stream"

# COMMAND ----------

(spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/nishanth/autoloader/checkpoint")
.option("path",f"{output_path}/nishanth/autoloader/output")
.table("autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC use nishanth

# COMMAND ----------

( spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.schemaLocation",f"{output_path}/nishanth/autoloader/schemalocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/nishanth/autoloader/checkpoint")
.option("path",f"{output_path}/nishanth/autoloader/output")
.table("nishanth.autoloader")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from hive_metastore.nishanth.autoloader

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

( spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation",f"{output_path}/nishanth/autoloader1/schemalocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/nishanth/autoloader1/checkpoint")
.option("path",f"{output_path}/nishanth/autoloader1/output")
.table("nishanth.autoloader1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nishanth.autoloader1

# COMMAND ----------

( spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation",f"{output_path}/nishanth/autoloader1/schemalocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/nishanth/autoloader1/checkpoint")
.option("path",f"{output_path}/nishanth/autoloader1/output")
.table("nishanth.autoloader1")
)

# COMMAND ----------

( spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation",f"{output_path}/nishanth/autoloader1/schemalocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/nishanth/autoloader1/checkpoint")
.option("path",f"{output_path}/nishanth/autoloader1/output")
.option("mergeSchema",True)
.table("nishanth.autoloader1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nishanth.autoloader1

# COMMAND ----------

( spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation",f"{output_path}/nishanth/autoloader1/schemalocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/nishanth/autoloader1/checkpoint")
.option("path",f"{output_path}/nishanth/autoloader1/output")
.option("mergeSchema",True)
.table("nishanth.autoloader1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nishanth.autoloader1

# COMMAND ----------

# MAGIC %fs ls

# COMMAND ----------

( spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation",f"{output_path}/nishanth/autoloader1/schemalocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/nishanth/autoloader1/checkpoint")
.option("path",f"{output_path}/nishanth/autoloader1/output")
.option("mergeSchema",True)
.table("nishanth.autoloader1")
)

# COMMAND ----------

( spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaLocation",f"{output_path}/nishanth/autoloader1/schemalocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/nishanth/autoloader1/checkpoint")
.option("path",f"{output_path}/nishanth/autoloader1/output")
.option("mergeSchema",True)
.table("nishanth.autoloader1")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nishanth.autoloader1

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### Stream should Not STOP

# COMMAND ----------

( spark.readStream
.format("cloudFiles")
.option("cloudFiles.format", "csv")
.option("cloudFiles.inferColumnTypes",True)
.option("cloudFiles.schemaEvolutionMode","rescue")
.option("cloudFiles.schemaLocation",f"{output_path}/nishanth/autoloader2/schemalocation")
.load(f"{input_file}")
.writeStream
.option("checkpointLocation", f"{output_path}/nishanth/autoloader2/checkpoint")
.option("path",f"{output_path}/nishanth/autoloader2/output")
.option("mergeSchema",True)
.table("nishanth.autoloader2")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from nishanth.autoloader2

# COMMAND ----------

for stream in spark.streams.active:
    stream.stop()

# COMMAND ----------

COPY INTO(SQL)

# COMMAND ----------


