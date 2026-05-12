# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

schema = StructType([
    StructField("source_id",      IntegerType(), nullable=False),
    StructField("endpoint_path",  StringType(),  nullable=False),
    StructField("watermark_date", StringType(),  nullable=True),
    StructField("watermark_id",   IntegerType(), nullable=True),
    StructField("updated_at",     TimestampType(), nullable=True),
])

data = [
    (2, '/oppdateringer/enheter', None,                                         None, datetime.now()),
    (3, 'data-meldingslogg',      None,                       None,     datetime.now()),
    (4, 'Regnskapbedrifter.xlsx', None, None,     datetime.now()),
]

df = spark.createDataFrame(data, schema)
df.write.mode("append").saveAsTable(
    "`av01-dev-datastores`.`lh_av01_admin`.metadata.watermark_store"
)
print("Seed ferdig")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
