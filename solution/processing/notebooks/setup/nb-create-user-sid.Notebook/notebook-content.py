# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import uuid

object_id = "7977fa2b-dfc0-4483-9046-9354fd0925e9"  # lim inn Object ID her
sid = "0x" + uuid.UUID(object_id).bytes_le.hex().upper()
print(sid)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

'''
CREATE USER [geir.forsmo.atea@siva.no]
    WITH SID = 0x2BFA7779C0DF834490469354FD0925E9, TYPE = E;
GO
'''

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
