import pandas as pd
from deltalake.writer import write_deltalake
from deltalake import DeltaTable

# Create a Pandas DataFrame
df = pd.DataFrame({"data": range(5)})

# Write to the Delta Lake table
write_deltalake("./tmp/deltars_table", df, mode="overwrite")

# Append new data
df = pd.DataFrame({"data": range(6, 11)})
write_deltalake("./tmp/deltars_table", df, mode="append")

# Read the Delta Lake table
dt = DeltaTable("./tmp/deltars_table")

# Show the Delta Lake table
table = dt.to_pandas()

# Display the Delta Lake table
print("Delta Lake Table:")
print(table)
