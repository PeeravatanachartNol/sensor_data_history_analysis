import pandas as pd
from dicts import *

# read and split df into 10 and 11
df = pd.read_csv("./data/4f_sample.csv", encoding="ISO-8859-1")
df_10 = df[df["sensor_id"].isin(bit_to_sid_10.values())]
df_10 = df_10.sort_values(by=["timestamp", "sensor_id"])
df_11 = df[df["sensor_id"].isin(bit_to_sid_11.values())]
df_11 = df_11.sort_values(by=["timestamp", "sensor_id"])

# # insert original_order column
df_10.insert(0, "original_order", range(0, len(df_10)))
df_11.insert(0, "original_order", range(0, len(df_11)))

# write
df_10.to_csv("./data/4f_sample_10.csv", index=False)
df_11.to_csv("./data/4f_sample_11.csv", index=False)

print("Split csv into two csv's for STV 10 and STV 11")
