import pandas as pd
from dicts import *

# read and split df into 10 and 11
df = pd.read_csv("./data/4f_sample.csv", encoding="ISO-8859-1")
df_10 = df[df["sensor_id"].isin(bit_to_sensor_id_10.values())]
df_10 = df_10.sort_values(by="id")
df_11 = df[df["sensor_id"].isin(pos_to_sensor_id_11.values())]
df_11 = df_11.sort_values(by="id")

# write
# df_10.to_csv("./data/4f_sample_10.csv")
df_11.to_csv("./data/4f_sample_11.csv")

print("Split csv into two csv's for STV 10 and STV 11")
