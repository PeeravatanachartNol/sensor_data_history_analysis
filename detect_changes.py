import pandas as pd
from dicts import *

def get_status_bin(row):
    return str(df["status_bin"].values[row])

df = pd.read_csv("./data/4f_sample_11.csv", encoding="ISO-8859-1")
df = df.sort_values(by="id")
num_rows = df.shape[0]

# make new column "sid" for calculated sensor_id
df["sid"] = None

# find changed bit position(s)
for i in range(num_rows):
    prev_status_bin = "1111111111111111" if i==0 else get_status_bin(i-1)
    curr_status_bin = get_status_bin(i)

    # check different bit position
    changed_pos = []
    for pos in range(16):
        if curr_status_bin[pos] != prev_status_bin[pos]:
            if pos in pos_to_sensor_id_11:
                changed_pos.append(pos_to_sensor_id_11[pos])

    # insert changed position into sid column
    if changed_pos:
        reversed_changed_pos = changed_pos[::-1]
        for j in range(len(changed_pos)):
            df["sid"].values[i+j] = reversed_changed_pos[j]

    print(f"{prev_status_bin}, {curr_status_bin}", reversed_changed_pos)



# insert new column "svalue" for calculated sensor_value
# check whether this is possible or not, 1>0>1>0 alternating entries is normal behavior
# should not see two consecutive numbers

# compare the columns sensor_id and sid
sid_diff_arr = (df["sensor_id"] != df["sid"]).tolist()
sid_diff_rows = [i for i,x in enumerate(sid_diff_arr) if x is True]
print("Rows where calculated sensor_id does not match actual sensor_id:", sid_diff_rows)

# write
df.to_csv("./data/4f_sample_11_edited.csv")
print("--------------------------------------------------")
print("Wrote to csv")
