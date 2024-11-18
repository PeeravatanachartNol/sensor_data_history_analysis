import pandas as pd
from dicts import *

def get_status_bin(row):
    return str(df["status_bin"].values[row])

stv_num = 11
bit_to_sid = bit_to_sid_10 if stv_num == 10 else bit_to_sid_11

df = pd.read_csv(f"./data/4f_sample_{stv_num}.csv", encoding="ISO-8859-1")
df = df.sort_values(by="timestamp")
# make new column "sid" for calculated sensor_id
df["sid"] = None

# find changed bit position(s)
for i in range(df.shape[0]):
    prev_status_bin = "1111111111111111" if i==0 else get_status_bin(i-1)
    curr_status_bin = get_status_bin(i)

    # check different bit position
    changed_bit = []
    for bit in range(16):
        if curr_status_bin[bit] != prev_status_bin[bit]:
            if bit in bit_to_sid:
                changed_bit.append(bit_to_sid[bit])

    # insert changed position into sid column
    if changed_bit:
        reversed_changed_bit = changed_bit[::-1]
        for j in range(len(changed_bit)):
            df["sid"].values[i+j] = reversed_changed_bit[j]

    # print(f"{prev_status_bin}, {curr_status_bin}", reversed_changed_bit)



# check whether sensor_value follow the pattern of 0101010 or not
# if not swap between same sensor 11-0
each_sensor_rows = df.index[df["sensor_id"] == "45600"].tolist()

each_sensor_sval = []
for row in each_sensor_rows:
    sval = int(df.loc[row, "sensor_value"])
    each_sensor_sval.append(sval)

print(each_sensor_rows)
print(each_sensor_sval)

incorrect_indices = []
for i in range(1, len(each_sensor_sval)):
    if each_sensor_sval[i] == each_sensor_sval[i-1]:
        incorrect_indices.append(i)
print(incorrect_indices)  

# compare the columns sensor_id and sid
# sid_diff_arr = (df["sensor_id"] != df["sid"]).tolist()
# sid_diff_rows = [i for i,x in enumerate(sid_diff_arr) if x is True]
# print("Rows where calculated sensor_id does not match actual sensor_id:", sid_diff_rows)
# print(df.shape[0])

# each_sensor_arr = [[each_sensor_rows[i], each_sensor_sval[i]] for i in range(len(each_sensor_rows))]



# write
df.to_csv(f"./data/4f_sample_{stv_num}_edited.csv")
print("--------------------------------------------------")
print("Wrote to csv")
