import pandas as pd
from dicts import *

def get_status_bin(row):
    return str(df["status_bin"].values[row])

stv_num = 10
bit_to_sid = bit_to_sid_10 if stv_num == 10 else bit_to_sid_11

df = pd.read_csv(f"./data/4f_sample_{stv_num}.csv", encoding="ISO-8859-1")
df = df.sort_values(by="timestamp")

# STEP 1
# check whether the sensor_value of each sensor follows the pattern of 0101010 or not
# if not then swap

for bit, sid in bit_to_sid_10.items():
    # print sensor_id and corresponding integer
    print(">>> Sensor ID", sid, "Bit", bit, "<<<")
    sensor_df = df[df["sensor_id"] == f"{sid}"]
    sval_arr = sensor_df["sensor_value"].to_list()
    print("sensor_value", sval_arr)

    incorrect_local_indices = []
    for i in range(len(sval_arr) - 1): # ignore last term
        prev_sval = 1 if i==0 else sval_arr[i-1]

        if sval_arr[i] == prev_sval:
            # looping sval swap until arr follow 0101010 pattern
            sval_arr[i], sval_arr[i+1] = sval_arr[i+1], sval_arr[i]
            incorrect_local_indices.append(i)
            incorrect_local_indices.append(i+1)

    incorrect_indices = sensor_df.index[incorrect_local_indices].to_list()
    print("Incorrect indices local", incorrect_local_indices)
    print("Incorrect indices", incorrect_indices)

    # perform row swaps
    for i in range(0, len(incorrect_indices), 2):
        first_row, second_row = incorrect_indices[i], incorrect_indices[i+1]
        temp = df.iloc[first_row].copy()
        df.iloc[first_row] = df.iloc[second_row]
        df.iloc[second_row] = temp
        print("Swapped rows", first_row, second_row)
    print("--------------------------------------------------")
    # sensor_df.to_csv(f"./data/sensor_{stv_num}.csv", index=True)

# STEP 2
# calculate sensor_id based on bit position

# # make new column "sid" for calculated sensor_id
# df["sid"] = None

# # find changed bit position(s)
# for i in range(df.shape[0]):
#     prev_status_bin = "1111111111111111" if i==0 else get_status_bin(i-1)
#     curr_status_bin = get_status_bin(i)

#     # check different bit position
#     changed_bit = []
#     for bit in range(16):
#         if curr_status_bin[bit] != prev_status_bin[bit]:
#             if bit in bit_to_sid:
#                 changed_bit.append(bit_to_sid[bit])

#     # insert changed position into sid column
#     if changed_bit:
#         reversed_changed_bit = changed_bit[::-1]
#         for j in range(len(changed_bit)):
#             df["sid"].values[i+j] = reversed_changed_bit[j]

#     # print(f"{prev_status_bin}, {curr_status_bin}", reversed_changed_bit)



# # compare the columns sensor_id and sid
# sid_diff_arr = (df["sensor_id"] != df["sid"]).tolist()
# sid_diff_indices = [i for i,x in enumerate(sid_diff_arr) if x is True]
# print("Rows where calculated sensor_id does not match actual sensor_id:", sid_diff_indices)

# sensor_arr = [[sensor_indices[i], sensor_sval[i]] for i in range(len(sensor_indices))]

# write
df.to_csv(f"./data/4f_sample_{stv_num}_edited.csv", index=False)
print("----- Wrote to csv -----")
