import pandas as pd
from dicts import *

def get_status_bin(row):
    return str(df["status_bin"].values[row])

stv_num = 10
bit_to_sid = bit_to_sid_10 if stv_num == 10 else bit_to_sid_11

df = pd.read_csv(f"./data/4f_sample_{stv_num}.csv", encoding="ISO-8859-1")
df = df.sort_values(by="timestamp")

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


# STEP 1
# check whether the sensor_value of each sensor follows the pattern of 0101010 or not
# if not then swap

sensor_df = df[df["sensor_id"] == "45570"]
sval_arr = sensor_df["sensor_value"].to_list()
print(sval_arr)

incorrect_local_indices = []
for i in range(len(sval_arr) - 1): # ignore last term
    prev_sval = 1 if i==0 else sval_arr[i-1]

    if sval_arr[i] == prev_sval:
        # swap
        sval_arr[i], sval_arr[i+1] = sval_arr[i+1], sval_arr[i]
        incorrect_local_indices.append(i)
        incorrect_local_indices.append(i+1)
print(incorrect_local_indices)
incorrect_indices = sensor_df.index[incorrect_local_indices].to_list()
print(incorrect_indices)
# incorrect_df = sensor_df[sensor_df[""]]
# sensor_sval = []
# for row in filte:
#     sval = int(df.loc[row, "sensor_value"])
#     sensor_sval.append(sval)

sensor_df.to_csv(f"./data/sensor_{stv_num}.csv", index=True)

# print("Row number", sensor_indices)
# print("Sensor values", sensor_sval)

# incorrect_local_indices = []
# for i in range(len(sensor_sval)):
#     prev_sensor_sval = 1 if i==0 else sensor_sval[i-1]

#     if sensor_sval[i] == prev_sensor_sval:
#         # swap sensor values array
#         temp = sensor_sval[i]
#         sensor_sval[i] = sensor_sval[i+1]
#         sensor_sval[i+1] = temp
#         incorrect_local_indices.append(i)
        
# incorrect_indices = [sensor_indices[i] for i in incorrect_local_indices]
# print("Local row number (incorrect sensor values)", incorrect_local_indices)  
# print("Row number (incorrect sensor values)", incorrect_indices)  

# # perform row swaps
# for i in range(len(incorrect_indices)):
#     first_row, second_row = incorrect_indices[i], incorrect_indices[i]+1
#     temp = df.iloc[first_row].copy()
#     df.iloc[first_row] = df.iloc[second_row]
#     df.iloc[second_row] = temp
#     print("Swapped rows", first_row, second_row)

# # test by swapping first two rows
# first_row, second_row = 111,112
# temp = df.iloc[first_row].copy()
# df.iloc[first_row] = df.iloc[second_row]
# df.iloc[second_row] = temp
# print("swapped rows")

# # compare the columns sensor_id and sid
# sid_diff_arr = (df["sensor_id"] != df["sid"]).tolist()
# sid_diff_indices = [i for i,x in enumerate(sid_diff_arr) if x is True]
# print("Rows where calculated sensor_id does not match actual sensor_id:", sid_diff_indices)
# print(df.shape[0])

# sensor_arr = [[sensor_indices[i], sensor_sval[i]] for i in range(len(sensor_indices))]

# write
df.to_csv(f"./data/4f_sample_{stv_num}_edited.csv", index=False)
print("--------------------------------------------------")
print("Wrote to csv")
