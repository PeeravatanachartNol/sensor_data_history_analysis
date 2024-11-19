import pandas as pd
from dicts import *

stv_num = 10
bit_to_sid = bit_to_sid_10 if stv_num == 10 else bit_to_sid_11

df = pd.read_csv(f"./data/4f_sample_{stv_num}.csv", encoding="ISO-8859-1")
df = df.sort_values(by=["timestamp", "sensor_id"])


for bit, sid in bit_to_sid.items():
    sensor_df = df[df["sensor_id"] == f"{sid}"]
    sval_arr = sensor_df["sensor_value"].to_list()
    print(">>> Sensor ID", sid, "Bit", bit, "<<<")
    print("sensor_value START", sval_arr)

    # STEP 1
    # apply row swaps until sensor_value follows [0,1,0,1,0, ...] pattern
    # get incorrect indices locally
    incorrect_local_indices = []
    swapped = True
    while swapped:
        swapped = False
        for i in range(len(sval_arr) - 1): # ignore last term
            prev_sval = 1 if i==0 else sval_arr[i-1]

            if sval_arr[i] == prev_sval:
                # apply swaps to sval_arr
                sval_arr[i], sval_arr[i+1] = sval_arr[i+1], sval_arr[i]
                incorrect_local_indices.append(i)
                incorrect_local_indices.append(i+1)
                print("Swapped sensor_value", i, i+1)
                swapped = True

    print("sensor_value END", sval_arr)
    print("Incorrect indices local", incorrect_local_indices)

    # STEP 2
    # map incorrect_local_indices to df rows
    # but before swapping, check whether the row below has a similar timestamp or not
    # if there is, swap that entry as well

    # 34 and 12 swap would result in the order 3, 4, 1, 2
    # 34 and 1 swap would result in the order 3, 4, 2, 1
    # 3 and 12 swap would result in the order 3, 1, 2, 4
    # 3 and 1 swap would result in the order 3, 2, 1, 4


    # THIS NEEDS FIXING
    for idx, loc in enumerate(range(0, len(incorrect_local_indices), 2)):
        first_row = sensor_df.index[loc]
        second_row = sensor_df.index[loc] + 1
        third_row = sensor_df.index[loc+1]
        fourth_row = sensor_df.index[loc+1] + 1
        print("+++++++++++++++++++++++++Rows", first_row, second_row, third_row, fourth_row)

        temp = df.iloc[[first_row, second_row, third_row, fourth_row]].copy()
        df.iloc[first_row] = temp.iloc[2]

        if df["timestamp"].values[third_row] == df["timestamp"].values[fourth_row]:
            df.iloc[second_row] = temp.iloc[3]

            if df["timestamp"].values[first_row] == df["timestamp"].values[second_row]:
                df.iloc[third_row] = temp.iloc[0]
                df.iloc[fourth_row] = temp.iloc[1]
            else:
                df.iloc[third_row] = temp.iloc[1]
                df.iloc[fourth_row] = temp.iloc[0]
        else:
            if df["timestamp"].values[first_row] == df["timestamp"].values[second_row]:
                df.iloc[second_row] = temp.iloc[0]
                df.iloc[third_row] = temp.iloc[1]
            else:
                df.iloc[third_row] = temp.iloc[0]

        print("Swapped rows", first_row, third_row)
    print("--------------------------------------------------")




    # incorrect_indices = sensor_df.index[incorrect_local_indices].to_list()
    # total_incorrect_indices.extend(incorrect_indices)
    # print("Incorrect indices local", incorrect_local_indices)
    # print("Incorrect indices", incorrect_indices)

    # perform row swaps
    # for i in range(0, len(incorrect_indices), 2):
    #     first_row, second_row = incorrect_indices[i], incorrect_indices[i+1]
    #     temp = df.iloc[first_row].copy()
    #     df.iloc[first_row] = df.iloc[second_row]
    #     df.iloc[second_row] = temp
        # print("Swapped rows", first_row, second_row)
    # sensor_df.to_csv(f"./data/sensor_{stv_num}.csv", index=True)

# STEP 2
# sort rows with the same timestamp based on sensor_id
# df = df.sort_values(by=["timestamp", "sensor_id"])

# STEP 3
# calculate sensor_id based on bit position

# make new column "sid" for calculated sensor_id
df["sid"] = None

def get_status_bin(row):
    return str(df["status_bin"].values[row])

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

    # print(f"{prev_status_bin}, {curr_status_bin}", changed_bit)

# compare the columns sensor_id and sid
sid_diff_arr = (df["sensor_id"] != df["sid"]).tolist()
sid_diff_indices = [i for i,x in enumerate(sid_diff_arr) if x is True]
print("Rows where calculated sensor_id does not match actual sensor_id:", sid_diff_indices)

# write
df.to_csv(f"./data/4f_sample_{stv_num}_edited.csv", index=False)
print("----- Wrote to csv -----")
