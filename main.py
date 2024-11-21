import pandas as pd
from dicts import *

stv_num = 10
bit_to_sid = bit_to_sid_10 if stv_num == 10 else bit_to_sid_11

df = pd.read_csv(f"./data/4f_sample_{stv_num}.csv", encoding="ISO-8859-1")
df = df.sort_values(by=["timestamp", "sensor_id"])

swap_indices = []
for bit, sid in bit_to_sid.items():
    sensor_df = df[df["sensor_id"] == f"{sid}"]
    sval_arr = sensor_df["sensor_value"].to_list()
    print(">>> Sensor ID", sid, "Bit", bit, "<<<")
    # print("sensor_value", sval_arr)

    # STEP 1
    # [0,1,0,1,0, ...]　のパターンにならないに異常の sensor_value を把握
    # [0,1,0,1,0, ...]　になるまで swap
    incorrect_local_indices = [] # 各センサーのインデックス
    swapped = True
    while swapped:
        swapped = False
        for i in range(len(sval_arr) - 1): #　最後の列は比較しない
            prev_sval = 1 if i==0 else sval_arr[i-1]

            if sval_arr[i] == prev_sval:
                incorrect_local_indices.append(i)
                incorrect_local_indices.append(i+1)
                sval_arr[i], sval_arr[i+1] = sval_arr[i+1], sval_arr[i]
                # print("Swapしたセンサーのインデックス", i, i+1)
                swapped = True

    # CHECK THIS AGAIN
    incorrect_indices = sensor_df.index[incorrect_local_indices].to_list()
    print("異常のあるメインインデックス", incorrect_indices)
    swap_indices.extend(incorrect_indices)
    print("--------------------------------------------------")

def swap_rows(df, g1_rows, g2_rows):
    df_g1 = df.iloc[g1_rows[0]:g1_rows[-1] + 1]
    df_g2 = df.iloc[g2_rows[0]:g2_rows[-1] + 1]
    df_beg = df.iloc[:g1_rows[0]]
    df_mid = df.iloc[g1_rows[-1] + 1:g2_rows[0]]
    df_end = df.iloc[g2_rows[-1] + 1:]
    df_swapped = pd.concat([df_beg, df_g2, df_mid, df_g1, df_end], ignore_index=False)
    return df_swapped

# check rows with the same timestamp
# if there is, swap that entry as well
print("Indices to swap, timestamp")
for i in range(len(swap_indices)):
    sensor_time = df.iloc[swap_indices[i]].timestamp
    swap_indices[i] = df[df.timestamp == sensor_time].index.to_list()
    print(swap_indices[i], sensor_time)

print(swap_indices)
print("--------------------------------------------------")
df_swapped = df

# STEP 2
# swap rows, comment these two lines out to see differences
for i in range(0, len(swap_indices), 2):
    df_swapped = swap_rows(df_swapped, swap_indices[i], swap_indices[i+1])

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

# sort rows with the same timestamp based on sensor_id
# df = df.sort_values(by=["timestamp", "sensor_id"])

# STEP 3
# calculate sensor_id based on bit position
# make new column "sid" for calculated sensor_id

def get_status_bin(row):
    return str(df_swapped["status_bin"].values[row])

def get_sid(df):
    df["sid"] = None
    df["sid_mismatch"] = None
    # find changed bit position(s)
    for i in range(df.shape[0]):
        # EDIT THIS TO IGNORE FIRST LINE SINCE FIRST STATUS MAY NOT ALWAYS BE 1111
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
    # print(sid_diff_arr)
    for j in range(len(sid_diff_arr)):
        if sid_diff_arr[j]:
            df["sid_mismatch"].values[j] = "Mismatch"

    print(sid_diff_indices)

print("sensor_id mismatch BEFORE")
get_sid(df)
print("sensor_id mismatch AFTER")
get_sid(df_swapped)

# write
df_swapped.to_csv(f"./data/4f_sample_{stv_num}_edited.csv", index=True)
print("----- Wrote to csv -----")