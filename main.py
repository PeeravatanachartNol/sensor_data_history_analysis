import pandas as pd
from dicts import *

stv_num = 10
bit_to_sid = bit_to_sid_10 if stv_num == 10 else bit_to_sid_11

df = pd.read_csv(f"./data/4f_sample_{stv_num}.csv", encoding="ISO-8859-1")
df = df.sort_values(by=["timestamp", "sensor_id"])
# df.insert(0, "original_order", range(0, len(df)))

# ステップ 1
# [0,1,0,1,0, ...]　のパターンにならない異常の sensor_value を把握
# 正しいパターンになるまで swapする列のインデックスを取得
swap_indices = []
for bit, sid in bit_to_sid.items():
    sensor_df = df[df["sensor_id"] == f"{sid}"]
    sval_arr = sensor_df["sensor_value"].to_list()
    print(">>> Sensor ID", sid, "Bit", bit, "<<<")
    print("sensor_value START", sval_arr)


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
        print("sensor_value END", sval_arr)


    incorrect_indices = sensor_df.index[incorrect_local_indices].to_list()
    print("異常のあるメインインデックス", incorrect_indices)
    swap_indices.extend(incorrect_indices)
    print("--------------------------------------------------")

# 同じ timestamp の列も把握し、swapする列のインデックスに追加
print("Indices to swap, timestamp")
for i in range(len(swap_indices)):
    sensor_time = df.iloc[swap_indices[i]].timestamp
    swap_indices[i] = df[df.timestamp == sensor_time].index.to_list()
    print(swap_indices[i], sensor_time)

print(swap_indices)
print("--------------------------------------------------")

# ステップ 2
# 実際に列の swap を行う

# # swap using index
# def swap_rows(df, g1_rows, g2_rows):
#     print("Swapping rows", g1_rows, g2_rows)
#     df_g1 = df.iloc[g1_rows[0]:g1_rows[-1] + 1]
#     df_g2 = df.iloc[g2_rows[0]:g2_rows[-1] + 1]
#     df_beg = df.iloc[:g1_rows[0]]
#     df_mid = df.iloc[g1_rows[-1] + 1:g2_rows[0]]
#     df_end = df.iloc[g2_rows[-1] + 1:]
#     df_swapped = pd.concat([df_beg, df_g2, df_mid, df_g1, df_end], ignore_index=False)
#     return df_swapped

# swap using original_order
def swap_rows(df, g1_rows, g2_rows):
    df_g1 = df[(df.original_order >= g1_rows[0]) & (df.original_order < g1_rows[-1] + 1)]
    df_g2 = df[(df.original_order >= g2_rows[0]) & (df.original_order < g2_rows[-1] + 1)]
    df_beg = df[df.original_order < g1_rows[0]]
    df_mid = df[(df.original_order >= g1_rows[-1] + 1) & (df.original_order < g2_rows[0])]
    df_end = df[df.original_order >= g2_rows[-1] + 1]
    df_swapped = pd.concat([df_beg, df_g2, df_mid, df_g1, df_end], ignore_index=False)
    return df_swapped

df_swapped = df

for i in range(0, len(swap_indices), 2):
    df_swapped = swap_rows(df_swapped, swap_indices[i], swap_indices[i+1])

# ALSO FIND SOMETHING THAT WORKS HERE
# Sort rows by sensor_id locally within each timestamp group
# df_swapped = df_swapped.groupby('timestamp', group_keys=False).apply(lambda x: x.sort_values(by='sensor_id'))

def get_cumsum_from_sensor_id(df):
    # create column "timestamp_shifted" to shift timestamp value up by one
    df['timestamp_shift'] = df['timestamp'].shift(1)
    df['group'] = (df['timestamp'] != df['timestamp_shift']).cumsum()
    # df = df.drop(columns=['timestamp_shift'])
    df = df.sort_values(by=['group', 'sensor_id'])


# ステップ 3
# 実際の sensor_id と計算された sid を比較
# 一致したら、セーフ

# def get_sid(df):
#     df["sid"] = None
#     df["sid_mismatch"] = None
#     # find changed bit position(s)
#     changed_bit = []
#     for i in range(df.shape[0]):
#         # EDIT THIS TO IGNORE FIRST LINE SINCE FIRST STATUS MAY NOT ALWAYS BE 1111
#         prev_status_bin = "1111111111111111" if i==0 else get_status_bin(df_swapped, i-1)
#         curr_status_bin = get_status_bin(df_swapped, i)

#         # check different bit position
#         # START CHECKING FROM HERE I GUESS
#         changed_bit_local = []
#         for bit in range(16):
#             if curr_status_bin[bit] != prev_status_bin[bit]:
#                 if bit in bit_to_sid:
#                     changed_bit_local.append(bit_to_sid[bit])
#         print(i, changed_bit_local)

#         # insert changed position into sid column
#         changed_bit.append(changed_bit_local)
#     print("LLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLLL")
#     print(changed_bit[:60])
#     print(len(changed_bit), df.shape[0])
#     # for j in range(len(changed_bit)):
#     #     df.loc[i+j, "sid"] = changed_bit[j]


#         # print(f"{prev_status_bin}, {curr_status_bin}", changed_bit)

# def get_mismatch(df):
#     # compare the columns sensor_id and sid
#     mismatch_arr = (df["sensor_id"] != df["sid"]).to_list()
#     mismatch_indices = [i for i,x in enumerate(mismatch_arr) if x is True]
#     # print(mismatch_arr)
#     for j in range(len(mismatch_arr)):
#         if mismatch_arr[j]:
#             df["sid_mismatch"].values[j] = "Mismatch"

#     print(mismatch_indices)
#     return mismatch_indices

# print("-----------------------------------------------")
# print("sensor_id mismatch BEFORE")
# # get_sid(df)
# # get_mismatch(df)
# # print("number of mismatches", len(get_mismatch(df)))

# print("-----------------------------------------------")
# print("sensor_id mismatch AFTER")

# get_sid(df_swapped)
# get_mismatch(df_swapped)
# print("number of mismatches", len(get_mismatch(df_swapped)))

def get_status_bin(df, original_order):
    # return str(df.loc[df["original_order"] == original_order, "status_bin"])
    return str(df.loc[original_order, "status_bin"])

get_cumsum_from_sensor_id(df_swapped)

df_swapped["sid"] = None
df_swapped["sid_mismatch"] = None
df_swapped["idx_shift"] = None

changed_bit = []
for i in range(df_swapped.shape[0]):
    curr_timestamp = df_swapped.loc[i, "timestamp"]
    prev_timestamp = None if i==0 else df_swapped.loc[i-1, "timestamp"]
    # 3つ前の行のtimestampを取得
    idx_shift = 0
    for j in range(1, 4):
        if i - j >= 0:
            prev_timestamp = df_swapped.loc[i-j, "timestamp"]
            if curr_timestamp != prev_timestamp:
                idx_shift = j
                break

    prev_bin = "1111111111111111" if i==0 else str(df_swapped.loc[i-idx_shift, "status_bin"])
    curr_bin = str(df_swapped.loc[i, "status_bin"])
    df_swapped.loc[i, "prev_bin"] = prev_bin
    df_swapped.loc[i, "curr_bin"] = curr_bin

    changed_bit_local = []
    for bit in range(16):
        if prev_bin[bit] != curr_bin[bit]:
            if bit in bit_to_sid:
                changed_bit_local.append(bit_to_sid[bit])
    changed_bit.append(changed_bit_local)

    if changed_bit_local:
        df_swapped.loc[i, "sid"] = changed_bit[i][0]
        df_swapped.loc[i, "idx_shift"] = idx_shift
    # else:
    #     df_swapped.loc[i, "sid"] = None if i==0 else changed_bit[i-1][0]


    print(i, changed_bit[i])


    # if prev_bin == curr_bin:
    #     df_swapped.loc[i, "sid"] = changed_bit_local[1]

    # for j in range(len(changed_bit_local)):
        # if changed_bit_local:
        # df_swapped.loc[i+j, "sid"] = changed_bit_local[j]
    # if prev_timestamp != curr_timestamp:
    #     df_swapped.loc[i, "sid"] = changed_bit_local
    # else:
    #     df_swapped.loc[i, "sid"] = changed_bit_local[0]

    # if prev_sid != curr_sid:
    #     df_swapped.loc[i, "sid"] = "kekw"
# write
df_swapped.to_csv(f"./data/4f_sample_{stv_num}_edited.csv", index=True)
print("----- Wrote to csv -----")