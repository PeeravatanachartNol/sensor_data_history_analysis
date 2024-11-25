import pandas as pd
from dicts import *

# status_binの変更したbitを取得
def get_changed_bits(curr_bin, prev_bin, bit_to_sid):
    if len(curr_bin) != 16 or len(prev_bin) != 16:
        print("status_binの長さが16ではありません")
        return []
    
    changed_bits = []
    for i in range(16):
        if curr_bin[i] != prev_bin[i]:
            if i in bit_to_sid:
                changed_bits.append(bit_to_sid[i])
    changed_bits.sort()
    return changed_bits

# stvのステーション番号、dictsから"45566" <-> "10-1"の変換
station_num = 10
bit_to_sid = bit_to_sid_10 if station_num == 10 else bit_to_sid_11

df = pd.read_csv(f"./data/4f_sample_{station_num}.csv", encoding="ISO-8859-1")
df = df.sort_values(by=["timestamp", "sensor_id"])
df["status_bin"] = df["status_bin"].astype(int).astype(str)

# 計算用の列を追加
df["timestamp_shift"] = df["timestamp"].shift(1)
df["time_is_diff"] = df["timestamp"] != df["timestamp_shift"]
df["group"] = (df["timestamp"] != df["timestamp_shift"]).cumsum()
df["group_count"] = df["group"].map(df["group"].value_counts())

df["changed_bits"] = None
df["changed_bits_len"] = None
df["duplicate_count"] = None
df["remove_extra_rows"] = None
df["remove_all_rows"] = None
df["add_rows"] = None
df["sid"] = None

# time_is_diffがTrueの行をループし、csvにフラグを立てる
for index, row in df[df["time_is_diff"]].iterrows():
    curr_bin = str(df.loc[index, "status_bin"])
    # prev_bin = str("1111111111111111" if index==0 else df.loc[index-1, "status_bin"])
    # ループ中でindexが変わったため、loc の代わりに iloc を使う必要がある 
    prev_bin = str("1111111111111110" if index==0 else df.iloc[df.index.get_loc(index)-1]["status_bin"])
    changed_bits = get_changed_bits(curr_bin, prev_bin, bit_to_sid)
    
    row_diff = len(changed_bits) - row["group_count"]
    for j in range(row["group_count"]):
        df.loc[index+j, "changed_bits"] = str(changed_bits)

        # ケース１：センサーが一つも変わっていない場合は、行を削除するフラグを立てる
        if len(changed_bits) == 0:
            df.loc[index+j, "remove_all_rows"] = True
        # ケース２：行が多すぎる場合は、行を削除するフラグを立てる
        elif row_diff < 0:
            df.loc[index+j, "remove_extra_rows"] = True
            df.loc[index+j, "duplicate_count"] = j+1
            df.loc[index+j, "changed_bits_len"] = len(changed_bits)
        # ケース３：行が少なすぎる場合は、行を追加するフラグを立てる
        elif row_diff > 0:
            df.loc[index+j, "add_rows"] = True
            df.loc[index+j, "changed_bits_len"] = len(changed_bits)

# フラグから削除か追加かを決める
rows_to_drop = []
rows_to_add = []

for i in range(df.shape[0]):
    # ケース１
    if df.loc[i, "remove_all_rows"]:
        rows_to_drop.append(i)
    # ケース２
    elif df.loc[i, "remove_extra_rows"] and df.loc[i, "duplicate_count"] > df.loc[i, "changed_bits_len"]:
        rows_to_drop.append(i)
    # ケース３
    elif df.loc[i, "add_rows"] and df.loc[i, "group_count"] < df.loc[i, "changed_bits_len"]:
        add_row_count = df.loc[i, "changed_bits_len"] - df.loc[i, "group_count"]
        rows_to_add.append((df.loc[i, "original_order"], df.loc[i].copy()))

# 行を削除
df.drop(rows_to_drop, inplace=True)

# 行を追加
for i, row in reversed(rows_to_add):
    df_beg = df.loc[df["original_order"] <= i]
    df_end = df.loc[df["original_order"] > i]
    df_add = pd.DataFrame([row] * add_row_count)
    df = pd.concat([df_beg, df_add, df_end], ignore_index=True)

# 変更したセンサーIDを"sid"列に追加
for i in range(df.shape[0]):
    curr_bin = str(df.loc[i, "status_bin"])
    prev_bin = str("1111111111111110" if i==0 else df.loc[i-1, "status_bin"])

    changed_bits = get_changed_bits(curr_bin, prev_bin, bit_to_sid)
    for j in range(len(changed_bits)):
        df.loc[i+j, "sid"] = changed_bits[j]

df = df.drop(columns=["original_order", "timestamp_shift", "time_is_diff", "group", "group_count", "changed_bits", "changed_bits_len", "duplicate_count", "remove_extra_rows", "remove_all_rows", "add_rows"])
df.to_csv(f"./data/new.csv", index=False)