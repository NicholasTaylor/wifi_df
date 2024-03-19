import pandas as pd


def remove_last_and_return(str, sep):
    str_l = str.split(sep)
    str_l.pop(len(str_l) - 1)
    return sep.join(str_l)


def get_fn(fp):
    fn = fp.split("/")[-1]
    parent_path = remove_last_and_return(fp, "/")
    fn_no_ext = remove_last_and_return(fn, ".")
    return parent_path, fn, fn_no_ext


def read_plaintext(fp):
    import re

    with open(fp, "rb") as f:
        data_raw = f.read().decode("cp1252")
        data_pattern_ap = r"(RSSI: )(.*?)(Ch: )(.*?)(BSSID: )(.*?)(ESSID: )(.*)(\n)(Beacon: )(.*)"
        data_pattern_sta = (
            r"([0-9]*)(: )(ap: )([A-Za-z0-9:]*)( -> sta: )([A-Za-z0-9:]*)"
        )
        data_raw_ap_arr = re.findall(data_pattern_ap, data_raw)
        data_raw_sta_arr = re.findall(data_pattern_sta, data_raw)
        return data_raw_ap_arr, data_raw_sta_arr


def gen_lists_ap(data_raw_ap_arr):
    RSSI_all_ap, ch_all_ap, BSSID_all_ap, ESSID_all_ap, beacon_all_ap = (
        [] for i in range(5)
    )
    for record in data_raw_ap_arr:
        RSSI_all_ap.append(record[1].strip())
        ch_all_ap.append(record[3].strip())
        BSSID_all_ap.append(record[5].strip())
        ESSID_all_ap.append(record[7].strip())
        beacon_all_ap.append(record[10].strip())
    return RSSI_all_ap, ch_all_ap, BSSID_all_ap, ESSID_all_ap, beacon_all_ap


def gen_lists_sta(data_raw_sp_arr):
    id_all_sta, ap_all_sta, sta_all_sta = ([] for i in range(3))
    for record in data_raw_sp_arr:
        id_all_sta.append(record[0].strip())
        ap_all_sta.append(record[3].strip())
        sta_all_sta.append(record[5].strip())
    return id_all_sta, ap_all_sta, sta_all_sta


def gen_df_ap(
    RSSI_all_ap, ch_all_ap, BSSID_all_ap, ESSID_all_ap, beacon_all_ap
):
    data_dict = {
        "ESSID": ESSID_all_ap,
        "BSSID": BSSID_all_ap,
        "Ch": ch_all_ap,
        "RSSI": RSSI_all_ap,
        "Beacon": beacon_all_ap,
    }
    return pd.DataFrame.from_dict(data_dict)


def gen_df_sta(id_all_sta, ap_all_sta, sta_all_sta):
    import copy

    data_dict = {"ID": id_all_sta, "BSSID": ap_all_sta, "Station": sta_all_sta}
    return pd.DataFrame(data_dict)


def export_df(
    data_df_ap, data_df_sta, parent_path, fn_no_ext, ACCESS_POINTS, STATIONS
):
    out_path_stem = "%s/%s" % (parent_path, fn_no_ext)
    data_df_sta_xlsx = data_df_sta.merge(data_df_ap, how="left", on="BSSID")
    data_df_sta_xlsx.drop(["Ch", "RSSI", "Beacon"], axis=1, inplace=True)
    data_df_sta_xlsx.set_index("ID", drop=True)
    data_df_sta_xlsx.sort_values(by="ESSID", kind="mergesort", inplace=True)
    data_df_sta_xlsx = data_df_sta_xlsx[["ESSID", "BSSID", "Station"]]
    with pd.ExcelWriter(
        "%s.xlsx" % out_path_stem, mode="w+", engine="openpyxl"
    ) as writer:
        data_df_ap.to_excel(writer, sheet_name=ACCESS_POINTS)
        data_df_sta_xlsx.to_excel(writer, sheet_name=STATIONS)
    data_df_ap.to_parquet("%s_ap.parquet.gzip" % out_path_stem)
    data_df_sta.to_parquet("%s_sta.parquet.gzip" % out_path_stem)


if __name__ == "__main__":
    import sys

    ACCESS_POINTS = "Access Points"
    STATIONS = "Stations"
    fp = sys.argv[1]
    parent_path, fn, fn_no_ext = get_fn(fp)
    data_raw_ap_arr, data_raw_sta_arr = read_plaintext(fp)
    RSSI_all_ap, ch_all_ap, BSSID_all_ap, ESSID_all_ap, beacon_all_ap = (
        gen_lists_ap(data_raw_ap_arr)
    )
    id_all_sta, ap_all_sta, sta_all_sta = gen_lists_sta(data_raw_sta_arr)
    data_df_ap = gen_df_ap(
        RSSI_all_ap, ch_all_ap, BSSID_all_ap, ESSID_all_ap, beacon_all_ap
    )
    data_df_sta = gen_df_sta(id_all_sta, ap_all_sta, sta_all_sta)
    export_df(
        data_df_ap,
        data_df_sta,
        parent_path,
        fn_no_ext,
        ACCESS_POINTS,
        STATIONS,
    )
