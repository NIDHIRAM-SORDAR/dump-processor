import pandas as pd
import os
import numpy as np
from scipy import stats
from scipy.signal import find_peaks
import errno


def parse_raw_file(input_file):
    # This a function for parsing file
    with open(input_file, "r", encoding="utf-8") as read_file:
        flag = True
        token = '"Resource Name"'
        BOM = "\ufeff"
        for line in read_file:
            if line.strip().startswith(token) and flag:
                yield line
                flag = False
            elif line.startswith('"') or line in ["\n"] or line.startswith(BOM):
                continue
            else:
                yield line


def write_to_file(input_file, output_file):
    with open(output_file, "w") as write_file:
        for line in parse_raw_file(input_file):
            write_file.write(line)


def clean_raw_data(input_file, out_file):
    with open(input_file, "r", encoding="utf-8") as read_file, open(
        out_file, "w"
    ) as write_file:
        flag = True
        token = '"Resource Name"'
        BOM = "\ufeff"
        for line in read_file:
            if line.strip().startswith(token) and flag:
                write_file.write(line)
                flag = False
            elif line.startswith('"') or line in ["\n"] or line.startswith(BOM):
                continue
            else:
                write_file.write(line)
    return out_file


def check_required_col(target, source):
    if set(target).issubset(set(source)):
        return True
    else:
        raise ValueError("Column name is inconsisted")


def prepare_dataframe(df):
    df_mod = df.copy()
    column_names = list(df_mod.columns)
    required_cols = [
        "Resource Name",
        "Collection Time",
        "Inbound Peak(bit/s)",
        "Outbound Peak(bit/s)",
    ]
    modified_col_names = [
        "site_name",
        "collection_time",
        "inbound_peak_rate",
        "outbound_peak_rate",
    ]
    col_names_dict = dict(zip(required_cols, modified_col_names))
    try:
        if check_required_col(required_cols, column_names):
            df_mod = df_mod[required_cols]
            df_mod.rename(columns=col_names_dict, inplace=True)

            def unit_conversion(x):
                if x[-1] == "K":
                    x = str("{:.4f}".format(float(x[:-1]) / 1000)) + "M"
                    return x
                else:
                    return x

            def unit_check(x):
                if str(x[-1]) not in ("M", "K"):
                    return True
                else:
                    return False

            df_mod.drop(
                df_mod[df_mod["inbound_peak_rate"].apply(unit_check)].index,
                inplace=True,
            )
            df_mod.drop(
                df_mod[df_mod["outbound_peak_rate"].apply(unit_check)].index,
                inplace=True,
            )

            df_mod.loc[:, "inbound_peak_rate"] = df_mod.loc[
                :, "inbound_peak_rate"
            ].apply(unit_conversion)
            df_mod.loc[:, "outbound_peak_rate"] = df_mod.loc[
                :, "outbound_peak_rate"
            ].apply(unit_conversion)

            df_mod.loc[:, "site_name"] = df_mod.loc[:, "site_name"].map(lambda x: x[:7])
            df_mod.loc[:, "inbound_peak_rate"] = df_mod.loc[:, "inbound_peak_rate"].map(
                lambda x: x[:-1]
            )
            df_mod.loc[:, "outbound_peak_rate"] = df_mod.loc[
                :, "outbound_peak_rate"
            ].map(lambda x: x[:-1])

            df_mod.drop(df_mod[df_mod["inbound_peak_rate"] == "-"].index, inplace=True)
            df_mod.drop(df_mod[df_mod["outbound_peak_rate"] == "-"].index, inplace=True)

            df_mod = df_mod.astype(
                {
                    "site_name": "object",
                    "collection_time": "datetime64[ns]",
                    "inbound_peak_rate": "float64",
                    "outbound_peak_rate": "float64",
                }
            )
            return df_mod
    except ValueError:
        raise


def congestion_calculation(df, std_value):
    result_list = []
    grouped_df = df.groupby("site_name")
    for site in grouped_df.groups.keys():
        result_dict = {}
        data_as_time_index = grouped_df.get_group(site)[
            ["collection_time", "inbound_peak_rate"]
        ].set_index("collection_time")
        max_consumption = data_as_time_index["inbound_peak_rate"].max()
        avg_consumption = data_as_time_index["inbound_peak_rate"].mean()
        min_consumption = data_as_time_index["inbound_peak_rate"].min()
        # Height parameter is included for peaks function
        # discarded prominence=1 parameter
        peaks, _ = find_peaks(
            data_as_time_index["inbound_peak_rate"].values,
            height=(max_consumption - 12, max_consumption),
        )
        data_as_time_index["max"] = False
        data_as_time_index.loc[data_as_time_index.iloc[peaks].index, "max"] = True
        # Convert peaks_consumption series to df
        peaks_consumption = data_as_time_index[data_as_time_index["max"]].loc[
            :, ["inbound_peak_rate"]
        ]
        # Convert median_of_peaks series to df
        median_of_peaks = (
            data_as_time_index[data_as_time_index["max"]]
            .loc[:, ["inbound_peak_rate"]]
            .groupby(pd.Grouper(level=0, axis=0, freq="D"))
            .median()
        )  # Change mean to median
        # Drop any column with NaN bcz it will mess with z score
        median_of_peaks.dropna(inplace=True)
        # calculate zscore of median peaks
        zscore_median_peaks = np.abs(stats.zscore(median_of_peaks["inbound_peak_rate"]))
        # drop the median peaks which zcore is greater than 2
        median_of_peaks = median_of_peaks[zscore_median_peaks <= 2]
        # as median_of_peaks is now dataframe we have to select series to list
        median_of_peaks_list = median_of_peaks["inbound_peak_rate"].tolist()
        median_of_peaks_list.append(max_consumption)
        median_of_peaks_ndarray = np.array(median_of_peaks_list)
        std = np.std(median_of_peaks_ndarray)
        # take account anomaly site
        # consumption is too low
        # avg_consumption taken into consideration
        if std <= std_value and avg_consumption >= 6:
            result_dict["Site_name"] = site
            result_dict["STD"] = std
            result_dict["Max_Consumption"] = max_consumption
            result_dict["Average_Consumption"] = avg_consumption
            result_dict["Minimum_Consumption"] = min_consumption
            result_dict["Congestion Status"] = "Congested"
        else:
            result_dict["Site_name"] = site
            result_dict["STD"] = std
            result_dict["Max_Consumption"] = max_consumption
            result_dict["Average_Consumption"] = avg_consumption
            result_dict["Minimum_Consumption"] = min_consumption
            result_dict["Congestion Status"] = "Not Congested"
        result_list.append(result_dict)
    return result_list


def silentremove(filename):
    try:
        os.remove(filename)
    except OSError as e:  # this would be "except OSError, e:" before Python 2.6
        if e.errno != errno.ENOENT:  # errno.ENOENT = no such file or directory
            raise  # re-raise exception if a different error occurred


if __name__ == "__main__":
    std_value = 2.8
    input_file = r"C:\Users\Asus\Downloads\atn_data\logs.csv"
    out_file = r"C:\Users\Asus\Downloads\atn_data\tream.csv"
    tream_file = clean_raw_data(input_file, out_file)
    df = pd.read_csv(tream_file)
    try:
        df = prepare_dataframe(df)
    except ValueError as e:
        print(f"there is an error {e}")
    print(df.head())
    output_path = os.path.join(os.path.dirname(out_file), "clean_data.xlsx")
    clean_path = os.path.join(os.path.dirname(out_file), "congestion_data.xlsx")
    df.to_excel(output_path, index=False)
    result_list = congestion_calculation(df, std_value)
    out_result = pd.DataFrame(result_list)
    out_result.to_excel(clean_path, index=False)
