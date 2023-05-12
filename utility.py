import os
import pandas as pd
import zipfile


def process_nokia(file_path, app):
    zip_path = file_path  # path to your zip file
    # get the directory where the zip file is located
    zip_dir = os.path.dirname(zip_path)

    # extract all files to the zip directory
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(zip_dir)

    # get the path of the first extracted file
    extracted_file_path = os.path.join(zip_dir, zip_ref.namelist()[0])
    print("Extracted file path:", extracted_file_path)

    df = pd.read_csv(extracted_file_path)

    time_list = ["ALARM_TIME", "CANCEL_TIME"]

    try:
        if all(elem in list(df.columns) for elem in time_list):
            print("Proceeding On")
        else:
            print("Data must include ALARM_TIME and CANCEL_TIME columns")
            exit()
    except Exception as e:
        print("An error occurred while checking if List2 is a subset of List1:", e)

    df["ALARM_TIME"] = pd.to_datetime(df["ALARM_TIME"], dayfirst=True)
    df["CANCEL_TIME"] = pd.to_datetime(df["CANCEL_TIME"], dayfirst=True)
    df["Down_time"] = df["CANCEL_TIME"] - df["ALARM_TIME"]
    # group the data by Category column and calculate the sum of Value column
    sum_df = df.groupby("Name", as_index=False).agg(
        {"Down_time": "sum", "TEXT": "count"}
    )
    sum_df = sum_df.rename(
        columns={"Down_time": "Sum_Down_time", "TEXT": "Count_fluctuation"}
    )
    # calculate total seconds and convert to timedelta data type
    sum_df["Sum_Down_time"] = pd.to_timedelta(
        sum_df["Sum_Down_time"].dt.total_seconds(), unit="s"
    )

    # format timedelta values to [h]:mm:ss
    sum_df["Sum_Down_time"] = sum_df["Sum_Down_time"].apply(
        lambda x: str(int(x.total_seconds() // 3600)).zfill(2)
        + ":"
        + str(int(x.total_seconds() // 60 % 60)).zfill(2)
        + ":"
        + str(int(x.total_seconds() % 60)).zfill(2)
    )
    output_path = os.path.join(os.path.dirname(file_path), "nokia_output.xlsx")
    return sum_df.to_excel(output_path, index=False)


def process_huawei(file_path):
    # Load the Excel file as a DataFrame
    df = pd.read_excel(file_path, index_col=None)

    # Delete the first column
    df = df.iloc[:, 1:]
    # Remove empty rows
    df = df.dropna(how="all")
    # Construct the modified file path
    base_name, ext = os.path.splitext(file_path)
    modified_path = f"{base_name}_modified{ext}"

    # Save the modified DataFrame back to the modified file path
    df.to_excel(modified_path, index=False, header=False)

    print(f"Modified file saved at {modified_path}")
    # Load the modified Excel file as a DataFrame
    df = pd.read_excel(modified_path, index_col=None)
    new_columns = [
        "MO Name",
        "Name",
        "Occurred On (NT)",
        "Cleared On (NT)",
        "Clearance Status",
    ]
    df_selected = df.loc[:, new_columns]
    modified_cols_name = {
        "Occurred On (NT)": "alarm_time",
        "Cleared On (NT)": "cancel_time",
        "Clearance Status": "cancel_status",
    }
    df_selected = df_selected.rename(columns=modified_cols_name)
    df = df_selected[df_selected["cancel_status"] == "Cleared"]
    df.loc[:, "alarm_time"] = pd.to_datetime(df["alarm_time"])
    df.loc[:, "cancel_time"] = pd.to_datetime(df["cancel_time"])
    df.loc[:, "downtime"] = df["cancel_time"] - df["alarm_time"]
    # group the data by Category column and calculate the sum of Value column
    sum_df = df.groupby("MO Name", as_index=False).agg(
        {"downtime": "sum", "Name": "count"}
    )
    sum_df = sum_df.rename(
        columns={"downtime": "Sum_Down_time", "Name": "Count_fluctuation"}
    )
    # calculate total seconds and convert to timedelta data type
    sum_df["Sum_Down_time"] = pd.to_timedelta(
        sum_df["Sum_Down_time"].dt.total_seconds(), unit="s"
    )

    # format timedelta values to [h]:mm:ss
    sum_df["Sum_Down_time"] = sum_df["Sum_Down_time"].apply(
        lambda x: str(int(x.total_seconds() // 3600)).zfill(2)
        + ":"
        + str(int(x.total_seconds() // 60 % 60)).zfill(2)
        + ":"
        + str(int(x.total_seconds() % 60)).zfill(2)
    )
    sum_df["MO Name"] = sum_df["MO Name"].apply(lambda x: x[:8])
    sum_df = sum_df.rename(columns={"MO Name": "Name"})
    output_path = os.path.join(os.path.dirname(file_path), "huawei_output.xlsx")
    return sum_df.to_excel(output_path, index=False)
