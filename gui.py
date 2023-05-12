import tkinter as tk
from tkinter import filedialog
import zipfile
import os
import threading
import pandas as pd
from congestion_detect import *


class App(tk.Frame):
    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        self.master.title("Dump Processor")
        # to keep track of the threads
        self.running_threads = 0
        self.std_value = 2.8

        # Create a dropdown list to choose from 4 options
        self.label = tk.Label(self.master, text="Select an option:")
        self.label.pack()
        self.var = tk.StringVar(value="Select vendor")
        self.dropdown = tk.OptionMenu(
            self.master,
            self.var,
            "Nokia",
            "Huawei",
            "Both",
            "Congestion",
            command=self.update_gui,
        )
        self.dropdown.pack(pady=10)

        # Create a frame to hold the file selection buttons
        self.file_frame = tk.Frame(self.master)
        self.file_frame.pack(pady=10)

        # Create a button to select a zip file
        self.file_button = tk.Button(
            self.file_frame, text="Select Zip File", command=self.select_file
        )
        self.file_button.pack(side="left")

        # Create a button to select a second zip file (if "Both" is selected)
        self.file_button2 = tk.Button(
            self.file_frame, text="Select Second Zip File", command=self.select_file2
        )
        # Hide the button by default
        self.file_button2.pack_forget()

        # Create a section to display file processing information
        self.process_label = tk.Label(self.master, text="Processing Information:")
        self.process_label.pack()
        self.process_text = tk.Text(self.master, height=10, width=50)
        self.process_text.pack(pady=10)

        # Create a button to process the selected file(s)
        self.process_button = tk.Button(
            self.master, text="Process File", command=self.process_file
        )
        self.process_button.pack(pady=10)

        # Create a button to open the processed file
        self.open_button = tk.Button(
            self.master, text="Open Processed File", command=self.open_file
        )
        self.open_button.pack(pady=10)

        # Set up initial state of GUI
        self.update_gui()

    def update_gui(self, *args):
        # Update the GUI based on the selected option
        if self.var.get() == "Both":
            self.file_button.config(text="Select Zip File")
            self.file_button2.config(text="Select XLSX File")
            if hasattr(self, "file_button2"):
                self.file_button2.pack(side="left", padx=10, pady=10)
            else:
                self.file_button2 = tk.Button(
                    self.file_frame,
                    text="Select Second File",
                    command=self.select_file2,
                )
                self.file_button2.pack(side="left", padx=10, pady=10)
        elif self.var.get() == "Nokia":
            self.file_button.config(text="Select Zip File")
            if hasattr(self, "file_button2"):
                self.file_button2.pack_forget()
        elif self.var.get() == "Huawei":
            self.file_button.config(text="Select XLSX File")
            if hasattr(self, "file_button2"):
                self.file_button2.pack_forget()
        elif self.var.get() == "Congestion":
            self.file_button.config(text="Select CSV File")
            if hasattr(self, "file_button2"):
                self.file_button2.pack_forget()

        # Update the position of the processing information label and text
        self.process_label.pack(padx=10, pady=10)
        self.process_text.pack(padx=10, pady=10)

    def select_file(self):
        # Display a file selection window and get the selected file path
        if self.var.get() == "Nokia" or self.var.get() == "Both":
            file_path = filedialog.askopenfilename(filetypes=[("Zip Files", "*.zip")])
        elif self.var.get() == "Huawei":
            file_path = filedialog.askopenfilename(
                filetypes=[("Excel Files", "*.xlsx")]
            )
        elif self.var.get() == "Congestion":
            file_path = filedialog.askopenfilename(filetypes=[("CSV Files", "*.csv")])
        if file_path:
            self.file_path = file_path
            self.file_button.config(text="Selected: " + os.path.basename(file_path))

    def select_file2(self):
        # Display a file selection window and get the selected file path for the second file
        file_path = filedialog.askopenfilename(filetypes=[("Excel Files", "*.xlsx")])
        if file_path:
            self.file_button2.config(text="Selected: " + os.path.basename(file_path))
            self.file_path2 = file_path

    def process_file(self):
        # Process the selected file and display information in the processing section
        if self.var.get() == "Nokia" and hasattr(self, "file_path"):
            file_name = os.path.basename(self.file_path)
            self.process_text.insert(tk.END, "Processing " + file_name + "...\n")
            # Disable the process button while processing the file
            self.process_button.config(state=tk.DISABLED)
            # Start a new thread to process the Nokia file
            nok_t = threading.Thread(target=self.process_nokia)
            nok_t.start()
            # assign the no of running threads
            self.running_threads = 1

            self.monitor(nok_t)

        elif self.var.get() == "Huawei" and hasattr(self, "file_path"):
            file_name = os.path.basename(self.file_path)
            self.process_text.insert(tk.END, "Processing " + file_name + "...\n")
            # Disable the process button while processing the file
            self.process_button.config(state=tk.DISABLED)

            # Start a new thread to process the Nokia file
            huw_t = threading.Thread(target=self.process_huawei)
            huw_t.start()

            # assign the no of running threads
            self.running_threads = 1

            self.monitor(huw_t)
        elif self.var.get() == "Congestion" and hasattr(self, "file_path"):
            file_name = os.path.basename(self.file_path)
            self.process_text.insert(tk.END, "Processing " + file_name + "...\n")
            # Disable the process button while processing the file
            self.process_button.config(state=tk.DISABLED)

            # Start a new thread to process the Nokia file
            cong_t = threading.Thread(target=self.process_congestion)
            cong_t.start()

            # assign the no of running threads
            self.running_threads = 1

            self.monitor(cong_t)
        elif (
            self.var.get() == "Both"
            and hasattr(self, "file_path")
            and hasattr(self, "file_path2")
        ):
            file_name = os.path.basename(self.file_path)
            self.process_text.insert(tk.END, "Processing " + file_name + "...\n")
            file_name2 = os.path.basename(self.file_path2)
            self.process_text.insert(tk.END, "Processing " + file_name2 + "...\n")
            # Disable the process button while processing the file
            self.process_button.config(state=tk.DISABLED)

            # Start two new threads to process the Nokia file nd the Huawei file
            nok_t = threading.Thread(target=self.process_nokia)
            huw_t = threading.Thread(target=self.process_huawei)
            nok_t.start()
            huw_t.start()
            # assign the no of running threads
            self.running_threads = 2
            # monitor the threads
            self.monitor(nok_t, huw_t)

        else:
            self.process_text.insert(tk.END, "No file selected.")

    def open_file(self):
        # Open the processed file using the default system application
        if self.var.get() == "Nokia" and hasattr(self, "output_path_nok"):
            os.startfile(self.output_path_nok)
        elif self.var.get() == "Huawei" and hasattr(self, "output_path_huw"):
            os.startfile(self.output_path_huw)
        elif self.var.get() == "Congestion" and hasattr(self, "output_path_cong"):
            os.startfile(self.output_path_cong)
        elif (
            self.var.get() == "Both"
            and hasattr(self, "output_path_nok")
            and hasattr(self, "output_path_huw")
        ):
            df1 = pd.read_excel(self.output_path_nok)
            df2 = pd.read_excel(self.output_path_huw)
            frames = [df1, df2]
            result_df = pd.concat(frames)
            output_path_combine = os.path.join(
                os.path.dirname(self.file_path), "combine_output.xlsx"
            )
            result_df.to_excel(output_path_combine, index=False)
            os.startfile(output_path_combine)
        else:
            self.process_text.insert(tk.END, "No file selected. \n")

    def process_nokia(self):
        zip_path = self.file_path  # path to your zip file
        # get the directory where the zip file is located
        zip_dir = os.path.dirname(zip_path)

        # extract all files to the zip directory
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(zip_dir)

        # get the path of the first extracted file
        extracted_file_path = os.path.join(zip_dir, zip_ref.namelist()[0])
        df = pd.read_csv(extracted_file_path)
        time_list = ["ALARM_TIME", "CANCEL_TIME"]
        try:
            if all(elem in list(df.columns) for elem in time_list):
                print("proceeding on")
            else:
                exit()
        except Exception as e:
            print(f"An error occurred while checking if List2 is a subset of List1:{e}")

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
        self.output_path_nok = os.path.join(
            os.path.dirname(self.file_path), "nokia_output.xlsx"
        )
        sum_df.to_excel(self.output_path_nok, index=False)

    def process_huawei(self):
        if self.var.get() == "Both":
            file_path = self.file_path2
        else:
            file_path = self.file_path
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
        sum_df["Sum_Down_time"] = pd.to_timedelta(sum_df["Sum_Down_time"])
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
        self.output_path_huw = os.path.join(
            os.path.dirname(file_path), "huawei_output.xlsx"
        )
        sum_df.to_excel(self.output_path_huw, index=False)

    def process_congestion(self):
        file_path = self.file_path
        input_file = file_path

        # Construct the modified file path
        base_name, ext = os.path.splitext(file_path)
        out_file = f"{base_name}_tream{ext}"
        tream_file = clean_raw_data(input_file, out_file)
        df_cong = pd.read_csv(tream_file)

        try:
            df = prepare_dataframe(df_cong)
        except ValueError as e:
            print(f"there is an error {e}")

        # file paths for clean and congestion result file
        clean_path = os.path.join(os.path.dirname(out_file), "clean_data.xlsx")
        self.output_path_cong = os.path.join(
            os.path.dirname(out_file), "congestion_data.xlsx"
        )

        df.to_excel(clean_path, index=False)
        result_list = congestion_calculation(df, self.std_value)
        out_result = pd.DataFrame(result_list)
        out_result.to_excel(self.output_path_cong, index=False)

    def monitor(self, *threads):
        still_running = False
        for t in threads:
            if t.is_alive():
                still_running = True
                break

        if still_running:
            self.master.after(100, lambda: self.monitor(*threads))
        else:
            self.process_text.insert(tk.END, "Processing complete. \n")
            # Enable the process button
            self.process_button.config(state=tk.NORMAL)
            self.running_threads = 0


root = tk.Tk()
app = App(master=root)
app.mainloop()
