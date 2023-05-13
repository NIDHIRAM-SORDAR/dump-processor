import tkinter as tk
import tkinter.font as font
from tkinter import ttk
from tkinter import filedialog
import zipfile
import os
import threading
import pandas as pd
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
from congestion_detect import *


class App(tk.Frame):
    def __init__(self, master=None):
        super().__init__(master)
        self.master = master
        self.master.title("Dump Processor")
        self.master.iconbitmap("database-solid.ico")
        # to keep track of the threads
        self.running_threads = 0
        self.std_value = 2.8
        self.df_graph = None

        # Define a custom font with bold weight
        bold_font = font.Font(family="Helvetica", size=11, weight="bold")
        button_font = font.Font(family="Segoe UI", size=12, weight="normal")
        text_box_font = font.Font(family="Arial", size=13)
        processed_file_label_font = font.Font(
            family="Helvetica", size=11, weight="bold"
        )

        # Create a dropdown list to choose from 4 options
        self.label = tk.Label(self.master, text="Select an Option")
        self.label.configure(font=bold_font)
        self.label.pack()
        self.var = tk.StringVar(value="Select Vendor")
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
            self.file_frame,
            text="Select File",
            font=button_font,
            command=self.select_file,
        )
        self.file_button.pack(side="left")

        # Create a button to select a second zip file (if "Both" is selected)
        self.file_button2 = tk.Button(
            self.file_frame,
            text="Select Second File",
            font=button_font,
            command=self.select_file2,
        )
        # Hide the button by default
        self.file_button2.pack_forget()

        # Create a section to set STD value
        self.std_frame = tk.Frame(self.master)
        self.std_frame.pack(pady=10)
        self.std_label = tk.Label(self.std_frame, text="Enter STD Value:")
        self.std_label.pack(side="left")
        self.std_entry = tk.Entry(self.std_frame)
        self.std_entry.pack(side="left", padx=10)
        self.std_button = tk.Button(
            self.std_frame, text="Set STD", font=button_font, command=self.set_std_value
        )
        self.std_button.pack(side="left", padx=10)

        # Hide the frame by default
        self.std_frame.pack_forget()
        # Create a section to display file processing information
        self.process_label = tk.Label(self.master, font=bold_font, text="Dashboard")
        self.process_label.pack()
        self.process_text = tk.Text(
            self.master, height=10, width=50, font=text_box_font
        )
        self.process_text.pack(pady=10)

        # Create a button to process the selected file(s)
        self.process_button = tk.Button(
            self.master,
            text="Process File",
            font=button_font,
            command=self.process_file,
        )
        self.process_button.pack(pady=10)

        # Create A frame to hold procssed file button and label
        self.processed_frame = tk.Frame(self.master)
        self.processed_frame.pack(pady=10)

        # Create a button to open the processed file
        self.open_button = tk.Button(
            self.processed_frame,
            text="Open Processed File",
            font=button_font,
            command=self.open_file,
        )
        self.open_button.pack(side="left", padx=10)
        # Create a label to show processed file path
        self.procssed_file_label = tk.Label(
            self.processed_frame, text="No file available yet"
        )
        self.procssed_file_label.config(
            font=processed_file_label_font,
            borderwidth=2,
            relief="ridge",
            padx=5,
        )
        self.procssed_file_label.pack(side="left", padx=10)

        # Create a button to show graph and initially hide it
        self.graph_button = tk.Button(
            self.master,
            text="View BW Graph",
            font=button_font,
            command=self.show_graph,
        )
        # Hide the button by default
        self.graph_button.pack_forget()
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
            if self.check_frame_status(self.std_frame):
                self.std_frame.pack_forget()
            if self.check_frame_status(self.graph_button):
                self.graph_button.pack_forget()
        elif self.var.get() == "Nokia":
            self.file_button.config(text="Select Zip File")
            if hasattr(self, "file_button2"):
                self.file_button2.pack_forget()
            if self.check_frame_status(self.std_frame):
                self.std_frame.pack_forget()
            if self.check_frame_status(self.graph_button):
                self.graph_button.pack_forget()
        elif self.var.get() == "Huawei":
            self.file_button.config(text="Select XLSX File")
            if hasattr(self, "file_button2"):
                self.file_button2.pack_forget()
            if self.check_frame_status(self.std_frame):
                self.std_frame.pack_forget()
            if self.check_frame_status(self.graph_button):
                self.graph_button.pack_forget()
        elif self.var.get() == "Congestion":
            self.file_button.config(text="Select CSV File")
            if hasattr(self, "file_button2"):
                self.file_button2.pack_forget()
            if not self.check_frame_status(self.std_frame):
                self.std_frame.pack(after=self.file_frame)

        # elif self.df_graph is not None:
        #     if not self.check_frame_status(self.graph_button):
        #         self.graph_button.pack(pady=10)

        # Update the position of the processing information label and text
        self.process_label.pack(padx=10, pady=10)
        self.process_text.pack(padx=10, pady=10)

    def check_frame_status(self, frame_name):
        if frame_name.winfo_manager() == "pack":
            return True
        else:
            return False

    def path_joiner(self, path1, path2):
        return os.path.normpath(os.path.join(path1, path2))

    def show_graph(self):
        self.new_window = tk.Toplevel(self.master)
        self.new_window.title("Site BW Utilization")

        # Create a frame for the first column
        self.combobox_frame = tk.Frame(self.new_window)
        self.combobox_frame.grid(row=0, column=0, sticky="nsew")

        # Create a frame for the second column
        self.graph_frame = tk.Frame(self.new_window)
        self.graph_frame.grid(row=0, column=1, sticky="nsew")

        # Set the size of the columns
        self.new_window.grid_columnconfigure(0, weight=1)
        self.new_window.grid_columnconfigure(1, weight=1)

        # Add label before combobox
        self.combo_label = tk.Label(self.combobox_frame, text="Select Site Name")
        self.combo_label.pack(side=tk.LEFT, padx=10, pady=10)

        # create a comboBOX

        self.options = list(self.df_graph.groupby("site_name").groups.keys())
        self.combobox = ttk.Combobox(
            self.combobox_frame,
            values=self.options,
            state="readonly",
        )
        self.combobox.current(0)  # Set the initial value to "Option 1"
        self.combobox.pack(side=tk.LEFT, padx=5)

        # Bind the "<<ComboboxSelected>>" event to a callback function
        self.combobox.bind("<<ComboboxSelected>>", self.draw_graph)

        # Create a figure and a subplot
        fig = Figure(figsize=(5, 4), dpi=100)
        self.ax = fig.add_subplot(111)
        # Create a canvas and display the graph in it
        self.canvas = FigureCanvasTkAgg(fig, master=self.graph_frame)
        self.canvas.draw()
        self.canvas_widget = self.canvas.get_tk_widget()
        self.canvas_widget.pack(fill=tk.BOTH, expand=True)

        # Add quit button
        self.quit_button = tk.Button(
            self.graph_frame,
            text="Close",
            font=font.Font(family="Segoe UI", size=12, weight="normal"),
            command=self.new_window.destroy,
        )
        self.quit_button.pack(
            side=tk.BOTTOM,
            padx=10,
            pady=10,
            fill=tk.X,
            expand=True,
        )

    def draw_graph(self, event):
        site = str(self.combobox.get())
        grouped_df = self.df_graph.groupby("site_name")
        data_as_time_index = grouped_df.get_group(site)[
            ["collection_time", "inbound_peak_rate"]
        ].set_index("collection_time")
        # Clear existing graph
        self.ax.clear()

        # Plot data
        self.ax.plot(
            data_as_time_index["inbound_peak_rate"],
        )

        # Update canvas
        self.canvas.draw()

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

    def set_std_value(self):
        try:
            value = float(self.std_entry.get())
            if value:
                self.std_value = value
        except ValueError:
            self.process_text.insert(tk.END, "Invalid Input. \n")

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
            os.startfile(self.output_path_combine)
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
        self.output_path_nok = self.path_joiner(
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
        self.output_path_huw = self.path_joiner(
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
            self.df_graph = prepare_dataframe(df_cong)
        except ValueError as e:
            print(f"there is an error {e}")

        # file paths for clean and congestion result file
        clean_path = self.path_joiner(os.path.dirname(out_file), "clean_data.xlsx")
        self.output_path_cong = self.path_joiner(
            os.path.dirname(out_file), "congestion_data.xlsx"
        )

        self.df_graph.to_excel(clean_path, index=False)
        result_list = congestion_calculation(self.df_graph, self.std_value)
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
            if self.var.get() == "Nokia":
                self.procssed_file_label.config(text=str(self.output_path_nok))
            elif self.var.get() == "Huawei":
                self.procssed_file_label.config(text=str(self.output_path_huw))
            elif self.var.get() == "Congestion":
                self.procssed_file_label.config(text=str(self.output_path_cong))
                if not self.check_frame_status(self.graph_button):
                    self.graph_button.pack(pady=10)
            else:
                if hasattr(self, "output_path_nok") and hasattr(
                    self, "output_path_huw"
                ):
                    df1 = pd.read_excel(self.output_path_nok)
                    df2 = pd.read_excel(self.output_path_huw)
                    frames = [df1, df2]
                    result_df = pd.concat(frames)
                    self.output_path_combine = self.path_joiner(
                        os.path.dirname(self.file_path), "combine_output.xlsx"
                    )
                    result_df.to_excel(self.output_path_combine, index=False)
                self.procssed_file_label.config(text=str(self.output_path_combine))


root = tk.Tk()
app = App(master=root)
app.mainloop()
