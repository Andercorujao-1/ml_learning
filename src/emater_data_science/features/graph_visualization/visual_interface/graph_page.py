# src/emater_data_science/features/graph_visualization/visual_interface/graph_page.py
import tkinter as tk
from tkinter import ttk
from tkinter import messagebox

import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from datetime import datetime
import polars as pl

from emater_data_science.features.graph_visualization.visual_interface.graph_settings_modal import (
    GraphSettingsModal,
)


class GraphPage(ttk.Frame):
    def __init__(self, controller, parent):
        super().__init__(parent)
        self.controller = controller
        self.canvas = None
        self.figure = None
        self.build_ui()
        # Subscribe to graph settings changes.
        self.controller.graphSettings.fAddObserver(self.update_graph)

    def build_ui(self):
        # Button to open the graph settings modal.
        settings_button = ttk.Button(
            self, text="Graph Settings", command=self.open_settings_modal
        )
        settings_button.pack(side=tk.TOP, pady=5)
        self.graph_container = ttk.Frame(self)
        self.graph_container.pack(fill=tk.BOTH, expand=True)
        self.plot_graph()

    def open_settings_modal(self):
        df = self.controller.loadedTableData
        if df is None:
            messagebox.showerror("Error", "No data loaded.")
            return
        # Get all column names from the DataFrame.
        available_columns = list(df.columns)
        # Open the modal, passing the current settings, available columns, and table data.
        GraphSettingsModal(
            self,
            self.controller.graphSettings.fGetValue(),
            self.on_settings_applied,
            available_columns,
            df,
        )

    def on_settings_applied(self):
        self.controller.graphSettings.fNotifyObservers()

    def plot_graph(self):
        # Remove old canvas if it exists.
        if self.canvas:
            self.canvas.get_tk_widget().destroy()

        df = self.controller.loadedTableData
        if df is None:
            logger.debug("No data loaded for graph.")
            return

        settings = self.controller.graphSettings.fGetValue()

        # Filter the data based on start_date and end_date.
        try:
            df_filtered = df.filter(
                (
                    pl.col(settings.date_column)
                    >= settings.start_date.strftime("%Y-%m-%d")
                )
                & (
                    pl.col(settings.date_column)
                    <= settings.end_date.strftime("%Y-%m-%d")
                )
            )
        except Exception as e:
            logger.exception(f"Error filtering data by date: {e}")
            df_filtered = df

        try:
            dates = [
                datetime.strptime(d, "%Y-%m-%d")
                for d in df_filtered[settings.date_column]
            ]
        except Exception as e:
            logger.exception(f"Error converting dates: {e}")
            dates = []

        try:
            y_values = df_filtered[settings.value_column].to_list()
        except Exception as e:
            logger.exception(
                f"Error retrieving values from column '{settings.value_column}': {e}"
            )
            y_values = []

        # Create a new matplotlib figure.
        self.figure, ax = plt.subplots(figsize=(8, 6))
        ax.plot(dates, y_values, marker="o", linestyle="-")
        ax.set_xlabel(settings.date_column)
        ax.set_ylabel(settings.value_column)
        ax.set_title("Time Series Graph")
        if settings.y_min is not None and settings.y_max is not None:
            ax.set_ylim(settings.y_min, settings.y_max)
        ax.grid(True)

        self.canvas = FigureCanvasTkAgg(self.figure, master=self.graph_container)
        self.canvas.draw()
        self.canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    def update_graph(self, _=None):
        self.plot_graph()
