# src/emater_data_science/features/graph_visualization/visual_interface/graph_settings_modal.py
import tkinter as tk
from tkinter import ttk, messagebox
from datetime import datetime

from emater_data_science.features.graph_visualization.graph_settings import (
    GraphSettings,
)


class GraphSettingsModal(tk.Toplevel):
    def __init__(
        self,
        parent,
        settings: GraphSettings,
        on_apply_callback,
        available_columns: list,
        table_data,
    ):
        """
        :param parent: Parent window.
        :param settings: Current GraphSettings instance.
        :param on_apply_callback: Function to call after applying new settings.
        :param available_columns: List of all column names from the loaded table.
        :param table_data: Polars DataFrame of the loaded table.
        """
        super().__init__(parent)
        self.settings = settings
        self.on_apply_callback = on_apply_callback
        self.table_data = table_data  # Polars DataFrame
        self.title("Graph Settings")

        # Ensure start_date and end_date are set by updating from table_data if needed.
        if not self.settings.start_date or not self.settings.end_date:
            try:
                self.settings.update_from_data(table_data)
            except Exception as e:
                logger.exception("Error updating graph settings from table data: {}", e)

        self.build_ui(available_columns)

    def build_ui(self, available_columns: list):
        # Filter out the date column from the available columns.
        value_columns = [
            col for col in available_columns if col != self.settings.date_column
        ]
        row = 0

        # --- Value Column (Dropdown) ---
        ttk.Label(self, text="Value Column:").grid(
            row=row, column=0, sticky="w", padx=5, pady=5
        )
        self.value_var = tk.StringVar(value=self.settings.value_column)
        self.value_combobox = ttk.Combobox(
            self, textvariable=self.value_var, values=value_columns, state="readonly"
        )
        self.value_combobox.grid(row=row, column=1, sticky="ew", padx=5, pady=5)
        self.value_combobox.bind("<<ComboboxSelected>>", self.on_value_column_changed)
        row += 1

        # --- Start Date ---
        ttk.Label(self, text="Start Date (YYYY-MM-DD):").grid(
            row=row, column=0, sticky="w", padx=5, pady=5
        )
        self.start_date_entry = ttk.Entry(self)
        start_date_str = (
            self.settings.start_date.strftime("%Y-%m-%d")
            if self.settings.start_date
            else ""
        )
        self.start_date_entry.insert(0, start_date_str)
        self.start_date_entry.grid(row=row, column=1, sticky="ew", padx=5, pady=5)
        row += 1

        # --- End Date ---
        ttk.Label(self, text="End Date (YYYY-MM-DD):").grid(
            row=row, column=0, sticky="w", padx=5, pady=5
        )
        self.end_date_entry = ttk.Entry(self)
        end_date_str = (
            self.settings.end_date.strftime("%Y-%m-%d")
            if self.settings.end_date
            else ""
        )
        self.end_date_entry.insert(0, end_date_str)
        self.end_date_entry.grid(row=row, column=1, sticky="ew", padx=5, pady=5)
        row += 1

        # --- Y-axis Min ---
        ttk.Label(self, text="Y-axis Min:").grid(
            row=row, column=0, sticky="w", padx=5, pady=5
        )
        self.y_min_entry = ttk.Entry(self)
        self.y_min_entry.insert(
            0, str(self.settings.y_min) if self.settings.y_min is not None else ""
        )
        self.y_min_entry.grid(row=row, column=1, sticky="ew", padx=5, pady=5)
        row += 1

        # --- Y-axis Max ---
        ttk.Label(self, text="Y-axis Max:").grid(
            row=row, column=0, sticky="w", padx=5, pady=5
        )
        self.y_max_entry = ttk.Entry(self)
        self.y_max_entry.insert(
            0, str(self.settings.y_max) if self.settings.y_max is not None else ""
        )
        self.y_max_entry.grid(row=row, column=1, sticky="ew", padx=5, pady=5)
        row += 1

        # --- Apply Button ---
        apply_button = ttk.Button(self, text="Apply", command=self.apply_settings)
        apply_button.grid(row=row, column=0, columnspan=2, pady=10)

    def on_value_column_changed(self, event):
        new_value_column = self.value_var.get()
        try:
            # Recalculate y-axis min and max based on the selected value column from table_data.
            col_values = self.table_data[new_value_column].to_list()
            new_y_min = min(col_values)
            new_y_max = max(col_values)
            # Update the y-axis entries with the new values.
            self.y_min_entry.delete(0, tk.END)
            self.y_min_entry.insert(0, str(new_y_min))
            self.y_max_entry.delete(0, tk.END)
            self.y_max_entry.insert(0, str(new_y_max))
        except Exception as e:
            logger.exception(f"Error recalculating y-axis limits: {e}")

    def apply_settings(self):
        try:
            new_value_column = self.value_var.get().strip()
            start_date_str = self.start_date_entry.get().strip()
            end_date_str = self.end_date_entry.get().strip()
            y_min_str = self.y_min_entry.get().strip()
            y_max_str = self.y_max_entry.get().strip()

            # Validate that date fields are not empty.
            if not start_date_str or not end_date_str:
                messagebox.showerror(
                    "Input Error", "Start Date and End Date cannot be empty."
                )
                return

            new_start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
            new_end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
            new_y_min = float(y_min_str) if y_min_str else None
            new_y_max = float(y_max_str) if y_max_str else None

            # Update the settings object.
            self.settings.value_column = new_value_column
            self.settings.start_date = new_start_date
            self.settings.end_date = new_end_date
            self.settings.y_min = new_y_min
            self.settings.y_max = new_y_max

            self.on_apply_callback()
            self.destroy()
        except Exception as e:
            logger.exception(f"Error applying graph settings: {e}")
