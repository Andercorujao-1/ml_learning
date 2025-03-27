# src/emater_data_science/features/graph_visualization/visual_interface/table_page.py
import tkinter as tk
from tkinter import ttk

import polars as pl


class TablePage(ttk.Frame):
    def __init__(self, controller, parent):
        super().__init__(parent)
        self.controller = controller
        self._build_ui()

    def _build_ui(self):
        # Create a Treeview widget to display tabular data.
        self.tree = ttk.Treeview(self)
        self.tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        # Vertical scrollbar.
        vsb = ttk.Scrollbar(self, orient="vertical", command=self.tree.yview)
        vsb.pack(side=tk.RIGHT, fill="y")
        self.tree.configure(yscrollcommand=vsb.set)
        # Horizontal scrollbar.
        hsb = ttk.Scrollbar(self, orient="horizontal", command=self.tree.xview)
        hsb.pack(side=tk.BOTTOM, fill="x")
        self.tree.configure(xscrollcommand=hsb.set)
        # Populate the table with data.
        self._populate_data()

    def _populate_data(self):
        # Clear existing data (if any).
        for item in self.tree.get_children():
            self.tree.delete(item)
        # Get the data from the controller.
        df = getattr(self.controller, "loadedTableData", None)
        if df is None:
            logger.debug("No data loaded to display in TablePage.")
            return

        # Setup columns.
        columns = list(df.columns)
        self.tree["columns"] = columns
        self.tree["show"] = "headings"
        for col in columns:
            self.tree.heading(col, text=col)
            self.tree.column(col, width=100, anchor="center")
        # Insert rows into the treeview.
        try:
            # Convert the polars DataFrame to a list of rows.
            data_rows = df.to_numpy().tolist()
        except Exception as e:
            logger.exception(f"Error converting DataFrame to list: {e}")
            data_rows = []
        for row in data_rows:
            self.tree.insert("", "end", values=row)
        logger.debug("TablePage populated with data.")
