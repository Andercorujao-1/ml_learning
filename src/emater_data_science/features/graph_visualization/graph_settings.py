# src/emater_data_science/features/graph_visualization/graph_settings.py
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import polars as pl


@dataclass
class GraphSettings:
    date_column: str = "Date"
    value_column: str = ""
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    y_min: Optional[float] = None
    y_max: Optional[float] = None

    def update_from_data(self, df: pl.DataFrame):
        # Auto-detect date column.
        if "Date" in df.columns:
            self.date_column = "Date"
        else:
            for col in df.columns:
                if "date" in col.lower():
                    self.date_column = col
                    break

        # Set a default value column if not already set.
        if not self.value_column:
            for col in df.columns:
                if col != self.date_column:
                    self.value_column = col
                    break

        try:
            # Get the column values.
            date_values = df[self.date_column].to_list()
            valid_dates = []
            # If the first value is not a string, assume they are already date/datetime objects.
            if date_values and not isinstance(date_values[0], str):
                valid_dates = [x for x in date_values if x is not None]
            else:
                # Otherwise, try to parse each value as a date.
                valid_dates = [
                    datetime.strptime(str(x).strip(), "%Y-%m-%d")
                    for x in date_values
                    if str(x).strip()
                ]
            if valid_dates:
                self.start_date = min(valid_dates)
                self.end_date = max(valid_dates)
            else:
                # If no valid dates found, default to today.
                self.start_date = datetime.today()
                self.end_date = datetime.today()
        except Exception as e:
            # Log error if needed.
            self.start_date = datetime.today()
            self.end_date = datetime.today()

        try:
            values = df[self.value_column].to_list()
            self.y_min = min(values)
            self.y_max = max(values)
        except Exception as e:
            # If unable to calculate, leave as is.
            pass
