#table_filter.py
import polars as pl
from dataclasses import dataclass
from typing import Dict, List

@dataclass
class DataFilter:
    """
    Represents a single filter applied to a table column.
    """
    validOperands = {"==", "!=", ">", "<", ">=", "<="}

    columnName: str
    filterOperand: str
    filterValue: any

    def __post_init__(self):
        if self.filterOperand not in self.validOperands:
            raise ValueError(f"Invalid filter operand: {self.filterOperand}")


class TableFilter:
    """
    Manages and applies filters to a Polars DataFrame.
    """
    def __init__(self, table: pl.DataFrame = None, tableName: str = None):
        self.tableName = tableName
        self.originalTable = table
        self.filteredTable = None
        self.filterObjects: Dict[str, DataFilter] = {}

    def setTable(self, table: pl.DataFrame, tableName: str):
        if self.tableName is not None:
            raise ValueError("Table already set. Resetting is not allowed.")
        self.tableName = tableName
        self.originalTable = table
        self.filteredTable = None
        self.checkValidFiltersForTable()

    def checkValidFiltersForTable(self, filterObject = None):
        if self.originalTable is None or len(self.filterObjects)==0:
            return

        if filterObject is not None:
            if filterObject.columnName not in self.originalTable.columns:
                raise ValueError(f"Invalid column name: {filterObject.columnName}")
        else:
            for filterObject in self.filterObjects.values():
                if filterObject.columnName not in self.originalTable.columns:
                    raise ValueError(f"Invalid column name: {filterObject.columnName}")

    def getFilteredTable(self) -> pl.DataFrame:
        """
        Applies all filters to the original table and returns the filtered DataFrame.
        """
        if self.originalTable is None:
            raise ValueError("theres no table here")
        if not self.filterObjects:
            return self.originalTable

        filters = []
        for filterObject in self.filterObjects.values():
            match filterObject.filterOperand:
                case "==":
                    filters.append(pl.col(filterObject.columnName) == filterObject.filterValue)
                case "!=":
                    filters.append(pl.col(filterObject.columnName) != filterObject.filterValue)
                case ">":
                    filters.append(pl.col(filterObject.columnName) > filterObject.filterValue)
                case "<":
                    filters.append(pl.col(filterObject.columnName) < filterObject.filterValue)
                case ">=":
                    filters.append(pl.col(filterObject.columnName) >= filterObject.filterValue)
                case "<=":
                    filters.append(pl.col(filterObject.columnName) <= filterObject.filterValue)
                case _:
                    raise ValueError(f"Invalid filter operand: {filterObject.filterOperand}")

        combinedFilter = filters[0] if filters else None
        for f in filters[1:]:
            combinedFilter &= f

        self.filteredTable = self.originalTable.filter(combinedFilter)
        return self.filteredTable

    def fromJsonSetFilters(self, filtersDictList: List[Dict]):
        """
        Sets filters from a list of dictionaries (JSON-like format).
        """
        for filterDict in filtersDictList:
            try:
                filterObject = DataFilter(**filterDict)
                self.setFilter(filterObject)
            except KeyError as e:
                print(f"Error: Missing required field in filter JSON: {e}")
            except ValueError as e:
                print(f"Error: Invalid filter in JSON: {e}")

    def setFilter(self, filterObject: DataFilter):
        """
        Adds or updates a filter.
        """
        if filterObject.filterOperand == "!=":
            key = f"{filterObject.columnName}{filterObject.filterOperand}{filterObject.filterValue}"
        else:
            key = f"{filterObject.columnName}{filterObject.filterOperand}"

        if key in self.filterObjects:
            print(f"Warning: Overwriting existing filter for key {key}")

        self.checkValidFiltersForTable(filterObject)
        
        self.filterObjects[key] = filterObject
            
        

    def getFilters(self) -> Dict[str, DataFilter]:
        """
        Returns the current filters.
        """
        return self.filterObjects

    def removeFilter(self, key: str):
        """
        Removes a filter by its key.
        """
        if key in self.filterObjects:
            del self.filterObjects[key]
        else:
            raise KeyError(f"Filter with key '{key}' does not exist.")


if __name__ == "__main__":
    # Example Usage
    def exampleTableData():
        data = [
            {"ID": 1, "Name": "Alice", "Age": 25, "Sex": "F"},
            {"ID": 2, "Name": "Bob", "Age": 30, "Sex": "M"},
            {"ID": 3, "Name": "Charlie", "Age": 22, "Sex": "M"},
            {"ID": 4, "Name": "Diana", "Age": 28, "Sex": "F"},
            {"ID": 5, "Name": "Eve", "Age": 35, "Sex": "F"},
        ]
        return pl.DataFrame(data)

    table = exampleTableData()
    tableName = "example_table_name"
    tableFilter = TableFilter(table=table, tableName=tableName)
    filtersJson = {
        "example_table_name": {
            "tableFilters": [
                {"columnName": "Age", "filterOperand": ">=", "filterValue": 30},
                {"columnName": "Sex", "filterOperand": "==", "filterValue": "F"},
            ]
        }
    }
    tableFilter.fromJsonSetFilters(filtersJson[tableName]["tableFilters"])

    print(tableFilter.getFilteredTable())
