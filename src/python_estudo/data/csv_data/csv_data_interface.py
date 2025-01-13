#csv_data_interface.py
from python_estudo.data.csv_data.generic_csv_fetcher import CsvFetcher
from python_estudo.models.table_filter import TableFilter

class CsvDataInterface:
    projectDataFolder = "%USERPROFILE%\\Documents\\projects_data\\python_estudo"

        
    @classmethod
    def csvTablesList(cls):
        return{ "Mock_Agricultural_Data": {"source": "csv", "fetchMethod": cls.csvTableFetch},}
    
    @classmethod
    def tableFetch(cls, tableName, tableFilter:TableFilter):
        table = CsvFetcher.fetch_csv(f"{cls.projectDataFolder}\\{tableName}.csv")
        tableFilter.setTable(table, tableName)
        return tableFilter.filteredTable()
    


if __name__ == "__main__":
    print(CsvDataInterface.tableFetch("Mock_Agricultural_Data"))