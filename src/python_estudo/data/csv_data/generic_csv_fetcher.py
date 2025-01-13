import polars as pl
from loguru import logger

from python_estudo.data.validade_windows_path import fValidateAndExpandWindowsPath


class CsvFetcher:
    """
    Handles fetching data from CSV files.
    """

    @staticmethod
    def fetch_csv(file_path: str) -> pl.DataFrame:
        """
        Fetches data from a CSV file.
        
        Args:
            file_path (str): The file path as a string.
        
        Returns:
            pl.DataFrame: The loaded data.
        """
        try:
            validatedPath = fValidateAndExpandWindowsPath(file_path)
            data = pl.read_csv(validatedPath)
            logger.info(f"Successfully loaded data from {validatedPath}")
            return data
        except Exception as e:
            logger.exception(f"Error fetching CSV data: {e}")
            raise


# Test example
if __name__ == "__main__":
    test = CsvFetcher()
    testPath = "%USERPROFILE%\\Documents\\projects_data\\python_estudo\\Mock_Agricultural_Data.csv"
    try:
        print(test.fetch_csv(testPath))
    except Exception as e:
        logger.error(f"Error during CSV fetch: {e}")