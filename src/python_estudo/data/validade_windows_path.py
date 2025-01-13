import os
from pathlib import Path
from loguru import logger

def fValidateAndExpandWindowsPath(file_path: str) -> Path:
        """
        Validates and expands a Windows-style string path.
        
        Args:
            file_path (str): The file path as a string.
        
        Returns:
            Path: A resolved Path object.
        """
        if file_path.startswith("%"):  # Expand environment variables
            file_path = os.path.expandvars(file_path)
        
        path_obj = Path(file_path)
        if not path_obj.exists():
            logger.error(f"File not found: {file_path}")
            raise FileNotFoundError(f"File not found: {file_path}")
        
        return path_obj