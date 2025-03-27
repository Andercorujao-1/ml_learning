import os
import json
from pathlib import Path
from datetime import datetime


def generate_folder_structure_json(root_path: str) -> None:
    """
    Generates a JSON file representing the folder structure of the given path.

    Args:
        root_path (str): The root folder path to generate the structure for.
    """

    def get_structure(directory: Path):
        structure = {"files": []}
        for item in directory.iterdir():
            if item.is_dir():
                if item.name != "__pycache__":
                    structure[item.name] = get_structure(item)
            elif item.is_file():
                structure["files"].append(item.name)
        return structure

    # Expand and validate the root path
    root = Path(os.path.expandvars(root_path)).resolve()
    print(f"Resolved root path: {root}")
    if not root.exists() or not root.is_dir():
        raise ValueError(f"Invalid root path: {root}")

    # Generate folder structure
    folder_structure = {
        "project_name": root.name,
        "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "folders": get_structure(root),
    }

    # Save as a JSON file at the root path
    output_file = root / "folders_structure.json"

    with open(output_file, "w", encoding="utf-8") as json_file:
        json.dump(folder_structure, json_file, indent=4)

    print(f"Folder structure saved to {output_file}")


# Example usage
if __name__ == "__main__":
    root_path = "%USERPROFILE%/Documents/programação/emater_data_science"
    try:
        generate_folder_structure_json(root_path)
    except Exception as e:
        print(f"Error: {e}")
