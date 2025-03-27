# src/emater_data_science/features/graph_visualization/graph_visualization_ui_builder.py
import tkinter as tk
from tkinter import ttk

from emater_data_science.features.feature_ui_builder import FeatureUiBuilder
from emater_data_science.features.graph_visualization.visual_interface.graph_page import (
    GraphPage,
)
from emater_data_science.features.graph_visualization.visual_interface.table_page import (
    TablePage,
)


class GraphVisualizationMainUi(FeatureUiBuilder):
    """
    Builds the UI for the graph visualization feature,
    including a drop-down menu in the top bar to select a table.
    """

    def fBuild(self, parent: tk.Widget) -> ttk.Frame:
        try:
            # Use the base builder method to assemble the container.
            container = super().fInitialDefaultBuild(parent)
            # Customize styling if needed.
            if not self.controller.style:
                self.controller.style = ttk.Style()
            self.controller.style.configure(
                "graphVisualizationMenu.TFrame", background="green"
            )
            logger.debug("GraphVisualization UI built successfully.")
            return container
        except Exception as e:
            logger.error(f"Error in GraphVisualizationUiBuilder.fBuild: {e}")
            raise

    def fBuildTopMenuBar(self, parent: ttk.Frame) -> ttk.Frame:
        """
        Overrides the base method to add a drop-down for table selection.
        """
        try:
            # Build the standard menu bar from the static config.
            topMenuConfig = self.controller.staticUiConfigs.get("topMenuBar", [])
            menuBar = ttk.Frame(parent)
            # Create buttons from the configuration.
            for index, btnCfg in enumerate(topMenuConfig):
                button = ttk.Button(
                    menuBar, text=btnCfg["name"], command=btnCfg["function"]
                )
                menuBar.grid_columnconfigure(index, minsize=40)
                button.grid(row=0, column=index, padx=2, pady=3, sticky="nsew")

            # --- Add the drop-down for table selection ---
            # Import OptionMenu from tkinter.
            from tkinter import OptionMenu, StringVar

            # Create a StringVar to hold the selected table.
            self.selectedTableVar = StringVar()
            availableTables = self.controller.availableTables
            if availableTables:
                self.selectedTableVar.set(availableTables[0])
            else:
                self.selectedTableVar.set("")

            # Create the OptionMenu.
            optionMenu = OptionMenu(
                menuBar,
                self.selectedTableVar,
                *availableTables,
                command=self._on_table_selected,
            )
            optionMenu.config(width=20)
            # Place the drop-down next to the buttons.
            col = len(topMenuConfig)
            optionMenu.grid(row=0, column=col, padx=2, pady=3, sticky="nsew")
            logger.debug("Top menu bar with drop-down built successfully.")
            return menuBar
        except Exception as e:
            logger.error(f"Error in fBuildTopMenuBar: {e}")
            raise

    def _on_table_selected(self, selected_table: str) -> None:
        """
        Callback invoked when a table is selected from the drop-down.
        Calls the controller to load data for the selected table.
        """
        try:
            logger.debug(f"Table selected from drop-down: {selected_table}")
            self.controller.fLoadTableData(selected_table)
        except Exception as e:
            logger.error(f"Error in _on_table_selected: {e}")
            raise

    def fGetPageClass(self, pageName: str):
        """
        Maps page names to their corresponding widget classes.
        """
        pagesDict = {"graphPage": GraphPage, "tablePage": TablePage}
        if pageName not in pagesDict:
            raise ValueError(f"Page '{pageName}' not recognized.")
        return pagesDict[pageName]
