# src/emater_data_science/features/graph_visualization/graph_visualization_controller.py
import tkinter as tk
from tkinter import ttk

from typing import Dict, Any

from emater_data_science.features.graph_visualization.graph_settings import (
    GraphSettings,
)
from emater_data_science.features.feature_controller import BaseFeatureController
from emater_data_science.library.changenotifier import ChangeNotifier


class GraphVisualizationTriggersManager:
    """
    Manages all triggers for GraphVisualizationController.
    """

    def __init__(self, controller: "GraphVisualizationController"):
        self.controller = controller

    def getUserTriggers(self) -> Dict[str, callable]:
        return {
            "fChangeToTablePage": self.fChangeToTablePage,
            "fChangeToGraphPage": self.fChangeToGraphPage,
            "fLoadMockAgriculturalData": self.fLoadMockAgriculturalData,
        }

    def fChangeToTablePage(self) -> None:
        self.controller.fChangeUiState("page", "tablePage")

    def fChangeToGraphPage(self) -> None:
        self.controller.fChangeUiState("page", "graphPage")

    def fLoadMockAgriculturalData(self) -> None:
        self.controller.fLoadTableData("Mock_Agricultural_Data")


class GraphVisualizationController(BaseFeatureController):
    """
    Controller for the Graph Visualization feature.
    """

    def __init__(self, style: ttk.Style = None):
        try:
            self.triggersManager = GraphVisualizationTriggersManager(self)
            uiStatesListing = {"page": ["tablePage", "graphPage"]}
            uiStatesCurrent = {"page": ChangeNotifier("tablePage")}
            self.availableTables = ["Mock_Agricultural_Data"]
            self.loadedTableData = None
            # Add a ChangeNotifier for graph settings.
            self.graphSettings = ChangeNotifier(GraphSettings())
            super().__init__(
                style=style,
                uiElementsStatesListing=uiStatesListing,
                uiElementsCurrentState=uiStatesCurrent,
            )
            logger.debug("GraphVisualizationController initialized.")
        except Exception as e:
            logger.error(f"Error in GraphVisualizationController.__init__: {e}")
            raise

    def fLoadStaticUiConfigs(self) -> Dict[str, Any]:
        """
        Loads static UI configurations specific to the Graph Visualization feature.
        """
        return {
            "topMenuBar": [
                {"name": "Table", "function": self.triggersManager.fChangeToTablePage},
                {"name": "Graph", "function": self.triggersManager.fChangeToGraphPage},
                {
                    "name": "Load Data",
                    "function": self.triggersManager.fLoadMockAgriculturalData,
                },
            ]
        }

    def fLoadTableData(self, table_name: str) -> None:
        try:
            from emater_data_science.data.data_interface import DataInterface

            data_interface = DataInterface()
            logger.debug(f"Loading data for table: {table_name}")
            df = data_interface.fetch_table(table_name)
            self.loadedTableData = df
            # Update graph settings based on loaded data.
            self.graphSettings.value.update_from_data(df)
            self.graphSettings.fNotifyObservers()  # notify listeners to update the graph
            # Switch to tablePage (or graphPage, depending on your flow)
            self.fChangeUiState("page", "graphPage")
        except Exception as e:
            logger.exception(f"Error loading table data for {table_name}: {e}")
            raise

    def fProvideFeatureUi(self, parent: tk.Widget) -> tk.Widget:
        """
        Provides the UI for the feature by instantiating the GraphVisualizationUiBuilder.
        """
        try:
            from emater_data_science.features.graph_visualization.graph_visualization_main_ui import (
                GraphVisualizationMainUi,
            )

            uiBuilder = GraphVisualizationMainUi(featureController=self)
            return uiBuilder.fBuild(parent)
        except Exception as e:
            logger.error(f"Error in fProvideFeatureUi: {e}")
            raise
