from typing import Dict, Type
from emater_data_science.features.feature_controller import BaseFeatureController

from emater_data_science.tests.tests_controller import TestsController
from emater_data_science.features.graph_visualization.graph_visualization_controller import (
    GraphVisualizationController,
)


def fFeatureControllersDict() -> Dict[str, Type[BaseFeatureController]]:
    return {
        "graph_visualization": GraphVisualizationController,
        "tests": TestsController,
    }
