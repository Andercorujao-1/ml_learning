from __future__ import annotations  # Allows class type hints like NavigationControlMainController
from typing import Dict, Type

from python_estudo.main_navigation_control.main_theme import getTtkStyle
from python_estudo.models.changenotifier import ChangeNotifier
from python_estudo.models.ui_button_data_model import UiButtonDataModel
from python_estudo.models.base_feature_controller import BaseFeatureController

from python_estudo.tests.tests_controller import TestsController
from python_estudo.feature_graph_visualization.graph_visualization_controller import (
    GraphVisualizationController,
)


def fDefaultConfigs() -> Dict[str, str]:
    """
    Returns default configuration for the NavigationControlMainController UI.
    """
    return {
        "title": "Data Visualization Interface",
        "resolution": "1400x700",
    }


def fDefaultButtons() -> Dict[str, Dict[str, str]]:
    """
    Returns a dict that maps button keys (e.g. 'button1') to another dict
    defining 'text' and 'userActionName'.
    """
    return {
        "button1": {"text": "Graph", "userActionName": "changeToGraphFeature"},
        "button2": {"text": "Test",  "userActionName": "changeToTestFeature"},
        "button3": {"text": "Api",   "userActionName": "changeToApiFeature"},
    }


def fFeatureControllersDict() -> Dict[str, Type[BaseFeatureController]]:
    """
    Returns a dictionary mapping feature names to their respective controller classes.
    """
    return {
        "graph_visualization": GraphVisualizationController,
        "tests": TestsController,
    }


class NavigationControlMainController:
    """
    The main (and only) navigation controller.

    - Manages which feature controller is active.
    - Provides a user actions map (changeToGraphFeature, changeToTestFeature, etc.).
    - Builds the UI for the currently active feature.
    """

    class UserActions:
        """
        Nested class that defines user actions for the NavigationControlMainController.
        """
        def __init__(self, controller: NavigationControlMainController) -> None:
            self.controller = controller

        def getActionsDict(self) -> Dict[str, callable]:
            return {
                "changeToGraphFeature": self.changeToGraphFeature,
                "changeToTestFeature":  self.changeToTestFeature,
                "changeToApiFeature":   self.changeToApiFeature,
            }

        def changeToGraphFeature(self) -> None:
            self.controller.changeFeature("graph_visualization")

        def changeToTestFeature(self) -> None:
            self.controller.changeFeature("tests")

        def changeToApiFeature(self) -> None:
            print("API button clicked")  # Example placeholder

    #################END OF USER ACTIONS#########

    def __init__(
        self,
        firstFeatureController: Type[BaseFeatureController] = GraphVisualizationController
    ) -> None:
        """
        Args:
            firstFeatureController: The class of the feature controller to start with.
        """
        self.style = None
        self.featureUi = None

        # For user actions (changing features)
        self.userActions = self.UserActions(controller=self)

        # Maps feature names to controller classes
        self.featuresDict: Dict[str, Type[BaseFeatureController]] = fFeatureControllersDict()

        # Cached button data
        self.buttonDataModels: Dict[str, UiButtonDataModel] | None = None

        # Current active feature controller (as a ChangeNotifier of a controller instance)
        self.featureController = ChangeNotifier(firstFeatureController())

    def loadStyle(self) -> object:
        """
        Loads and applies the default style to the currently active feature controller.
        Returns:
            The ttk.Style object.
        """
        self.style = getTtkStyle()
        featureController = self.featureController.getValue()

        # If the active feature controller is set, inject the style
        featureController.style = self.style
        return self.style

    def LaunchUi(self, configs: Dict[str, str] = fDefaultConfigs()) -> None:
        """
        Launches the main UI for the navigation controller.
        """
        self.title = configs["title"]
        self.resolution = configs["resolution"]

        # Deferred import to avoid circular references
        from .navigation_control_main_ui import NavigationControlMainUi

        # Create and run the NavigationControlMainUi
        NavigationControlMainUi().launchMainUi(self)

    def provideButtonDataObjects(self) -> Dict[str, UiButtonDataModel]:
        """
        Returns a dictionary of button data models for building the left menu bar.
        The data is cached after first creation.
        """
        if self.buttonDataModels is not None:
            return self.buttonDataModels

        # Build from default data + user actions
        buttonsData = fDefaultButtons()
        userActionsDict = self.userActions.getActionsDict()

        self.buttonDataModels = UiButtonDataModel.factoryFromJson(
            buttonsData, userActionsDict
        )
        return self.buttonDataModels

    def buildFeatureUi(self, featureArea) -> None:
        """
        Called by the UI to build the current feature's UI in the 'featureArea' frame.
        """
        self.featureArea = featureArea
        # Observe changes in the active feature controller
        self.featureController.addObserver(self.aplyFeatureUi)
        self.aplyFeatureUi()

    def aplyFeatureUi(self) -> None:
        """
        Rebuilds the UI for the active feature controller.
        """
        if self.featureUi is not None:
            self.featureUi.destroy()

        featureControllerObject = self.featureController.getValue()
        self.featureUi = featureControllerObject.provideFeatureUi(self.featureArea)
        self.featureUi.pack(fill="both", expand=True)

    def changeFeature(self, featureName: str) -> None:
        """
        Changes the active feature to 'featureName'.
        """
        newFeatureControllerClass = self.featuresDict[featureName]
        currentFeatureController = self.featureController.getValue()

        # If already on this feature, do nothing
        if isinstance(currentFeatureController, newFeatureControllerClass):
            print("already this feature:", featureName)
            return

        # Otherwise, create a new feature controller instance with the existing style
        self.featureController.changeValue(newFeatureControllerClass(style=self.style))