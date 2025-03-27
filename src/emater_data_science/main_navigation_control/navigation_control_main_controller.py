# navigation_control_main_controller.py
from typing import Dict, Type, Any, Literal

from .main_theme import getTtkStyle
from emater_data_science.library.changenotifier import ChangeNotifier
from emater_data_science.features.feature_controller import BaseFeatureController

# from emater_data_science.tests.tests_controller import TestsController
from emater_data_science.features.graph_visualization.graph_visualization_controller import (
    GraphVisualizationController,
)

# Define the allowed feature names as a Literal type.
AllowedFeatureName = Literal["graph_visualization", "tests", "api"]


def fDefaultConfigs() -> Dict[str, str]:
    return {
        "title": "Data Visualization Interface",
        "resolution": "1400x700",
    }


def fFeatureControllersDict() -> Dict[AllowedFeatureName, Type[BaseFeatureController]]:
    return {
        "graph_visualization": GraphVisualizationController,
        # "api": ApiController,  # Uncomment or add when available.
    }


class NavigationControlTriggersManager:
    """
    Manages global navigation triggers.
    Provides trigger functions that change the active feature by calling
    the controller’s fChangeFeature with an allowed feature name.
    """

    def __init__(self, controller: "NavigationControlMainController"):
        self.controller = controller

    def getUserTriggers(self) -> Dict[str, callable]:
        # The keys here are the button labels.
        return {
            "Change to Graph": lambda: self.controller.fChangeFeature(
                "graph_visualization"
            ),
            "Change to Test": lambda: self.controller.fChangeFeature("tests"),
            "Change to Api": lambda: self.controller.fChangeFeature("api"),
        }


class NavigationControlMainController:
    """
    Global navigation controller.

    - Loads global configurations (title, resolution, style).
    - Holds a ChangeNotifier wrapping the current feature controller.
    - Uses a triggers manager to expose user triggers.
    - Loads a static UI configuration for the left menu bar.
    """

    def __init__(
        self,
        firstFeatureController: Type[
            BaseFeatureController
        ] = GraphVisualizationController,
    ) -> None:
        try:
            self.style: Any = (
                None  # Global ttk.Style instance; will be loaded in fLoadStyle.
            )
            self.title: str = ""
            self.resolution: str = ""
            # Instantiate the triggers manager.
            self.triggersManager = NavigationControlTriggersManager(self)
            # Load the static UI configuration (for example, left menu bar buttons).
            self.staticUiConfigs: Dict[str, Any] = self.fLoadStaticUiConfigs()
            # Map allowed feature names to controller classes.
            self.featuresDict: Dict[AllowedFeatureName, Type[BaseFeatureController]] = (
                fFeatureControllersDict()
            )
            # Hold the active feature controller in a ChangeNotifier.
            self.featureController: ChangeNotifier = ChangeNotifier(
                firstFeatureController()
            )
        except Exception as e:
            logger.error(f"Error in NavigationControlMainController.__init__: {e}")
            raise

    def fLoadStaticUiConfigs(self) -> Dict[str, Any]:
        """
        Loads static UI configurations for navigation control.
        Here we define a "leftMenuBar" configuration: a dictionary mapping button labels
        to trigger functions.
        """
        return {"leftMenuBar": self.triggersManager.getUserTriggers()}

    def fLoadStyle(self) -> Any:
        try:
            self.style = getTtkStyle()
            # Inject the style into the current feature controller.
            self.featureController.fGetValue().style = self.style
            logger.debug("Style loaded successfully.")
            return self.style
        except Exception as e:
            logger.error(f"Error in fLoadStyle: {e}")
            raise

    def fLaunchUi(self, configs: Dict[str, str] = fDefaultConfigs()) -> None:
        try:
            self.title = configs["title"]
            self.resolution = configs["resolution"]
            from .navigation_control_main_ui import NavigationControlMainUi

            ui = NavigationControlMainUi()
            ui.fLaunchMainUi(self)
        except Exception as e:
            logger.error(f"Error in fLaunchUi: {e}")
            raise

    def fBuildFeatureUi(self, featureArea: Any) -> None:
        self.featureArea = featureArea
        self.featureController.fAddObserver(self.fApplyFeatureUi)
        self.fApplyFeatureUi()

    def fApplyFeatureUi(self) -> None:
        try:
            if hasattr(self, "featureUi") and self.featureUi is not None:
                self.featureUi.destroy()
            featureControllerObj = self.featureController.fGetValue()
            self.featureUi = featureControllerObj.fProvideFeatureUi(self.featureArea)
            self.featureUi.pack(fill="both", expand=True)
            logger.debug("Feature UI applied successfully.")
        except Exception as e:
            logger.error(f"Error in fApplyFeatureUi: {e}")
            raise

    def fChangeFeature(self, feature: AllowedFeatureName) -> None:
        """
        Changes the active feature.
        The parameter 'feature' must be one of the allowed feature names.
        """
        try:
            newFeatureControllerClass = self.featuresDict.get(feature)
            if newFeatureControllerClass is None:
                logger.error(f"Feature '{feature}' not found.")
                return
            currentFeatureController = self.featureController.fGetValue()
            if isinstance(currentFeatureController, newFeatureControllerClass):
                logger.debug(f"Already on feature '{feature}'.")
                return
            self.featureController.fChangeValue(
                newFeatureControllerClass(style=self.style)
            )
            logger.debug(f"Changed feature to '{feature}'.")
        except Exception as e:
            logger.error(f"Error in fChangeFeature: {e}")
            raise
