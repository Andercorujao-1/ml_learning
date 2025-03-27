# feature_controller.py
from abc import ABC

from typing import Dict, List, Optional, Any
from emater_data_science.library.changenotifier import ChangeNotifier


class BaseFeatureController(ABC):
    """
    Base controller for a feature.

    Holds UI configuration data and state for UI elements.
    It does not yet include triggers; those will be added later once
    fLoadStaticUiConfigs is fully implemented.
    """

    def __init__(
        self,
        style: Optional[Any] = None,
        uiElementsStatesListing: Optional[Dict[str, List[str]]] = None,
        uiElementsCurrentState: Optional[Dict[str, ChangeNotifier]] = None,
    ):
        self.style = style
        # Mapping of UI element names to allowed state strings.
        self.uiElementsStatesListing: Dict[str, List[str]] = (
            uiElementsStatesListing or {}
        )
        # Mapping of UI element names to their current state as a ChangeNotifier.
        self.uiElementsCurrentState: Dict[str, ChangeNotifier] = (
            uiElementsCurrentState or {}
        )
        # Static UI configuration loaded from an external source.
        self.staticUiConfigs: Dict[str, Any] = self.fLoadStaticUiConfigs()

    def fLoadStaticUiConfigs(self) -> Dict[str, Any]:
        """
        Loads static UI configurations.

        This is where you define the configuration of UI elements (e.g., top menu bar buttons).
        For example, you might return a dictionary like:
            {
                "topMenuBar": [
                    {"name": "button1", "function": None},
                    {"name": "button2", "function": None},
                ],
                ...
            }
        Later, you will use triggers to assign the "function" values.
        """
        # Placeholder implementation; update with real configuration as needed.
        return {
            "topMenuBar": [
                {"name": "button1", "function": None},
                {"name": "button2", "function": None},
            ]
        }

    def fChangeUiState(self, elementName: str, newState: str) -> None:
        """
        Changes the state of a UI element after validating that the element
        exists, has been initialized, and that the new state is allowed.
        """
        try:
            if elementName not in self.uiElementsStatesListing:
                logger.error(f"UI element '{elementName}' not found in states listing.")
                return
            if elementName not in self.uiElementsCurrentState:
                logger.error(
                    f"UI element '{elementName}' does not have state initialized."
                )
                return
            if newState not in self.uiElementsStatesListing[elementName]:
                logger.error(
                    f"UI element '{elementName}' cannot have the state '{newState}'."
                )
                return
            currentStateName = self.uiElementsCurrentState[elementName].fGetValue()
            if currentStateName == newState:
                logger.debug(
                    f"Already in state '{newState}' for element '{elementName}'."
                )
                return
            self.uiElementsCurrentState[elementName].fChangeValue(newState)
            logger.debug(f"Changed the state of '{elementName}' to '{newState}'.")
        except Exception as e:
            logger.error(f"Error in fChangeUiState: {e}")
            raise
