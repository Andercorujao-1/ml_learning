# feature_ui_builder.py
from abc import ABC, abstractmethod
import tkinter as tk
from tkinter import ttk

from typing import Any, Optional


class FeatureUiBuilder(ABC):
    """
    Builds the feature UI by assembling common elements such as the top menu bar
    and the page area. Observes the 'page' state in the controller to update the view.
    """

    def __init__(self, featureController: Any):
        self.controller = featureController
        self.pageArea: Optional[ttk.Frame] = None
        self.currentPage: Optional[tk.Widget] = None

    def fInitialDefaultBuild(self, parent: tk.Widget) -> ttk.Frame:
        """
        Builds the initial UI container, including the top menu bar and page area.
        Observes changes in the 'page' state.
        """
        try:
            container = ttk.Frame(parent)
            # Build the top menu bar.
            topBar = self.fBuildTopMenuBar(container)
            topBar.pack(side=tk.TOP, fill=tk.X, padx=6, pady=4)
            # Create the page area.
            self.pageArea = ttk.Frame(container)
            self.pageArea.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=4, pady=6)
            # Build the initial page UI.
            self.fUpdatePage()
            # Observe page state changes.
            self.controller.uiElementsCurrentState["page"].fAddObserver(self.fUpdatePage)
            logger.debug("fInitialDefaultBuild built successfully.")
            return container
        except Exception as e:
            logger.error(f"Error in FeatureUiBuilder.fInitialDefaultBuild: {e}")
            raise

    def fUpdatePage(self) -> None:
        """
        Updates the page area based on the current 'page' state.
        Retrieves the page class via fGetPageClass and instantiates it.
        """
        try:
            if self.currentPage is not None:
                self.currentPage.destroy()
            # Retrieve the current page state as a string.
            pageClassName = self.controller.uiElementsCurrentState["page"].fGetValue()
            pageClass = self.fGetPageClass(pageClassName)
            self.currentPage = pageClass(controller=self.controller, parent=self.pageArea)
            self.currentPage.pack(fill="both", expand=True)
            logger.debug("Feature UI updated successfully.")
        except Exception as e:
            logger.error(f"Error in FeatureUiBuilder.fUpdatePage: {e}")
            raise

    def fBuildTopMenuBar(self, parent: ttk.Frame) -> ttk.Frame:
        """
        Builds the top menu bar based on the static UI configuration provided by the controller.
        """
        try:
            menuBar = ttk.Frame(parent)
            topMenuConfig = self.controller.staticUiConfigs.get("topMenuBar", [])
            for index, btnCfg in enumerate(topMenuConfig):
                # Use a descriptive loop variable (btnCfg) instead of reusing buttonData.
                button = ttk.Button(
                    menuBar,
                    text=btnCfg["name"],
                    command=btnCfg["function"]  # The function (trigger) will be assigned later.
                )
                menuBar.grid_columnconfigure(index, minsize=40)
                button.grid(row=0, column=index, padx=2, pady=3, sticky="nsew")
            logger.debug("Menu built successfully.")
            return menuBar
        except Exception as e:
            logger.error(f"Error in fBuildTopMenuBar: {e}")
            raise

    @abstractmethod
    def fGetPageClass(self, pageName: str) -> Any:
        """
        Should return the widget class corresponding to the page name.
        You need to implement this mapping.
        """
        # Placeholder: you should provide a mapping of page names to widget classes.
        raise NotImplementedError("fGetPageClass must be implemented to map page names to classes.")
