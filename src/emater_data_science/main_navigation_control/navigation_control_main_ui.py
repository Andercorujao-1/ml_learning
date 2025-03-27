# navigation_control_main_ui.py
import tkinter as tk
from tkinter import ttk

from typing import Dict, Callable
from emater_data_science.main_navigation_control.navigation_control_main_controller import (
    NavigationControlMainController,
)


class NavigationControlMainUi:
    """
    Global navigation UI.

    Builds a left-side menu bar (based on the static UI configuration)
    and a feature area.
    """

    def __init__(self) -> None:
        pass

    def fLaunchMainUi(self, controller: NavigationControlMainController) -> None:
        try:
            self.controller = controller
            root = tk.Tk()
            root.title(controller.title)
            root.geometry(controller.resolution)
            controller.fLoadStyle()
            controller.style.configure("mainMenu.TFrame", background="blue")
            menuBar = ttk.Frame(root, style="mainMenu.TFrame")
            menuBar.pack(side=tk.LEFT, fill=tk.Y, padx=4, pady=6)
            self.fBuildMenuBar(menuBar)
            featureArea = ttk.Frame(root)
            featureArea.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=4, pady=6)
            controller.fBuildFeatureUi(featureArea)
            root.mainloop()
        except Exception as e:
            logger.error(f"Error in fLaunchMainUi: {e}")
            raise

    def fBuildMenuBar(self, parent: ttk.Frame) -> None:
        try:
            # Get the left menu configuration: a dict mapping button labels to trigger functions.
            leftMenuConfig: Dict[str, Callable] = self.controller.staticUiConfigs.get(
                "leftMenuBar", {}
            )
            for index, (buttonText, triggerFunction) in enumerate(
                leftMenuConfig.items()
            ):
                button = ttk.Button(parent, text=buttonText, command=triggerFunction)
                parent.grid_rowconfigure(index, minsize=40)
                button.grid(row=index, column=0, padx=2, pady=3, sticky="nsew")
            logger.debug("Navigation menu bar built successfully.")
        except Exception as e:
            logger.error(f"Error in fBuildMenuBar: {e}")
            raise
