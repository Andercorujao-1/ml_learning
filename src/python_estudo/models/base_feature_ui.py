import tkinter as tk
from tkinter import ttk

class BaseFeatureMainUi(ttk.Frame):
    """A base UI for feature main screens that have a top menu bar and a page area."""
    def __init__(self, controller, parent):
        super().__init__(parent)
        self.controller = controller
        
        # Optionally configure the style if needed
        if self.controller.style:
            self.configureStyle()
        
        # Create top bar
        topBar = self.createTopBar()
        topBar.pack(side=tk.TOP, fill=tk.X, padx=6, pady=4)

        # Build the page area
        pageArea = ttk.Frame(self)
        pageArea.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=4, pady=6)
        self.controller.buildPageUi(pageArea)

    def configureStyle(self):
        """Subclasses or the base class can define style changes."""
        pass

    def createTopBar(self):
        """Create and return the top bar frame. Subclasses can override to style or add menu items."""
        topBar = ttk.Frame(self)
        self.buildMenuBar(topBar)
        return topBar

    def buildMenuBar(self, parent):
        """Constructs buttons from the controller's `provideButtonDataObjects()` method."""
        i = 0
        buttonDataModels = self.controller.provideButtonDataObjects()
        for btnData in buttonDataModels.values():
            button = ttk.Button(parent, text=btnData.text, command=btnData.userActionFunction)
            button.grid(row=0, column=i, padx=2, pady=3, sticky="nsew")
            i += 1