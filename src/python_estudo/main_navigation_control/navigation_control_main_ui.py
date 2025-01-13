import tkinter as tk
from tkinter import ttk

class NavigationControlMainUi:
    """
    The main UI for the global navigation control.
    It manages:
    - The root window (title, geometry).
    - A left menu bar with navigation buttons.
    - A feature area where the currently active feature UI is displayed.
    """

    def __init__(self) -> None:
        # Typically, no heavy logic here. The UI gets built in launchMainUi.
        pass

    def launchMainUi(self, controller) -> None:
        """
        Build and run the main Tkinter window with a left bar and the feature area.
        """
        self.controller = controller

        # Create the main window
        root = tk.Tk()
        root.title(self.controller.title)
        root.geometry(self.controller.resolution)

        # Load style and set background
        self.controller.loadStyle()
        self.controller.style.configure("mainMenu.TFrame", background="blue")

        # Create the left menu bar
        menuBar = ttk.Frame(root, style="mainMenu.TFrame")
        menuBar.pack(side=tk.LEFT, fill=tk.Y, padx=4, pady=6)
        self.buildMenuBar(menuBar)

        # Create the feature area on the right
        featureArea = ttk.Frame(root)
        featureArea.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=4, pady=6)

        # Ask the controller to build the current feature UI in featureArea
        self.controller.buildFeatureUi(featureArea)

        # Start the Tk event loop
        root.mainloop()

    def buildMenuBar(self, parent: ttk.Frame) -> None:
        """
        Builds the vertical menu bar with all buttons from the controller's provideButtonDataObjects().
        """
        buttonDataModels = self.controller.provideButtonDataObjects()

        for i, buttonDataModel in enumerate(buttonDataModels.values()):
            button = ttk.Button(
                parent,
                text=buttonDataModel.text,
                command=buttonDataModel.userActionFunction,
            )
            # For each row, configure a minsize
            parent.grid_rowconfigure(i, minsize=40)
            button.grid(row=i, column=0, padx=2, pady=3, sticky="nsew")




