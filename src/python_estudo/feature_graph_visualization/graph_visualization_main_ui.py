# graph_visualization_main_ui.py
from python_estudo.models.base_feature_ui import BaseFeatureMainUi
import tkinter as tk
from tkinter import ttk


class GraphVisualizationMainUi(BaseFeatureMainUi):
    def configureStyle(self):
        self.controller.style.configure("graphVisualizationMenu.TFrame", background="green")

    def createTopBar(self):
        topBar = ttk.Frame(self, style="graphVisualizationMenu.TFrame")
        self.buildMenuBar(topBar)
        return topBar






# basic testing
if __name__ == "__main__":
    root = tk.Tk()
    root.title("Data Visualization Interface")
    root.geometry("1400x700")

    style = ttk.Style()
    style.theme_use("clam")
    from python_estudo.feature_graph_visualization.graph_visualization_controller import (
        GraphVisualizationController,
    )

    controller = GraphVisualizationController(style=style)

    featurePage = controller.provideFeatureUi(root)
    featurePage.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=4, pady=6)

    root.mainloop()
