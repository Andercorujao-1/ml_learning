#tests_main_ui
from python_estudo.models.base_feature_ui import BaseFeatureMainUi
import tkinter as tk
from tkinter import ttk


class TestsMainUi(BaseFeatureMainUi):
    def configureStyle(self):
        self.controller.style.configure("testsMenu.TFrame", background="lightblue")

    def createTopBar(self):
        topBar = ttk.Frame(self, style="testsMenu.TFrame")
        self.buildMenuBar(topBar)
        return topBar


if __name__ == "__main__":
    root = tk.Tk()
    root.title("Data Visualization Interface")
    root.geometry("1400x700")

    style = ttk.Style()
    style.theme_use("clam")
    from python_estudo.tests.tests_controller import TestsController

    controller = TestsController(style=style)

    featurePage = controller.provideFeatureUi(root)
    featurePage.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=4, pady=6)

    root.mainloop()
