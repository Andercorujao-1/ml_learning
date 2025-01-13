import tkinter as tk
from tkinter import ttk

class GraphPage(ttk.Frame):
    def __init__(self, controller, parent):
        super().__init__(parent)
        self.controller = controller
        self.style = controller.style
        self.style.configure("MyFrame3.TFrame", background="yellow")

        mainFrame = ttk.Frame(self, style="MyFrame3.TFrame")
        mainFrame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        label = ttk.Label(mainFrame, text="gaph page")
        label.pack()

if __name__ == "__main__":
    root = tk.Tk()
    root.title("Data Visualization Interface")
    root.geometry("1400x700")

    style = ttk.Style()
    style.theme_use("clam")

    class testClass:
        def __init__(self, style):
            self.style = style
    controller = testClass(style)

    featurePage = GraphPage(controller=controller, parent=root)
    featurePage.pack(side=tk.TOP, fill=tk.BOTH, expand=True, padx=6, pady=4)

    root.mainloop()