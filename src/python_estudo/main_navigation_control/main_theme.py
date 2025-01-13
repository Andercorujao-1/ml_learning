#main_theme.py
from tkinter import ttk

def getTtkStyle():
    style = ttk.Style()
    style.theme_use("clam")
    return style

if __name__ == "__main__":
    print(getTtkStyle())