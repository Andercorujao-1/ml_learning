from tkinter import ttk


class EmptyPage(ttk.Frame):
    def __init__(self, parent, controller):
        super().__init__(parent) 
        button = ttk.Button(parent, text='empty')
        button.pack()