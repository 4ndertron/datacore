"""
This file was last edited while following the tutorial at this link:
https://pythonprogramming.net/organizing-gui/?completed=/embedding-live-matplotlib-graph-tkinter-gui/

Progress was stopped in order to create and configure the custom DataHandler class that this program will
use to run functions and display data.
"""
import tkinter as tk
import matplotlib
import matplotlib.animation as animation
from tkinter import ttk
from matplotlib import style
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk

LARGE_FONT = ('Verdana', 12)
matplotlib.use('TkAgg')
style.use('ggplot')

f = Figure(figsize=(5, 4), dpi=100)
a = f.add_subplot(111)


def animate(i):
    pull_data = open('./data/sampleText.txt', 'r').read()
    data_array = pull_data.split('\n')
    xar = []
    yar = []
    for eachLine in data_array:
        if len(eachLine) > 1:
            x, y = eachLine.split(',')
            xar.append(int(x))
            yar.append(int(y))
    a.clear()
    a.plot(xar, yar)


class DataCore(tk.Tk):
    def __init__(self, *args, **kwargs):
        tk.Tk.__init__(self, *args, **kwargs)

        tk.Tk.iconbitmap(self, default='datacore.ico')
        tk.Tk.wm_title(self, 'DataCore Client')

        container = tk.Frame(self)
        container.pack(side='top', fill='both', expand=True)
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)

        self.frames = {}

        for f in (StartPage, PageOne, PageTwo, PageThree):
            frame = f(container, self)
            self.frames[f] = frame
            frame.grid(row=0, column=0, sticky='nsew')

        self.show_frame(StartPage)

    def show_frame(self, cont):
        frame = self.frames[cont]
        frame.tkraise()


class StartPage(tk.Frame):
    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        label = ttk.Label(self, text='Start Page', font=LARGE_FONT)
        label.pack(pady=10, padx=10)

        button = ttk.Button(self, text='Visit Page 1', command=lambda: controller.show_frame(PageOne))
        button.pack()

        button2 = ttk.Button(self, text='Visit Page 2', command=lambda: controller.show_frame(PageTwo))
        button2.pack()

        button3 = ttk.Button(self, text='Visit Graph Page', command=lambda: controller.show_frame(PageThree))
        button3.pack()


class PageOne(tk.Frame):
    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        label = ttk.Label(self, text='Page One!', font=LARGE_FONT)
        label.pack(pady=10, padx=10)

        button1 = ttk.Button(self, text='Back to Home', command=lambda: controller.show_frame(StartPage))
        button1.pack()

        button2 = ttk.Button(self, text='Page Two', command=lambda: controller.show_frame(PageTwo))
        button2.pack()


class PageTwo(tk.Frame):
    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        label = ttk.Label(self, text='Page Two!', font=LARGE_FONT)
        label.pack(pady=10, padx=10)

        button1 = ttk.Button(self, text='Back to Home', command=lambda: controller.show_frame(StartPage))
        button1.pack()

        button2 = ttk.Button(self, text='Page One', command=lambda: controller.show_frame(PageOne))
        button2.pack()


class PageThree(tk.Frame):
    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        label = ttk.Label(self, text='Graph Page!', font=LARGE_FONT)
        label.pack(pady=10, padx=10)

        button1 = ttk.Button(self, text='Back to Home', command=lambda: controller.show_frame(StartPage))
        button1.pack()

        canvas = FigureCanvasTkAgg(f, self)
        canvas.draw()
        canvas.get_tk_widget().pack(side=tk.BOTTOM, fill=tk.BOTH, expand=True)

        toolbar = NavigationToolbar2Tk(canvas, self)
        toolbar.update()
        canvas._tkcanvas.pack(side=tk.TOP, fill=tk.BOTH, expand=True)


def main():
    app = DataCore()
    ani = animation.FuncAnimation(f, animate, interval=1000)
    app.mainloop()


if __name__ == '__main__':
    main()
