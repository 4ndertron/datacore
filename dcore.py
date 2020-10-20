"""
This file was last edited while following the tutorial at this link:
https://pythonprogramming.net/plotting-live-bitcoin-price-data-tkinter-matplotlib/

Progress was stopped in order to fit the current DataHandler object with parallel processing
using the threading module.
"""
import json
import tkinter as tk
import matplotlib
import matplotlib.animation as animation
from modules.data_handler import DataHandler
from tkinter import ttk
from matplotlib import style
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk

LARGE_FONT = ('Verdana', 12)
matplotlib.use('TkAgg')
style.use('ggplot')

f = Figure(figsize=(5, 4), dpi=100)
a = f.add_subplot(111)
dh = DataHandler(creds=json.loads(open('./secrets/creds.json', 'r').read()))


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


class DataCore(tk.Tk):
    def __init__(self, *args, **kwargs):
        tk.Tk.__init__(self, *args, **kwargs)

        tk.Tk.iconbitmap(self,
                         default='datacore.ico')
        tk.Tk.wm_title(self,
                       'DataCore Client')

        container = tk.Frame(self)
        container.pack(side='top',
                       fill='both',
                       expand=True)
        container.grid_rowconfigure(0, weight=1)
        container.grid_columnconfigure(0, weight=1)

        menubar = tk.Menu(container)
        filemenu = tk.Menu(menubar, tearoff=0)
        filemenu.add_command(label="Save settings", command=lambda: popupmsg('Not supported just yet!'))
        filemenu.add_separator()
        filemenu.add_command(label="Exit", command=quit)
        menubar.add_cascade(label="File", menu=filemenu)

        tk.Tk.config(self, menu=menubar)

        self.frames = {}

        for F in (StartPage, DCPage):
            frame = F(container, self)
            self.frames[F] = frame
            frame.grid(row=0,
                       column=0,
                       sticky='nsew')

        self.show_frame(StartPage)

    def show_frame(self, cont):
        frame = self.frames[cont]
        frame.tkraise()


class StartPage(tk.Frame):
    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        label = ttk.Label(self,
                          text='Welcome to the DataCore Visualization Initiation (DCVI)',
                          font=LARGE_FONT)
        label.pack(padx=10,
                   pady=10)

        button = ttk.Button(self,
                            text='Continue',
                            command=lambda: controller.show_frame(DCPage))
        button.pack()

        button2 = ttk.Button(self,
                             text='Abort',
                             command=quit)
        button2.pack()


class DCPage(tk.Frame):
    def __init__(self, parent, controller):
        tk.Frame.__init__(self, parent)
        label = ttk.Label(self,
                          text=dh.engines['sqlite_engine'].engine.url,
                          font=LARGE_FONT)
        label.pack(padx=10,
                   pady=10)

        button1 = ttk.Button(self,
                             text='Back to Home',
                             command=lambda: controller.show_frame(StartPage))
        button1.pack()

        canvas = FigureCanvasTkAgg(f, self)
        canvas.draw()
        canvas.get_tk_widget().pack(side=tk.BOTTOM,
                                    fill=tk.BOTH,
                                    expand=True)

        toolbar = NavigationToolbar2Tk(canvas, self)
        toolbar.update()
        canvas._tkcanvas.pack(side=tk.TOP,
                              fill=tk.BOTH,
                              expand=True)


def main():
    app = DataCore()
    app.geometry('1280x720')
    ani = animation.FuncAnimation(f,
                                  animate,
                                  interval=1000)
    app.mainloop()


if __name__ == '__main__':
    main()
