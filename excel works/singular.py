import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import style
import matplotlib.dates
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

style.use("dark_background")

plt.rcParams['patch.force_edgecolor'] = True
plt.rcParams['patch.facecolor'] = 'b'
plt.rcParams['keymap.pan'] = 'z'
plt.rcParams['keymap.zoom'] = 'c'
plt.rcParams['keymap.back'] = 'a'
plt.rcParams['keymap.forward'] = 'd'
plt.rcParams['keymap.home'] = 'w'

df = pd.read_excel("C:\\Users\\ZM\\Desktop\\ore.xlsx")
df["date"] = pd.to_datetime(df["date"])

mean = df["tj1"].rolling(window=20).mean()
df["tj1_mean"] = mean

std = df["tj1"].rolling(window=20).std()
df["tj1_upper"] = mean + 1.5*std
df["tj1_lower"] = mean - 1.5*std

corr = df["tj1"].rolling(window=30).corr(df["tj2"])
df["tj1 vs tj2"] = corr

def tj():

    plt.figure("Price over Time")

    plt.plot(df["date"], df["tj1"], label="tj1", linewidth=1, color="#00ff00")
    plt.plot(df["date"], df["tj1_mean"], label="tj1_ma", linewidth=.6, color="#00ffff")
    plt.plot(df["date"], df["tj1_upper"], label="tj1_upper", linewidth=.6, color="r")
    plt.plot(df["date"], df["tj1_lower"], label="tj1_lower", linewidth=.6, color="r")

    plt.legend()
    plt.grid(b=True, which='major', color='#666666', linestyle='-')

    datefmt = matplotlib.dates.DateFormatter("%d-%b-%Y")
    fmt = lambda x,y : "{}, {:.3f}".format(datefmt(x), y)
    plt.gca().format_coord = fmt

    plt.xticks(rotation=45)

def tj1_vs_tj2():

    fig, ax = plt.subplots(2,1, sharex=True, gridspec_kw = {'height_ratios':[3, 1]})
    fig.autofmt_xdate()

    ax[0].plot(df["date"], df["tj1"], label="TJ 61.5% Powder", linewidth=1, color="r")
    ax[0].plot(df["date"], df["tj2"], label="TJ 62.5% Block", linewidth=1, color="#00ff00")

    datefmt = matplotlib.dates.DateFormatter("%d-%b-%Y")
    fmt = lambda x,y : "{}, {:.3f}".format(datefmt(x), y)
    fig.gca().format_coord = fmt
    
    ax[1].plot(df["date"], df["tj1 vs tj2"], label="correlation", linewidth=.6, color="#00ffff")

    for i in range(2):
        ax[i].legend()
        ax[i].grid(b=True, which='major', color='#666666', linestyle='-')

tj1_vs_tj2()
wm = plt.get_current_fig_manager()
wm.window.state('zoomed')

plt.tight_layout()
plt.show()