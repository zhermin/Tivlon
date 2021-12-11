import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from matplotlib import style
import matplotlib.dates
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

style.use("dark_background")

#plt.suptitle("Price Differences over Time")
plt.rcParams['patch.force_edgecolor'] = True
plt.rcParams['patch.facecolor'] = 'b'
plt.rcParams['keymap.pan'] = 'z'
plt.rcParams['keymap.zoom'] = 'c'
plt.rcParams['keymap.back'] = 'a'
plt.rcParams['keymap.forward'] = 'd'
plt.rcParams['keymap.home'] = 'w'


df = pd.read_excel("C:\\Users\\ZM\\Desktop\\ore.xlsx")
df["date"] = pd.to_datetime(df["date"])

#print(df.head())
#print(df.describe())

df["rzdiff"] = df["rz2"] - df["rz1"]
df["tjdiff"] = df["tj2"] - df["tj1"]
df["615diff"] = df["rz1"] - df["tj1"]
df["625diff"] = df["rz2"] - df["tj2"]

# plt.xlabel("Date")
# plt.ylabel("Price Difference")

#plt.scatter(df["date"], df["diff"], label="Price Difference over Time", marker=".", s=10)

fig, ax = plt.subplots(2,1, sharex=True)
fig.autofmt_xdate()

all_axis = [(0,0), (0,1), (1,0), (1,1)]
names = ["tj1", "tj2", "rz1", "rz2"]
colors = ["#00ff00", "#00ff00", "r", "b"]


#print(df["rz1_ma"])

for i in range(2):

    mean = df[f"{names[i]}"].rolling(window=20).mean()
    df[f"{names[i]}_ma"] = mean

    std = df[f"{names[i]}"].rolling(window=20).std()
    df[f"{names[i]}_upper"] = mean + 2*std
    df[f"{names[i]}_lower"] = mean - 2*std

    corr = df["tj1"].rolling(window=20).corr(df["tj2"])
    df["tj1 vs tj2"] = corr

    ax[i].plot(df["date"], df[names[i]], label=names[i], linewidth=1, color=colors[i])
    ax[i].plot(df["date"], df[f"{names[i]}_ma"], label=f"{names[i]}_ma", linewidth=.6, color="#00ffff")
    ax[i].plot(df["date"], df[f"{names[i]}_upper"], label=f"{names[i]}_upper", linewidth=.6, color="r")
    ax[i].plot(df["date"], df[f"{names[i]}_lower"], label=f"{names[i]}_lower", linewidth=.6, color="r")

    ax[i].legend()
    ax[i].grid(b=True, which='major', color='#666666', linestyle='-')
    # ax[all_axis[i]].minorticks_on()
    # ax[all_axis[i]].grid(b=True, which='minor', color='#999999', linestyle='-', alpha=0.2)

    datefmt = matplotlib.dates.DateFormatter("%d-%b-%Y")
    fmt = lambda x,y : "{}, {:.3f}".format(datefmt(x), y)
    plt.gca().format_coord = fmt

    # ax[0,1].plot(df["date"], df["rz2"], label="RZ2", linewidth=1)
    # ax[1,0].plot(df["date"], df["tj1"], label="TJ1", linewidth=1)
    # ax[1,1].plot(df["date"], df["tj2"], label="TJ2", linewidth=1)

# plt.plot(df["date"], df["rzdiff"], label="RZ [62.5PB - 61.5PB]", linewidth=1)
# plt.plot(df["date"], df["tjdiff"], label="TJ [62.5PB - 61.5PB]", linewidth=1)
# plt.plot(df["date"], df["625diff"], label="RZ - TJ [62.5PB]", linewidth=1)
# plt.plot(df["date"], df["615diff"], label="RZ - TJ [61.5PB]", linewidth=1)

#sns.distplot(df["diff"])
#sns.regplot(df["price1"], df["price2"], data=df)

wm = plt.get_current_fig_manager()
wm.window.state('zoomed')

plt.tight_layout()
plt.show()