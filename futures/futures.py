import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
#plt.rcParams['figure.figsize'] = [30, 15]

df = pd.read_excel("C:\\Users\\ZM\\Desktop\\futures\\DCE Iron Ore Datas.xls", skiprows=[1], skipfooter=1)
df.dropna(how="all", inplace=True)
df.columns=["date", "price", "closing", "vol", "oi"]
df = df[(df['date'] >= '2018-1-1')]

df["price %chg"] = (df["price"].pct_change(1)*100).round(2)

df["vol %chg"] = (df["vol"].pct_change(1)*100).round(2)
df["vol sma"] = df["vol"].rolling(5).mean()
df["vol std"] = df["vol"].rolling(5).std().round(2)
df["vol std chg"] = (df["vol"] - df["vol"].rolling(10).mean()) / df["vol"].rolling(10).std()

df["oi %chg"] = (df["oi"].pct_change(1)*100).round(2)
df["oi sma"] = df["oi"].rolling(5).mean()

fig, ax1 = plt.subplots()
plt.xticks(rotation=45)

color="red"
ax1.set_xlabel('date')
ax1.set_ylabel('price', color=color)
ax1.plot(df["date"], df["price"], color=color, linewidth=3, label="price")
ax1.tick_params(axis='y', labelcolor=color)

ax2 = ax1.twinx()

color="tab:blue"
ax2.plot(df["date"], df["vol sma"], color=color, label="vol 5d sma")
ax2.plot(df["date"], df["oi sma"], color="tab:orange", label="oi 5d sma")
ax2.tick_params(axis='y', labelcolor=color)

fig.tight_layout()
fig.legend(loc=9, prop={'size': 10})
plt.show()