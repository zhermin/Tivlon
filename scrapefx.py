import os, time
import pandas as pd
starttime = time.time()
current_folder = os.getcwd()
print("updating fx file with the latest data...")

url = "https://www.exchangerates.org.uk/USD-CNY-exchange-rate-history.html"
try:
    df = pd.read_html(url)[0]
except:
    print("something went wrong... check if you have a working internet connection and try again...")
    input("press enter to exit >> ")
    exit()

fxtable = df.drop([0,3], axis=1)
fxtable = fxtable[2:-1]
fxtable = fxtable[[2,1]]
fxtable[2] = fxtable[2].str.split(" ").str[-1]
fxtable[1] = fxtable[1].str.split(" ").str[3]
fxtable.columns = ["date", "usdcny"]
fxtable["date"] = pd.to_datetime(fxtable["date"], dayfirst=True)

try:
    usdcny_df = pd.read_csv(current_folder + "\\usdcny.csv", parse_dates=[0], dayfirst=True)
except FileNotFoundError:
    print("\nunable to find inputs file [usdcny.csv], make sure this script and the required files are in the same folder")
    input("press enter to exit >> ")
    exit()

updatefx = pd.concat([usdcny_df, fxtable], sort=False)
updatefx["date"] = updatefx["date"].astype("datetime64[ns]")
updatefx.drop_duplicates(subset="date", keep="first", inplace=True)
updatefx.sort_values(by="date", inplace=True, ascending=False)
updatefx.to_csv("usdcny.csv", index=False)
print(f"\ndone...  ({round((time.time() - starttime), 2)}s)")
input("press enter to exit >> ")