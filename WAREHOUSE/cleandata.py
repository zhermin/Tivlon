import os, time
import pandas as pd
from functools import reduce
starttime = time.time()
pd.options.mode.chained_assignment = None  # default="warn"
current_folder = os.getcwd()

print(f"updating usd/cny fx file... ({round((time.time() - starttime), 2)}s)")
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
print(f"usd/cny fx file updated... ({round((time.time() - starttime), 2)}s)")


print (f"*reading ports file... ({round((time.time() - starttime), 2)}s)")
try:
    ports_df = pd.read_excel(current_folder + "\\IO PORTS PRICES IN RMB.xlsx", sheet_name=None, skiprows=[0,2,3,4,5,6])
except FileNotFoundError:
    print("\nunable to find ports file [IO PORTS PRICES IN RMB.xlsx], make sure this script and the required files are in the same folder")
    input("press enter to exit >> ")
    exit()

print(f"adding ports prefix to columns... ({round((time.time() - starttime), 2)}s)")
for key in ports_df.keys():
    newcol = ["指标名称"]
    for col in ports_df[key].columns:
        if col != "指标名称":
            newcol.append(key[:-5] + "-" + str(col))

    ports_df[key].columns = newcol

print(f"moving sheet with most rows to front... ({round((time.time() - starttime), 2)}s)")
all_ports_dfs = list(ports_df.values())
rowlengths = [len(all_ports_dfs[sheet]) for sheet in range(len(all_ports_dfs))]
index_most_rows = rowlengths.index(max(rowlengths))
all_ports_dfs.insert(0, all_ports_dfs.pop(index_most_rows))

print(f"merging all sheets in ports file... ({round((time.time() - starttime), 2)}s)")
merged = reduce(lambda left, right: pd.merge(left, right, on="指标名称", how="left"), all_ports_dfs)

print(f"updating timeframe file... ({round((time.time() - starttime), 2)}s)")
pfm_ldr_timeframes = [-4,-9,-14,-19,-24,-29,-39,-49,-59] # pfd_ldr = performance leaders
std_dev_timeframes = [5,10,15,20,25,30,40,50,60]
all_pfm_ldrs = []
all_std_dev = []

for i in range(len(pfm_ldr_timeframes)):
    all_pfm_ldrs.append(merged.select_dtypes(include=['number']).diff(periods=pfm_ldr_timeframes[i]).head(1)) # added "periods=" for clarity
    all_std_dev.append((merged.drop("指标名称", axis=1).iloc[[0]] - merged.head(std_dev_timeframes[i]).mean().to_frame().transpose()) / merged.head(std_dev_timeframes[i]).std().to_frame().transpose())

pfm_ldr_df = pd.concat(all_pfm_ldrs)
std_dev_df = pd.concat(all_std_dev)

timeframe_list = ["5d", "10d", "15d", "20d", "25d", "30d", "40d", "50d", "60d"]
pfm_ldr_df["timeframe"] = timeframe_list
std_dev_df["timeframe"] = timeframe_list

upv_timeframe = pfm_ldr_df.melt(id_vars=["timeframe"], var_name="name", value_name="change in price")
upv_std_dev = std_dev_df.melt(id_vars=["timeframe"], var_name="name", value_name="std from avg")

upv_timeframe[["port", "product"]] = upv_timeframe["name"].str.split("-", expand=True)
upv_timeframe["product"] = upv_timeframe["product"].str.split("品牌").str[0]
upv_timeframe["name"] = upv_timeframe["name"].str.split("-").str[1]
upv_timeframe["std from avg"] = upv_std_dev["std from avg"]

upv_timeframe.to_csv("timeframe.csv", index=False, encoding="utf-8-sig")

print(f"*unpivoting and splitting columns... ({round((time.time() - starttime), 2)}s)")
cleaned = merged.melt(id_vars=["指标名称"], var_name="name", value_name="price (rmb)").dropna()
cleaned.rename(columns={"指标名称" : "date"}, inplace=True)
cleaned[["port", "product"]] = cleaned["name"].str.split("-", expand=True)
cleaned["name"] = cleaned["name"].str.split("-").str[1]
cleaned["percentage"] = cleaned["name"].str.split("%").str[0]
cleaned["product"] = cleaned["product"].str.split("品牌").str[0]

print(f"merging with inputs... ({round((time.time() - starttime), 2)}s)")
try:
    inputs_df = pd.read_csv(current_folder + "\\inputs.csv", usecols=["product","moisture"])
except FileNotFoundError:
    print("\nunable to find inputs file [inputs.csv], make sure this script and the required files are in the same folder")
    input("press enter to exit >> ")
    exit()
merge_inputs = pd.merge(cleaned, inputs_df, on="product", how="left")
merge_inputs["moisture"].fillna(0.08, inplace=True)

print(f"merging with usdcny... ({round((time.time() - starttime), 2)}s)")
try:
    usdcny_df = pd.read_csv(current_folder + "\\usdcny.csv", parse_dates=[0], dayfirst=True)
except FileNotFoundError:
    print("\nunable to find fx file [usdcny.csv], make sure this script and the required files are in the same folder")
    input("press enter to exit >> ")
    exit()
merge_fx = pd.merge(merge_inputs, usdcny_df, on="date", how="left")

# changing data types and calculating the DMTU prices and the usd converted prices
merge_fx[["price (rmb)", "percentage", "moisture", "usdcny"]] = merge_fx[["price (rmb)", "percentage", "moisture", "usdcny"]].apply(pd.to_numeric)
merge_fx["dmtu"] = merge_fx["price (rmb)"]/merge_fx["percentage"]
merge_fx["price (usd)"] = (merge_fx["price (rmb)"]-30)/1.13/(1-merge_fx["moisture"])/merge_fx["usdcny"]

print(f"cleaning index files... ({round((time.time() - starttime), 2)}s)")
try:
    platts_df = pd.read_excel(current_folder + "\\Platts IO Index.xlsx", skiprows=[0,2], sheet_name="Sheet2")
    mb_df = pd.read_excel(current_folder + "\\MBIOI.xls", skiprows=[0,1,2,4,5,6,7], usecols=[0,9])
except FileNotFoundError:
    print("\nunable to find platts file [Platts IO Index.xlsx] or [MBIOI.xls], make sure this script and the required files are in the same folder")
    input("press enter to exit >> ")
    exit()

platts_df = pd.merge(platts_df, mb_df, on="指标名称", how="left")
platts_df.fillna(0, inplace=True)
platts_df.columns = ["date", "P62", "P65", "P58", "P58LA", "MB65"]

inputs_df = pd.read_csv(current_folder + "\\inputs.csv", usecols=["product", "index", "premium+", "premium%"])

print(f"calculating premiums... ({round((time.time() - starttime), 2)}s)")
for index, row in inputs_df.iterrows():
    if pd.isnull(row["premium+"]) and pd.isnull(row["premium%"]):
        pass
    elif pd.isnull(row["premium+"]):
        platts_df[row["index"] + "/" + row["product"]] = platts_df[row["index"]] * row["premium%"]
    else:
        platts_df[row["index"] + "/" + row["product"]] = platts_df[row["index"]] + row["premium+"]

print(f"unpivoting all indices... ({round((time.time() - starttime), 2)}s)")
platts_df = platts_df.melt(id_vars=["date"], var_name="name", value_name="price (usd)").dropna()
platts_df["product"] = platts_df["name"].str.split("/").str[1]
platts_df["product"].fillna("INDEX", inplace=True)
platts_df["port"] = "OFFSHORE"

print(f"*merging with indices... ({round((time.time() - starttime), 2)}s)")
concat = pd.concat([merge_fx, platts_df], sort=False)
concat["date"] = concat["date"].astype("datetime64[ns]")

concat.to_csv("master.csv", index=False, encoding="utf-8-sig")
print(f"\ndone...  ({round((time.time() - starttime), 2)}s)")
input("press enter to exit >> ")