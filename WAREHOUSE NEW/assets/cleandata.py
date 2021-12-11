import logging, os

## logging settings (did not use getLogger)
## logging configs need to be above all lib imports since they might have their own logging capabilities
## very basic logging - just saves errors to debug.log file
logging.basicConfig(
    filename=os.path.join("assets", "debug.log"),
    filemode="a", 
    level=logging.WARNING, 
    format="%(asctime)s — %(levelname)s — %(message)s\n", 
    datefmt="%d %b %Y %H:%M:%S"
)

import pandas as pd
import numpy as np
import dask
import dask.dataframe as dd

import pinyin as py
import pinyin.cedict as pyc
import re
import time

## performs A-B differentials on every product pair without duplicates (no B-A)
## https://stackoverflow.com/questions/44147284/fastest-way-to-calculate-difference-in-all-columns

def triu(df, pandas_mode):
    arr = df.values
    r,c = np.triu_indices(arr.shape[1],1)
    cols = df.columns
    sep = " – " if pandas_mode else " - "
    nm = [cols[i] + sep + cols[j] for i,j in zip(r,c)]
    return pd.DataFrame(arr[:,r] - arr[:,c], columns=nm) if pandas_mode else dd.from_array(arr[:,r] - arr[:,c], columns=nm)

## basically wraps the functionality of pd.read_excel() with try/except clause to loop through folders if the files are not found
## takes in dict of the pd.read_excel() arguments and pass it as file_args which is then unpacked using **
def openfile(dir_matches, filename, file_args):
    for i in range(len(dir_matches) - 1):
        try:
            ## reads from /WAREHOUSE NEW/Raw Excel Data/latest_directory/filename
            print (f"\nopening [{filename}] from /{dir_matches[i]}...")
            df = pd.read_excel(
                os.path.join(
                    os.path.abspath(".."), 
                    "Raw Excel Data", 
                    dir_matches[i], 
                    filename
                ), **file_args
            ) 
        except FileNotFoundError:
            logging.exception("Exception (File Not Found)")
            print(f"\n[ERROR] unable to find '{filename}' from the folder /{dir_matches[i]}")
            print(f"trying to find the file in the next latest folder /{dir_matches[i+1]}")
        except Exception as e:
            logging.exception(f"Uncaught exception occurred while trying to open [{filename}]")
            print("\n[ERROR] an uncaught error has occurred, please contact the admin")
            print(f"trying the next latest folder /{dir_matches[i+1]}")
        else:
            print(f"file opened successfully... ({round((time.time() - starttime), 2)}s)")
            return df


try:
    starttime = time.time()

    # --------------------------------------------- GET LATEST FOLDER --------------------------------------------- #

    ## ...abspath("..") navigates to parent directory (dir) from current dir (assumed to be "/WAREHOUSE NEW")
    ## then joins with desired dir of all the raw excel data to get all subdirectories in folder "/Raw Excel Data"
    ## latest dir and name is retrieved from last item in the sorted + regex-matched list
    rawdata_path = os.path.join(os.path.abspath(".."), "Raw Excel Data")
    dir_names = sorted([d.path for d in os.scandir(rawdata_path) if d.is_dir()])
    dirnames_split = [d.split("\\")[-1] for d in dir_names]

    ## regex match [4 nums + WK + 2 nums + no space afterwards], eg. 2020WK30
    dir_regex = r"[0-9]{4}WK[0-9]{2}(?! )"
    dir_matches = [d for d in dirnames_split if re.match(dir_regex, d)][::-1]

    print(f"latest folder: /{dir_matches[0]}")
    print(f"please check if that is the latest version...")

    # ---------------------------------------------- GET KEY PRODUCTS --------------------------------------------- #

    keyproducts_file = "key_products.csv"
    print(f"\nopening [{keyproducts_file}]...")
    key_products = pd.read_csv(os.path.join("assets", keyproducts_file), encoding="utf-8")

    lines = key_products["product"].tolist()
    lines.insert(0, "指标名称")
    usecols = lambda x: any(line in x for line in lines)

    print(f"file opened successfully... ({round((time.time() - starttime), 2)}s)")

    # ----------------------------------------------- GET PORTS DATA ---------------------------------------------- #

    ports_df = openfile(
        dir_matches, 
        "IO PORTS PRICES IN RMB.xlsx", 
        {
            "sheet_name":"Rizhao Port", 
            "skiprows":[0,2,3,4,5,6],
            "usecols":usecols,
            "encoding":"utf-8"
        }
    )
    
    date_col = ports_df.iloc[:,0]
    dateless_df = ports_df.drop("指标名称", axis=1)

    ## remove repeated characters at end of string and pre-existing abbr like (BRBF) etc. since the abbr will be added manually below
    dateless_df.columns = [header.split("品牌")[0].split("（")[0] for header in dateless_df.columns]
    dateless_df = dateless_df[key_products["product"].str.split("（").str[0]]
    dateless_df.columns = [f"{dateless_df.columns[i]} ({key_products['abbr'][i]})" for i in range(len(dateless_df.columns))]

    ports_df = dateless_df.copy()
    ports_df.insert(loc=0, column="指标名称", value=date_col)

    # ------------------------------------------- GET STEEL MARGIN DATA ------------------------------------------- #

    steel_margin = openfile(
        dir_matches,
        "Steel Product Margin.xlsx", 
        {
            "sheet_name":"黑色利润体系", 
            "header":None, 
            "skiprows":[0], 
            "usecols":[5,34,36], 
            "names":["指标名称", "hrc", "rebar"],
            "encoding":"utf-8"
        }
    ).dropna()

    # ------------------------------------------- CREATE PORTS_UPV.CSV -------------------------------------------- #

    print("\nmerging and unpivoting ports + steel margin data...")
    ports_df = ports_df.merge(steel_margin, on="指标名称", how="left")
    ports_df = ports_df.melt(id_vars="指标名称", var_name="product", value_name="price").dropna().reset_index(drop=True)
    ports_df.columns = ["date", "product", "price"]

    print(f"\nsaving file... ({round((time.time() - starttime), 2)}s)")
    ports_df.to_csv(os.path.join(os.getcwd(), "cleaned_data", "ports_upv.csv"), index=False, encoding="utf-8-sig")
    print(f"[ports_upv.csv] saved successfully... ({round((time.time() - starttime), 2)}s)")

    # ---------------------------------------- GET DIFFERENTIALS DATAFRAME ---------------------------------------- #

    ## dataframe of A-B differential combinations using pairwise function above

    ## if duplicates not needed, ie. (A-B) but no (B-A), just pairwise(dateless_df) once
    ## duplicates were requested instead, so pairwise a 2nd time on the inverse of the dataframe
    print("\ncalculating every single product differential...")
    diff_df = pd.concat([triu(dateless_df, True), triu(dateless_df.iloc[:, ::-1], True)], axis=1)
    # diff_df = triu(dateless_df, True)
    print(f"total number of product pairs = {len(diff_df.columns)}... ({round((time.time() - starttime), 2)}s)")

    # -------------------------------------------- CREATE DIFF_UPV.CSV -------------------------------------------- #

    ## timeseries differentials data for line charts // able to include timeseries analysis like MOVING AVGS
    print("\ncleaning and unpivoting all product differentials...")
    diff_upv = diff_df.copy()
    diff_upv.insert(loc=0, column="date", value=date_col)

    diff_upv = diff_upv.melt(id_vars="date", var_name="product_diff", value_name="price_diff").dropna().reset_index(drop=True)
    diff_upv[["product_A","product_B"]] = diff_upv["product_diff"].str.split(" – ", expand=True)
    diff_upv = diff_upv[["date", "product_A", "product_B", "product_diff", "price_diff"]]

    print(f"\nsaving file... ({round((time.time() - starttime), 2)}s)")
    diff_upv.to_csv(os.path.join(os.getcwd(), "cleaned_data", "diff_upv.csv"), index=False, encoding="utf-8-sig")
    print(f"[diff_upv.csv] saved successfully... ({round((time.time() - starttime), 2)}s)")

    # ------------------------------------------ CREATES WEEKLY_DIFF.CSV ------------------------------------------ #

    print("\nextracting weekly product differential data and unpivoting them...")
    weekly_diff = diff_df.copy()

    diff_minmax = weekly_diff.agg([min, max])
    diff_minmax["header"] = ["Min", "Max"]
    diff_minmax["week"] = ["Min", "Max"]

    weekly_diff["date"] = date_col
    weekly_diff["header"] = date_col.dt.strftime("%#d/%#m/%y") # WK%V")
    weekly_diff["week"] = date_col.dt.strftime("%YWK%V")

    weekly_diff["weekday"] = weekly_diff["date"].dt.dayofweek
    latest_weekday = weekly_diff["weekday"][0] # 0-6: sunday-monday
    weekly_diff = weekly_diff[weekly_diff["weekday"] == latest_weekday] # extracts every latest_weekday row
    weekly_diff = weekly_diff.drop("weekday", 1) # delete weekday column because it won't be used anymore

    weekly_diff = pd.concat([diff_minmax, weekly_diff], sort=False)
    weekly_diff = weekly_diff.reset_index(drop=True).reset_index()

    weekly_diff = weekly_diff.melt(id_vars=["index", "week", "date", "header"], var_name="product_diff", value_name="price_diff").dropna(subset=["price_diff"]).reset_index(drop=True)
    weekly_diff[["product_A","product_B"]] = weekly_diff["product_diff"].str.split(" – ", expand=True)
    weekly_diff = weekly_diff[["index", "week", "date", "header", "product_A", "product_B", "product_diff", "price_diff"]]
    weekly_diff = weekly_diff.bfill()

    print(f"\nsaving file... ({round((time.time() - starttime), 2)}s)")
    weekly_diff.to_csv(os.path.join(os.getcwd(), "cleaned_data", "weekly_diff.csv"), index=False, encoding="utf-8-sig")
    print(f"[weekly_diff.csv] saved successfully... ({round((time.time() - starttime), 2)}s)")

    # ------------------------------------------- CREATES STEEL_INV.CSV ------------------------------------------- #

    ## note: steel inventory data is already in weekly basis so no additional extraction is required
    steel_inv = openfile(
        dir_matches,
        "Steel Product Inventory across each cities.xlsx", 
        {
            "sheet_name":"Rebar", 
            "skiprows":[0,2,3,4,5,6],
            "encoding":"utf-8"
        }
    )

    print("\ncollating weekly steel inventory data and unpivoting them...")
    steel_inv_cols = steel_inv.columns.tolist()
    steel_inv_cols = [col[8:-3] for col in steel_inv_cols[1:]]
    steel_inv_cols = ["date"] + [f'{word.split("：")[0]} ({pyc.translate_word(word.split("：")[0])[0].title().split(" ")[0]}) - {word.split("：")[1]} ({py.get(word.split("：")[1], format="strip", delimiter=" ").title().replace(" ", "")})' for word in steel_inv_cols]

    ## verbose version of above's list comprehension
    # steel_inv_temp = []
    # for word in steel_inv_cols:
    #     district = word.split("：")[0]
    #     city = word.split("：")[1]
    #     translated_district = pyc.translate_word(district)[0].title().split(" ")[0]
    #     translated_city = py.get(city, format="strip", delimiter=" ").title().replace(" ", "")
    #     steel_inv_temp.append(f"{district} ({translated_district}) - {city} ({translated_city})")
    # steel_inv_cols = ["date"] + steel_inv_temp

    steel_inv.columns = steel_inv_cols

    steelinv_date = steel_inv.iloc[:,0]
    steel_inv = steel_inv.drop("date", axis=1)

    districts = list(set([col.split(" - ")[0] for col in steel_inv.columns if " - " in col]))
    for district in districts: steel_inv[f"{district} - TOTAL"] = steel_inv[[col for col in steel_inv.columns if district in col]].sum(axis=1)
    steel_inv = steel_inv[steel_inv.columns.sort_values()]

    steel_inv = steel_inv.pct_change(-1).round(2).replace([np.inf, -np.inf], np.nan)

    steelinv_minmax = steel_inv.agg([min, max])
    steelinv_minmax["header"] = ["Min", "Max"]
    steelinv_minmax["week"] = ["Min", "Max"]

    steel_inv.insert(loc=0, column="date", value=steelinv_date)
    steel_inv["header"] = steel_inv["date"].dt.strftime("%#d/%#m/%y") # WK%V")
    steel_inv["week"] = steel_inv["date"].dt.strftime("%YWK%V")

    steel_inv = pd.concat([steelinv_minmax, steel_inv], sort=False)
    steel_inv = steel_inv.reset_index(drop=True).reset_index()

    steel_inv = steel_inv.melt(id_vars=["index", "week", "date", "header"], var_name="location", value_name="% chg")
    steel_inv["date"] = steel_inv["date"].bfill()
    steel_inv[["district","city"]] = steel_inv["location"].str.split(" - ", expand=True)
    steel_inv = steel_inv[["index", "week", "date", "header", "district", "city", "location", "% chg"]]

    print(f"\nsaving file... ({round((time.time() - starttime), 2)}s)")
    steel_inv.to_csv(os.path.join(os.getcwd(), "cleaned_data", "steel_inv.csv"), index=False, encoding="utf-8-sig")
    print(f"[steel_inv.csv] saved successfully... ({round((time.time() - starttime), 2)}s)")

    # ------------------------------------------- CREATES DIFF_DIFF.CSV ------------------------------------------- #

    def create_diffdiff():
        print("\ngenerating spreads vs spreads...")

        diff_diff = triu(dateless_df, True)

        ## merge abbr together, eg. "XXXX (PBF) - XXXX (SSF)" to "XXXX - XXXX (PBF-SSF)"
        diff_diff.columns = [f'{h.split(" (")[0]}{h.split(" (")[1].split(")")[1]} ({h.split(" (")[1].split(")")[0]}-{h.split(" (")[2]}' for h in diff_diff.columns]

        ## verbose version of above's list comprehension
        # new_headers = []
        # for header in diff_diff.columns:
        #     split_header = header.split(" (")
        #     product_A = split_header[0]
        #     split_section = split_header[1].split(")")
        #     product_B = split_section[1]
        #     new_headers.append(f"{product_A}{product_B} ({split_section[0]}-{split_header[2]}")
        # diff_diff.columns = new_headers

        diff_diff = triu(diff_diff, False)

        diff_diff = diff_diff.repartition(npartitions=200)
        diff_diff = diff_diff.reset_index(drop=True)

        dd_date_col = dd.from_array(date_col)
        dd_date_col = dd_date_col.repartition(npartitions=200)
        dd_date_col = dd_date_col.reset_index(drop=True)

        diff_diff = diff_diff.assign(date = dd_date_col)

        diff_diff = dd.melt(diff_diff, id_vars="date", var_name="product_diff", value_name="price_diff").dropna().reset_index(drop=True)

        diff_diff["product_diff"] = diff_diff["product_diff"].astype("category")

        diff_diff["differential_A"] = diff_diff["product_diff"].str.partition(" - ")[0]
        diff_diff["differential_B"] = diff_diff["product_diff"].str.partition(" - ")[2]

        print(f"\nsaving file... ({round((time.time() - starttime), 2)}s)")
        dd.to_csv(
            df=diff_diff, 
            filename=os.path.join(os.getcwd(), "cleaned_data", "diff_diff.csv"),
            index=False, 
            single_file=True, 
            encoding="utf-8-sig",
            chunksize=10000
        )
        print(f"[diff_diff.csv] saved successfully... ({round((time.time() - starttime), 2)}s)")

    while True:
        print("\nupdating the spreads vs spreads file may take >5mins")
        print("you can skip this step by entering 'n'")
        clean_diffdiff = input("do you want to update the spreads vs spreads file? (type y/n) >> ").lower()
        if clean_diffdiff == "y": 
            create_diffdiff()
            break
        elif clean_diffdiff == "n":
            break
        else: 
            print("type either y or n")
            continue


    # ------------------------------------------- END OF CLEANING ------------------------------------------- #

except MemoryError:
    logging.exception("Memory Error")
    print(f"\n[ERROR] ran out of memory, try to close some programs and rerun")
    input("press enter to exit >> ")
    exit()
except Exception as e:
    logging.exception(f"Uncaught exception occurred in the main program")
    print("\n[ERROR] an uncaught error has occurred, please contact the admin")
    input("press enter to exit >> ")
    exit()
else:
    logging.shutdown()
    print(f"\ndata cleaned with 0 errors...  (total: {round((time.time() - starttime), 2)}s)")
    input("press enter to exit >> ")