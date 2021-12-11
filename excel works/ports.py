import pandas as pd
from functools import reduce

path = "C:\\Users\\ZM\\Desktop\\portstrans.xlsx"
df = pd.read_excel(path, skiprows=[0,2,3,4,5,6], sheet_name=None, ignore_index=True)
#print(df.keys())

for key in df.keys():
    for i in range(len(df[key].iloc[0])):
        df[key].iloc[0,0] = "port"
        df[key].iloc[0,i] = key
    #df[key].insert(1, "port", key) 

dfs = list(df.values())
merged = reduce(lambda left, right: pd.merge(left, right, on='Indicator name', how='left'), dfs)
merged.transpose().to_csv("merged.csv", header=None, date_format='%Y/%m/%d')

# output = pd.DataFrame()
# for sheetname in xlsx.sheet_names:
#     print(sheetname)
#     df = pd.read_excel(path, skiprows=[0,2,3,4,5,6], header=None, sheet_name=sheetname)
#     output.append(df)

# print(output.head())

# for sheetname in df.sheet_names:
#     print(sheetname)
#     col = df.parse(sheetname, skiprows=[0,2,3,4,5,6], header=None)
#     col.to_excel(writer, sheet_name="merged", index=False, header=0)

# writer.save()
# writer.close()
print("\nDONE")