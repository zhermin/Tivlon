import pandas as pd
from googletrans import Translator

tr = Translator()
#print(tr.translate('안녕하세요').text)

path = "C:\\Users\\ZM\\Desktop\\ports.xlsx"
df = pd.ExcelFile(path)

writer = pd.ExcelWriter("C:\\Users\\ZM\\Desktop\\portsss.xlsx", engine="xlsxwriter", datetime_format="yyyy-mm-dd")
for sheetname in df.sheet_names:
    print(sheetname)
    col = df.parse(sheetname, skiprows=[0,2,3,4,5,6], header=None)

    # for i in range(len(col.iloc[0])):
    #     col.iloc[0,i] = tr.translate(col.iloc[0,i]).text

    col.to_excel(writer, sheet_name=sheetname, index=False, header=0)

writer.save()
writer.close()
print("\nDONE")