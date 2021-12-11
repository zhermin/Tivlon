# Tivlon Internship (2019-2020)
_A backup for the work done during my internship at Tivlon_

---

## Description

My work as a Data Scientist/Engineer starts from the large raw excel files I receive. They contain historical prices of commodities such as Iron Ores from ports in China. My job was to clean and restructure the data into a format that Business Intelligence (BI) tools such as Tableau and Power BI can read. This means that the data have to be unpivoted from a 2D format to a 1D format. 

Once the data have been wrangled using the ELT pipeline I built using Python's data manipulation libraries, the output is saved back into CSV format for the BI tools to read and be easily updated. Building the dashboards then entails organising the theming, selected data to be displayed and some ad-hoc code using the BI tools' proprietary language such as DAX for Power BI. Power BI also allows sharing of the dashboards online which you can access through the link below. 

## Most Recent Technologies Used during Tivlon (~2020)
[View Tivlon's Dashboard](https://app.powerbi.com/view?r=eyJrIjoiZmFkM2I4YTAtNTVhMS00OTVjLTkzODItMWJlODI0N2MyNzZhIiwidCI6IjVjMGI1NTc0LTQ1NzUtNGE3Ni04OTdkLWZiNDczZDA0MGZkZiIsImMiOjEwfQ%3D%3D&pageName=ReportSection40f0aac501bb4eddea58 "Tivlon Power BI Dashboard")

* Python
    * Jupyter: Current Notebook [Here](https://nbviewer.jupyter.org/github/zhermin/data-science/blob/master/tivlon/WAREHOUSE%20NEW/cleandata.ipynb "Current Jupyter Notebook on Iron Ore Futures")
    * Pandas - For data cleaning and exporting to CSV files
    * Numpy - For certain cell operations
    * Dask - For huge datasets that pandas cannot handle
* Microsoft Power BI - For data visualisation

## Previous Technologies Used during Tivlon (~2019)
* Microsoft Power Query - For data cleaning
* Tableau - For data visualisation
* Python
    * Jupyter: Old Notebook [Here](https://nbviewer.jupyter.org/github/zhermin/data-science/blob/master/tivlon/futures/futures.ipynb "Previous Jupyter Notebook on Iron Ore Futures")

_Jupyter Viewer generated using [nbviewer](https://nbviewer.jupyter.org/ "Jupyter's NBViewer Tool")_\
_Add "?flush_cache=true" to the end of Jupyter Notebook url if unable to view changes_