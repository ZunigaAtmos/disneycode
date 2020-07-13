# -*- coding: utf-8 -*-
"""
Created on Sat Jun 27 16:32:14 2020

@author: jpkal
"""
import pandas as pd


df_pirates_all = pd.read_csv(
    "https://cdn.touringplans.com/datasets/pirates_of_caribbean_dlr.csv",usecols=['date','datetime','SPOSTMIN'],
    parse_dates=['date', 'datetime'], 
)
df_pirates_all['ride'] = 'pirates'
df_pirates_all['open'] = ~((df_pirates_all['SPOSTMIN'] == -999))

df_pirates = df_pirates_all.set_index('datetime').sort_index()
df_pirates = df_pirates.loc['2017-01-01 06:00':'2017-02-01 00:00']
df_pirates = df_pirates.resample('15Min').ffill()


df_star_tours_all = pd.read_csv(
    "https://cdn.touringplans.com/datasets/star_tours_dlr.csv", usecols=['date','datetime','SPOSTMIN'],
    parse_dates=['date', 'datetime']
)
df_star_tours_all['ride'] = 'star_tours'
df_star_tours_all['open'] = ~((df_star_tours_all['SPOSTMIN'] == -999))

df_star_tours = df_star_tours_all.set_index('datetime').sort_index()
df_star_tours = df_star_tours.loc['2017-01-01 06:00':'2017-02-01 00:00']
df_star_tours = df_star_tours.resample('15Min').ffill()

df_space_all = pd.read_csv(
    "https://cdn.touringplans.com/datasets/space_mountain_dlr.csv", usecols=['date','datetime','SPOSTMIN'], 
    parse_dates=['date', 'datetime']
)
df_space_all['ride'] = 'space'
df_space_all['open'] = ~((df_space_all['SPOSTMIN'] == -999))

df_space = df_space_all.set_index('datetime').sort_index()
df_space = df_space.loc['2017-01-01 06:00':'2017-02-01 00:00']
df_space = df_space.resample('15Min').ffill()


df_nemo_all = pd.read_csv(
    "https://cdn.touringplans.com/datasets/finding_nemo_subs.csv", usecols=['date','datetime','SPOSTMIN'], 
    parse_dates=['date', 'datetime']
)
df_nemo_all['ride'] = 'nemo'
df_nemo_all['open'] = ~((df_space_all['SPOSTMIN'] == -999))

df_nemo = df_nemo_all.set_index('datetime').sort_index()
df_nemo = df_nemo.loc['2017-01-01 06:00':'2017-02-01 00:00']
c = df_nemo.groupby(level=0).transform("count")
c[c["date"]>1].index.tolist()
df_nemo = df_nemo[~df_nemo.index.isin(c[c["date"]>1].index.tolist())].resample('15Min').ffill()

df_pirates["dataset"]="pirates"
df_star_tours["dataset"]="star_tours"
df_space["dataset"] = "space"
df_nemo["dataset"] = "nemo"
all_data = pd.concat([df_pirates, df_star_tours, df_space, df_nemo]).reset_index()
all_data = (
    all_data
        # Drop any "NaN" values in the column 'ride'
        .dropna(subset=['ride', ])
        # Make datetime and ride a "Multi-Index"
        .set_index(['datetime',"dataset"])
        # Choose the column 'SPOSTMIN'
        ['SPOSTMIN']
        # Take the last index ('ride') and rotate to become column names
        .unstack()
)
flat_data = all_data.reset_index()
for month, group in flat_data.groupby(pd.Grouper(freq='M')):
    with pd.ExcelWriter(f'{month.strftime("%B %Y")}a.xlsx') as writer:
        for day, dfsub in group.groupby(pd.Grouper(freq='D')):
            dfsub.to_excel(writer,sheet_name=str(day.date()))