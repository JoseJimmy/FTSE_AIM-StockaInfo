import pandas as pd
import re
ftseaim = pd.read_csv('C:\Python\HLG_CompInfo\hlinfo\FtseAim_Crawled.csv')
temp = list(ftseaim.metaval.values)
dict_list=[]
powers = {'billion': 10 ** 9, 'million': 10 ** 6}


def f(num_str):
    match = re.search(r"([0-9.]+)\s?( million| billion)", num_str)
    if match is not None:
        quantity = match.group(1)
        magnitude = match.group(2).strip()
        return float(quantity) * powers[magnitude]


for items in temp:
    itemdict = {}
    for item  in items.split('\n'):
        if('EPIC' in item):
            itemdict['EPIC'] =item.split(':')[1].strip()
        if('Market cap' in item):
            itemdict['MKT_CAP'] =f(item.split(':')[1].strip())
        if('Shares in issue' in item):
            itemdict['SHARES_ISSUED'] =f(item.split(':')[1].strip())
        if('Exchange' in item):
            itemdict['EXCHANGE'] =item.split(':')[1].strip()
        if('Indices' in item):
            itemdict['INDICES'] =item.split(':')[1].strip()
        if('Currency' in item):
            itemdict['CURRENCY'] =(item.split(':')[1].strip())
        if('SIN' in item):
            itemdict['ISIN'] =item.split(':')[1].strip()
    dict_list.append(itemdict)
temp=pd.DataFrame(dict_list)
ftseaim = pd.merge(ftseaim,temp,how='inner',on='EPIC')
ftseaim = ftseaim.drop(['metaval'],axis=1)

ftseallshare = pd.read_csv('C:\Python\HLG_CompInfo\hlinfo\FtseAllShare_Crawled.csv')
temp = list(ftseallshare.metaval.values)
dict_list=[]
for items in temp:
    itemdict = {}
    for item  in items.split('\n'):
        if('EPIC' in item):
            itemdict['EPIC'] =item.split(':')[1].strip()
        if('Market cap' in item):
            itemdict['MKT_CAP'] =f(item.split(':')[1].strip())
        if('Shares in issue' in item):
            itemdict['SHARES_ISSUED'] =f(item.split(':')[1].strip())
        if('Exchange' in item):
            itemdict['EXCHANGE'] =item.split(':')[1].strip()
        if('Indices' in item):
            itemdict['INDICES'] =item.split(':')[1].strip()
        if('Currency' in item):
            itemdict['CURRENCY'] =(item.split(':')[1].strip())
        if('SIN' in item):
            itemdict['ISIN'] =item.split(':')[1].strip()
    dict_list.append(itemdict)
temp=pd.DataFrame(dict_list)
ftseallshare = pd.merge(ftseallshare,temp,how='inner',on='EPIC')
ftseallshare = ftseallshare.drop(['metaval'],axis=1)

ftseallshare.to_pickle('ftseallshare.pkl')
ftseaim.to_pickle('ftseaim.pkl')
ftseallshare.to_csv('FtseAllShareDetails_HL.csv')