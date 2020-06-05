import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import requests
import numpy as np
from subprocess import PIPE, Popen
import os
import pyarrow as pa
import pyarrow.parquet as pq

os.environ['ARROW_LIBHDFS_DIR'] = \
    '/opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib64/'

hdfsClient = pa.hdfs.connect(host='dev-shop-collection-nn-ncl.nfra.io', port=8020, user='irteamsu')

hdfsClient.ls('/user/irteamsu/stock/input')

web_url = 'https://finance.naver.com/item/main.nhn?code=071050'

code_list = ['071050', '000660', '005380', '092130', '051910']

appended_data = []
for code in code_list:
    try:
        with urllib.request.urlopen('https://finance.naver.com/item/main.nhn?code='+str(code)) as response:
            html = response.read()
            soup = BeautifulSoup(html, 'html.parser')

        finance_html = soup.find('table', {'class' : 'tb_type1_ifrs'})
        th_data = [item.get_text().strip() for item in finance_html.select('thead th')]
        annual_date = th_data[3:7]
        quarter_date = th_data[7:13]

        #print(annual_date) #['2017.12', '2018.12', '2019.12', '2020.12(E)']
        #print(quarter_date) #['2018.12', '2019.03', '2019.06', '2019.09', '2019.12', '2020.03(E)']

        finance_index = [item.get_text().strip() for item in finance_html.select('th.h_th2')][3:]

        #print(finance_index) #['매출액', '영업이익', '당기순이익', '영업이익률', '순이익률', 'ROE(지배주주)', '부채비율', '당좌
비율', '유보율', 'EPS(원)', 'PER(배)', 'BPS(원)', 'PBR(배)', '주당배당금(원)', '시가배당률(%)', '배당성향(%)']

finance_data = [item.get_text().strip() for item in finance_html.select('td')]

#print(finance_data) #['66,220', '84,521', '107,713', '', '23,317' ...

finance_data = np.array(finance_data)
finance_data.resize(len(finance_index), 10) #dataframe(row size x column size)
finance_date = annual_date + quarter_date #sum annual_date with quarter_date


#make dataframe using finance_data, finance_index, finance_date
finance = pd.DataFrame(data=finance_data[0:,0:], index=finance_index, columns=finance_date)
#print(finance)
quarter_finance = finance.iloc[:, 2]
quarter_finance.loc['code'] = code
data_str = ('^'.join(quarter_finance))
print('---- finance result ----')
print(data_str)
appended_data.append(data_str)
except:
print('except')
continue

print('------------ res ------------')
result_str = ('|'.join(appended_data))
print(result_str)

tempFilePath = '/home1/irteamsu/work/python/stock/foo2.txt'
outputFilePath = '/user/irteamsu/stock/input/res'

with open(tempFilePath, "w") as f:
    f.write(result_str)

with open(tempFilePath, 'rb') as f_upl:
    hdfsClient.upload(outputFilePath, f_upl)