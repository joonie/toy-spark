import pandas as pd
from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import requests
import numpy as np

#stock_df = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download', header=0)[0]

#stock_code_name_df = stock_df[['회사명', '종목코드']]

#code_list = stock_code_name_df['종목코드'].values.tolist()

#print(code_list)
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

        #print(finance_index) #['매출액', '영업이익', '당기순이익', '영업이익률', '순이익률', 'ROE(지배주주)', '부채비율', '당좌비율', '유보율', 'EPS(원)', 'PER(배)', 'BPS(원)', 'PBR(배)', '주당배당금(원)', '시가배당률(%)', '배>당성향(%)']

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
        print('---- quarter finance ----')
        print(quarter_finance)
        result_df = ('^'.join(quarter_finance))
        print('---- finance result ----')
        print(result_df)

    except:
        continue

print('------------ res ------------')
#appended_data = pd.concat(appended_data)
#print(appended_data)