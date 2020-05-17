import pandas as pd
from bs4 import BeautifulSoup
import urllib.request
import urllib.parse

# excel 파일을 다운로드하는거와 동시에 pandas에 load하기
# 흔히 사용하는 df라는 변수는 data frame을 의미합니다.
stock_df = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download', header=0)[0]

stock_code_name_df = stock_df[['회사명', '종목코드']]


#print(stock_code_name_df.head())

web_url = 'https://finance.naver.com/item/coinfo.nhn?code=071050&target=finsum_more'
# web_url에 원하는 웹의 URL을 넣어주시면 됩니다.
with urllib.request.urlopen(web_url) as response:
    html = response.read()
    soup = BeautifulSoup(html, 'html.parser')

#not works yet...
table = soup.find('table',{'class':'gHead01 all-width','summary':'주요재무정보를 제공합니다.'})

print(table)