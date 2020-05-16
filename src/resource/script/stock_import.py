import pandas as pd


# excel 파일을 다운로드하는거와 동시에 pandas에 load
stock_df = pd.read_html('http://kind.krx.co.kr/corpgeneral/corpList.do?method=download', header=0)[0]

stock_code_name_df = stock_df[['회사명', '종목코드']]

print(stock_code_name_df.head())