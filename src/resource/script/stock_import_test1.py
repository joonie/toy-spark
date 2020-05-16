import pandas as pd

def get_url(code):
    url = 'http://finance.naver.com/item/sise_day.nhn?code={code}'.format(code=code)

    print("요청 URL = {}".format(url))
    return url


stock_code_list = ['000660', '017670']


df = pd.DataFrame()

for idx in range(0,2):
    url = get_url(stock_code_list[idx])
    df = df.append(pd.read_html(url, header=0)[0], ignore_index=True)

df = df.dropna()

print(df)