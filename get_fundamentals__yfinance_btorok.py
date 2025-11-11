"""
Be careful when running this code as yfinance is not created by yahoo and
hitting their API will lead to rate limiting for over a day if the endpoint is
hit too frequently.
"""

import os
import time
import requests
import pandas as pd
import yfinance as yf
from io import StringIO


def sp1500_tickers():
    tickers = []
    # set headers and urls for S&P 500, 400, and 600 to get S&P1500 composite
    headers = {"User-Agent": "Mozilla/5.0"}
    url_500 = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    url_400 = "https://en.wikipedia.org/wiki/List_of_S%26P_400_companies"
    url_600 = "https://en.wikipedia.org/wiki/List_of_S%26P_600_companies"

    url_list = [url_500, url_400, url_600]
    tickers = []
    for url in url_list:
        html = requests.get(url, headers=headers).text
        table = pd.read_html(StringIO(html))[0]
        curr_tickers = table["Symbol"].tolist()
        curr_tickers = [t.replace(".", "-") for t in curr_tickers]
        tickers.extend(curr_tickers)
    return tickers


def get_company_info(ticker):
    company_info = yf.Ticker(ticker).get_info()
    return company_info


sp1500_tickers = sp1500_tickers()
sp1500_data = []

for stock in sp1500_tickers:
    curr_company_info = get_company_info(stock)
    time.sleep(0.2)  # avoid rate limiting
    if curr_company_info:
        sp1500_data.append(curr_company_info)

here = os.path.abspath(__file__)
input_dir = os.path.abspath(os.path.join(here, os.pardir))
output_file = os.path.join(input_dir, 'sp1500_company_info.csv')
df_sp1500_companies = pd.DataFrame(sp1500_data)
df_sp1500_companies.to_csv(output_file, index=False)
