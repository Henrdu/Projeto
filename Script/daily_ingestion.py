import requests
from datetime import datetime, date, timedelta
import json

# BITCOIN - Qwsogvtv82FCd
# ETH - razxDUgYGNAdQ
# ADA - qzawljRxB5bYu
# BRL - n5fpnvMGNsOS

def get_date_list():
    date_list = []
    for i in range(0,10):
        data =  (datetime.now() - timedelta(days=i)).date()
        date_list.append(data)
    return date_list

def get_unixtimestamp_list():
    unix_timestamp_list = []
    date_list = get_date_list()
    for date in date_list:
        date = str(date) + ' 21:00:00'
        formated_date = datetime.strptime(date,"%Y-%m-%d %H:%M:%S")
        unix_timestamp = datetime.timestamp(formated_date)
        unix_timestamp_list.append(unix_timestamp)
    return unix_timestamp_list


def api_request():
    unix_timestamp_list = get_unixtimestamp_list()
    currencies = ['Qwsogvtv82FCd', 'razxDUgYGNAdQ', 'qzawljRxB5bYu']
    for currency in currencies:
        for unix_timestamp in unix_timestamp_list:
            url = f"https://coinranking1.p.rapidapi.com/coin/{currency}/price"

            querystring = {"referenceCurrencyUuid":"n5fpnvMGNsOS","timestamp":f"{unix_timestamp}"}

            headers = {
                "X-RapidAPI-Key": "85ecd427femsh9cd687b4015085bp1aa3d2jsn146974367318",
                "X-RapidAPI-Host": "coinranking1.p.rapidapi.com"
            }
            response = requests.get(url, headers=headers, params=querystring)
            print(response.json())


if __name__ == '__main__':
    api_request()
