import requests
from datetime import datetime, date
import json

# BITCOIN - Qwsogvtv82FCd
# ETH - razxDUgYGNAdQ
# ADA - qzawljRxB5bYu
# BRL - n5fpnvMGNsOS

def get_today_unixtimestamp():
    today = str(datetime.now().date())
    today = today + ' 21:00:00'
    formated_date = datetime.strptime(today,"%Y-%m-%d %H:%M:%S")
    unix_timestamp = datetime.timestamp(formated_date)
    return unix_timestamp

def api_request():
    unix_timestamp = get_today_unixtimestamp()
    currencies = ['Qwsogvtv82FCd', 'razxDUgYGNAdQ', 'qzawljRxB5bYu']
    for currency in currencies:
        url = f"https://coinranking1.p.rapidapi.com/coin/{currency}/price"

        querystring = {"referenceCurrencyUuid":"n5fpnvMGNsOS","timestamp":f"{unix_timestamp}"}

        headers = {
            "X-RapidAPI-Key": "85ecd427femsh9cd687b4015085bp1aa3d2jsn146974367318",
            "X-RapidAPI-Host": "coinranking1.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring)
        print(response.json())

api_request()