import requests
from datetime import datetime, date, timedelta
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# BTC - Qwsogvtv82FCd
# ETH - razxDUgYGNAdQ
# ADA - qzawljRxB5bYu
# BRL - n5fpnvMGNsOS

def get_date_list():
    date_list = []
    for i in range(0,10):
        data =  (datetime.now() - timedelta(days=i)).date()
        date_list.append(data)
    return date_list

def get_unixtimestamp_dict():
    unix_timestamp_dict = {}
    date_list = get_date_list()
    for date in date_list:
        date_hour = str(date) + ' 21:00:00'
        formated_date = datetime.strptime(date_hour,"%Y-%m-%d %H:%M:%S")
        unix_timestamp = datetime.timestamp(formated_date)
        unix_timestamp_dict[str(date)] = unix_timestamp
    return unix_timestamp_dict


def api_request():
    unix_timestamp_dict = get_unixtimestamp_dict()
    currencies = {'btc':'Qwsogvtv82FCd', 'eth': 'razxDUgYGNAdQ', 'ada':'qzawljRxB5bYu'}
    for currency in list(currencies.keys()):
        for date in list(unix_timestamp_dict.keys()):
            url = f"https://coinranking1.p.rapidapi.com/coin/{currencies[currency]}/price"

            querystring = {"referenceCurrencyUuid":"n5fpnvMGNsOS","timestamp":f"{unix_timestamp_dict[date]}"}

            headers = {
                "X-RapidAPI-Key": "85ecd427femsh9cd687b4015085bp1aa3d2jsn146974367318",
                "X-RapidAPI-Host": "coinranking1.p.rapidapi.com"
            }
            response = requests.get(url, headers=headers, params=querystring)
            json_file = json.dumps(response.json())
            path = generate_path(currency, date)

            save_json_azure(path, json_file)


def generate_path(currency, date):
    path = f'daily/raw/{currency}/{date[0:4]}/{date[5:7]}/{date[8:10]}/moeda.json'
    return path

def save_json_azure(path, json_file):
    connect_str = "***"
    container_name = "grupo1"
    blob_name = path

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json_file, overwrite=True)


if __name__ == '__main__':
    api_request()
    
