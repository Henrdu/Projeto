import requests
from datetime import datetime, date, timedelta
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# BTC - Qwsogvtv82FCd
# ETH - razxDUgYGNAdQ
# ADA - qzawljRxB5bYu
# BRL - n5fpnvMGNsOS

def api_request():
    current_date = datetime.now().strftime("%Y%m%d_%H%M")
    currencies = {'btc':'Qwsogvtv82FCd', 'eth': 'razxDUgYGNAdQ', 'ada':'qzawljRxB5bYu'}
    for currency in list(currencies.keys()):
        url = f"https://coinranking1.p.rapidapi.com/coin/{currencies[currency]}/price"

        querystring = {"referenceCurrencyUuid":"n5fpnvMGNsOS"}

        headers = {
            "X-RapidAPI-Key": "85ecd427femsh9cd687b4015085bp1aa3d2jsn146974367318",
            "X-RapidAPI-Host": "coinranking1.p.rapidapi.com"
        }
        response = requests.get(url, headers=headers, params=querystring)
        json_file = json.dumps(response.json())
        path = generate_path(currency, current_date)

        save_json_azure(path, json_file)


def generate_path(currency, current_date):
    path = f'streaming/raw/{currency}/{current_date}.json'
    return path

def save_json_azure(path, json_file):
    connect_str = "xxxxx"
    container_name = "grupo1"
    blob_name = path

    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    container_client = blob_service_client.get_container_client(container_name)

    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json_file, overwrite=True)


if __name__ == '__main__':
    api_request()
    
