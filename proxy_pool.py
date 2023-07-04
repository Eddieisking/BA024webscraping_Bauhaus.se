"""
# Project: proxies pool
# Author: Eddie
# Date:  22/06/2023
"""
import requests

proxy_url = 'https://api.stormproxies.cn/web_v1/ip/get-ip-v3?app_key=64318690cd8b0c33d643b078d3974ebf&pt=9&num=20&ep=&cc=&state=&city=&life=5&protocol=1&format=txt&lb=%5Cr%5Cn'
proxy_text = 'proxy_text.txt'

response = requests.get(proxy_url)

if response.status_code == 200:
    data = response.text.strip().split('\n')
    proxy_list = [f'http://{ip}' for ip in data]

    with open(proxy_text, 'w') as file:
        file.write('\n'.join(proxy_list))

    print("Data saved to", proxy_text)
else:
    print("Failed to fetch data from the website.")

