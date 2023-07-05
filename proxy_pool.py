"""
# Project: proxies pool
# Author: Eddie
# Date:  22/06/2023
"""
import requests
import re

# Clear the proxy_text
proxy_text = 'proxy_text.txt'

with open(proxy_text, 'w') as file:
    file.write('')


def proxy_generation(number):
    for i in range(number):
        proxy_url = 'https://myip.ipip.net'
        proxy = 'http://storm-stst123_area-GB:123123@eu.stormip.cn:1000'

        response = requests.get(proxy_url, proxies={'http': proxy, 'https': proxy})
        proxy_text = 'proxy_text.txt'

        if response.status_code == 200:
            ip_address = re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', response.text)
            proxy_list = [f'http://{ip}:6023' for ip in ip_address]
            print(proxy_list)
            with open(proxy_text, 'a') as file:
                file.write('\n'.join(proxy_list))
                file.write('\n')

            print("Data saved to", proxy_text)
        else:
            print("Failed to fetch data from the website.")


# Change the number to decide the number of proxies generated
proxy_generation(2)
