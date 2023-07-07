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
        ###########
        proxyip = "http://storm-stst123_area-FR:123123@proxy.stormip.cn:1000"
        url = "http://myip.ipip.net"
        proxies = {
            'http': proxyip,
            'https': proxyip,
        }
        print(proxies)
        with open(proxy_text, 'a') as file:
            file.write(proxyip)
            file.write('\n')

        print("Data saved to", proxy_text)
        # response = requests.get(url=url, proxies=proxies)
        ###########
        # proxy_text = 'proxy_text.txt'

        # if response.status_code == 200:
        #     ip_address = re.findall(r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', response.text)
        #     proxy_list = [f'http://{ip}:1000' for ip in ip_address]
        #     print(proxy_list)
        #     with open(proxy_text, 'a') as file:
        #         file.write('\n'.join(proxy_list))
        #         file.write('\n')

        #     print("Data saved to", proxy_text)
        # else:
        #     print("Failed to fetch data from the website.")


# Change the number to decide the number of proxies generated
proxy_generation(2)


# Change the number to decide the number of proxies generated
proxy_generation(2)
