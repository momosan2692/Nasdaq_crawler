
#!/usr/bin/python
# -*- coding: utf-8 -*-

import os
import json
import csv
import time
from datetime import date
import requests
import datetime
import sched
import time as tm
from time import gmtime, strftime                
                
s = sched.scheduler(tm.time, tm.sleep)
node_id = "NODE_002"

# This example requires the requests library be installed.  You can learn more
# about the Requests library here: http://docs.python-requests.org/en/latest/
from requests import get
my_ip = get('https://api.ipify.org').text
print('My public IP address is: ProcessNode {} IP {}'.format(node_id, my_ip))

class CrawlerController(object):
    '''Split targets into several Crawler, avoid request url too long'''

    def __init__(self, targets, max_stock_per_crawler=1):
        self.crawlers = []
        print ( '{0:>16s} {1:>16s} {2:>16s} {3:>10s} {4:>16s} {5:>16s}'.format("SYM",      "PRICE"    ,"UP/DOWN"  ,"PERCENTS","netChange","LASTCLOSE"))
        print ( '{0:>16s} {1:>16s} {2:>16s} {3:>10s} {4:>16s} {5:>16s}'.format("=========","=========","=========","=========","=========","========="))
        for index in range(0, len(targets), max_stock_per_crawler):
            crawler = Crawler(targets[index:index + max_stock_per_crawler])
            self.crawlers.append(crawler)

    def run(self):
        data = []
        for crawler in self.crawlers:
            data.extend(crawler.get_data())
        return data

class Crawler(object):
    '''Request to Market Information System'''
    def __init__(self, targets):
        endpoint = 'https://api.nasdaq.com/api/quote/'
        # Add 1000 seconds for prevent time inaccuracy
        # timestamp = int(time.time() * 1000 + 1000000)
        ## channels = '|'.join('tse_{}.tw'.format(target) for target in targets)
        ##  please mark tse_|otc_ markets in stocknumber.csv 
        channels = '|'.join('{}'.format(target) for target in targets)
        self.query_url = '{}{}/chart?assetclass=stocks'.format(endpoint, channels)
        
    def get_data(self):
        try:
            # Get original page to get session
            # req = requests.session()
            # req.get('http://mis.twse.com.tw/stock/index.jsp',
            #         headers={'Accept-Language': 'zh-TW'})

            
            headers = {
                        'authority': 'www.nasdaq.com',
                        'upgrade-insecure-requests': '1',
                        'user-agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/83.0.4103.116 Safari/537.36',
                        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
                        'sec-fetch-site': 'none',
                        'sec-fetch-mode': 'navigate',
                        'sec-fetch-user': '?1',
                        'sec-fetch-dest': 'document',
                        'accept-language': 'en-US,en;q=0.9',
                    }

            # page_response = requests.get(h_url, timeout=30, allow_redirects=True, headers=headers)
            
            response = requests.get(self.query_url, timeout=30, allow_redirects=True, headers=headers)
            content = json.loads(response.text)

        except Exception as err:
            print("GetData Fatal: ",err)
            data = []
            
        else:
            tdata = content['data']
        
            print ( '{0:>16s} {1:>16s} {2:>16s} {3:>10s} {4:>16s} {5:>16s}'.format(tdata["symbol"], \
                                                                                   tdata["lastSalePrice"] , \
                                                                                   tdata["deltaIndicator"], \
                                                                                   tdata["percentageChange"], \
                                                                                   
                                                                                   tdata["netChange"], \
                                                                                   tdata["previousClose"]) )
            ddata = [{
                "symbol" : tdata["symbol"], 
                "company" : tdata["company"],
                "lastSalePrice" : tdata["lastSalePrice"],   
                "previousClose" : tdata["previousClose"],
                "netChange" : tdata["netChange"],
                "percentageChange" : tdata["percentageChange"],    
                "deltaIndicator" : tdata["deltaIndicator"],             
            }]
            data = ddata

        return data

class Recorder(object):
    '''Record data to csv'''
    def __init__(self, path='data'):
        if not os.path.isdir(path):
            os.mkdir(path)                # if no data path created then create it 
        self.folder_path = '{}/{}'.format(path, date.today().strftime('%Y%m%d'))
        if not os.path.isdir(self.folder_path):
            os.mkdir(self.folder_path)

    def record_to_csv(self, data):
        UTC_Time = datetime.datetime.utcnow().strftime("%H:%M:%S.%f")[:-3]
   
        for row in data:
            try:
                file_path = '{}/{}.csv'.format(self.folder_path, row['symbol'])
                 
                with open(file_path, 'a') as output_file:
                    writer = csv.writer(output_file, delimiter=',')
                    writer.writerow ([
                        UTC_Time, 
                        row['symbol'],           # 資料時間
                        row['company'],          # 資料時間
                        row['lastSalePrice'],    # 資料時間
                        row['percentageChange'],    # 資料時間
                        row['deltaIndicator'],    # 資料時間
                        row['netChange'], # 
                        row['previousClose'], 
                        node_id, 
                        my_ip,                   # my public ip address
                    ])

            except Exception as err:
                print("Record CSV error: ", err)

def main_crawler ():   
    
    from IPython.display import display, clear_output
    
    targets = [_.strip() for _ in open('stocknumber.csv', 'r')]

    UTC_Time = datetime.datetime.utcnow().strftime("%H:%M:%S.%f")[:-3]
    time = datetime.datetime.now()  
    timestr = str(time.date())+':'+str(time.hour)+":"+str(time.minute)+":"+str(time.second)
    print("Nasdaq trading time: Pre-Market time start from 4:00ET (17PM TPI) to 7:30ET ")
    print("                     Normal trading time start from 9:30ET to 16:00ET")
    print("開始更新時間:" + timestr, " UTC Time: ", UTC_Time)

    start_time = datetime.datetime.strptime(str(time.date())+'9:00', '%Y-%m-%d%H:%M')
    end_time =  datetime.datetime.strptime(str(time.date())+'5:00', '%Y-%m-%d%H:%M')
    
    # tm.sleep (3) # 避免證交所伺服器鎖 IP，可能為都是網頁伺服器的rate limiting 在作祟。
    # 判斷爬蟲終止條件
    sleeptimer = 0.5
    
    if ((start_time < end_time) and (time >= start_time and time <= end_time)) or \
       ((start_time > end_time) and not (time <= start_time and time >= end_time)) :  # 處理跨日情況
        tm.sleep (sleeptimer)
        try: 
            clear_output(wait=True)
            controller = CrawlerController(targets)
            data = controller.run()

            recorder = Recorder()
            recorder.record_to_csv(data)
        except Exception as err:
            msg =  err
        else:
            msg = "更新時間:" + str(time.date())+':'+str(time.hour)+":"+str(time.minute)+":"+str(time.second)
        finally:
            # print("更新時間:" + str(time.date())+':'+str(time.hour)+":"+str(time.minute)+":"+str(time.second))
            print("Done.", msg)
            s.enter(5, 0, main_crawler, argument=())
            tm.sleep (sleeptimer)
            
            
    else:
        # trade time off 
        '''
        if TIMEUP == 1: 
            print ('非營業時間，不提供連續資料。')
            print ('繼續等待交易時間。。。')
            TIMEUP = 0
        '''
        s.enter(100, 0, main_crawler, argument=())        
                
def main():
    targets = [_.strip() for _ in open('stocknumber.csv', 'r')]

    controller = CrawlerController(targets)
    data = controller.run()

    recorder = Recorder()
    recorder.record_to_csv(data)

if __name__ == '__main__':
    
    
    
    s.enter(1, 0, main_crawler, argument=())
    s.run()
