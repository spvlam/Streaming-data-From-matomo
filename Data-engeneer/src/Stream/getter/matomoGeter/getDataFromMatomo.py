# it still be the content of matomo_Stream.batch file
import requests
import json
class MatomoLog:
    def __init__(self,api_url,auth_token):
        self.api=api_url
        self.auth_token=auth_token
        self.defaultLimit=1  
    def initiliseLog(self,siteId,period,startDay):   
        self.params = {
            'module': 'API',
            'method': 'Live.getLastVisitsDetails',
            'idSite': siteId,
            'period': period,
            'date': startDay,
            'doNotFetchActions': 1,
            'format': 'json',
            'filter_limit':self.defaultLimit,
            'filter_offset':0,
            # get some specifix feature from matomo
            # 'showColumns': """location,userId,idSite,idVisit,deviceBrand,
            # lastActionDateTime,siteName,secondsSinceFirstVisit,
            # operatingSystemName,daysSinceLastVisit,dimension1,visitorType,LastActionTimestamp,FisrtActionTimestamp,
            # language,deviceModel,operatingSystem""",
            'token_auth': self.auth_token
        }

    def getLogFromSiteId(self):
        # define params like what kind of inf,format,size
        # period : day , siteId:1, startDay:yesterday
        response = requests.get(self.api, params=self.params, headers={'Authorization': 'Bearer ' + self.auth_token})
        try:
            if response.status_code == 200:
                self.data = response.json()
                self.params['filter_offset']=self.params['filter_offset']+self.defaultLimit
            else:
                print('Error occurred during API request:', response.text)
        except Exception as err:
            print(err)

"""
testing data have been getten from matomo
"""
if __name__ == '__main__':
    api_url1 = 'https://matomo.eupgroup.net/index.php'
    auth_token2='108d97003517053884ef87f5dda5819e'
    abc = MatomoLog(api_url1,auth_token2)
    abc.initiliseLog(1,'day','yesterday')
    abc.getLogFromSiteId()
    with open("abc.json","w",encoding="utf-8") as fi:
        json.dump(abc.data,fi,ensure_ascii=False,indent=4)