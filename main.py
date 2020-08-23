import requests
from activecampaign.client import Client
import logging
from lib import lib_bigquery
import asyncio
import os
import json
import datetime

#Set path variable
path = os.path.dirname(os.path.realpath(__name__))

#Logging
logging.basicConfig(format='%(asctime)s %(levelname)-8s %(message)s', filename=path + '/main.log',level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)


#activeCampaign
activeCampaign_settings = json.load(open(path + "/auth/activeCampaign.json", 'r'))

class activeCampaign(Client):
    def __init__ (self, URL:str, API_KEY:str):
        self.db = lib_bigquery.bigqueryWrapper()
        self.timestamp = datetime.datetime.now() - datetime.timedelta(days=30 * 6)
        #'created_after': self.timestamp.strftime("%Y-%m-%dT%H:%M:%S")
        super(activeCampaign, self).__init__(URL, API_KEY)


    async def runReport(self, reportName, outputFile):

        
        def runAPI():
            print(reportName)
            x = 0
            finalData = None
            while True:
                if reportName == "deals":
                    data = self.deals.list_all_deals(**{'offset': str(x), 'limit': '100', 'created_after': self.timestamp})
                elif reportName == "contacts":
                    data = self.contacts.list_all_contacts(**{'offset': str(x), 'limit': '100', 'created_after': self.timestamp})
                elif reportName == 'automations':
                    data = self.automations.list_all_automations(**{'offset': str(x), 'limit': '100', 'created_after': self.timestamp})
                elif reportName == 'contactAutomations':
                    data = self.contacts.list_all_automations_a_contact_is_in()
                    return data
                elif reportName == 'dealStages':
                    data = self.deals.list_all_stages(**{'offset': str(x), 'limit': '100', 'created_after': self.timestamp})
                else:
                    raise ValueError("Not recognized endpoint report type")

                if len(data[reportName]) > 0:
                    
                    if finalData == None:
                        finalData = dict(data)
                        x += 100
                    else:
                        finalData[reportName].extend(data[reportName])
                        x += 100
                else:
                    print("Returning")
                    return finalData
        
        data = runAPI()

        await asyncio.sleep(1)

        
        def check(data):
            if data != None:
                if type(data) == dict:
                    if len(data[reportName]) > 0:
                        return True
            return False

        def deleteOldfile():
            try:
                os.remove(outputFile)
            except:
                logger.info("Old file not found not removed")

        def parseDic(rowDic):
            timestampFields = [k for k, v in lib_bigquery.settingsJson['schema'][reportName].items() if v[0] == "TIMESTAMP"]
            for k, v in rowDic.items():
                if type(v) in [list, dict]:
                    rowDic[k] = json.dumps(v)
                
                elif k in timestampFields:
                    if rowDic[k] != None:
                        if "0000-00-00 00:00:00" == str(rowDic[k])[:19]:
                            rowDic[k] = None
                        else:
                            rowDic[k] = str(rowDic[k])[:19]
            return rowDic


            if type(value) in [list, dict]:
                return json.dumps(value)
            else:
                return value


        # Write to a file
        if check(data) == True:

            deleteOldfile()
            json.dump(data, open("debug" + reportName + ".json", 'w'))
            with open(outputFile, 'a') as writeFile:
                for dic in data[reportName]:
                    writeFile.write(json.dumps(parseDic(dic)) + "\n")
            db = lib_bigquery.bigqueryWrapper()
            db.settings['table'] = reportName
            db.deleteLoad("id", [x['id'] for x in data[reportName]])
            #db.dropTable()
            #db.AddTable()
            db.load_json_from_file(outputFile)
                
        else:
            logging.info("Report {} did not return any data".format(reportName))


    async def main(self, outputPath, *args):
        await asyncio.gather(*[self.runReport(reportName, outputPath + reportName + ".json") for reportName in args[0]])



def active_campaign(*args):
    outputPath = "/tmp/"
    client = activeCampaign(activeCampaign_settings['URL'], activeCampaign_settings['API_KEY'])
    asyncio.run(client.main(outputPath, [rep for rep in lib_bigquery.settingsJson['schema'].keys()]))

#if __name__ == '__main__':
#   active_campaign()
