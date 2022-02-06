import requests
import datetime
import json
import csv

API_KEY ="96fmvyckhforx64"
# todo this should be in a config file somewhere and get loaded at runtime
#


class SmartestEnergyDataWrapper:
  def __init__(self):
    pass

  def write_list_to_csv_file(self, listOfLists, targetDirectory, targetFilename):
    """Takes a list of lists and outputs to a  to a target csv file"""

    # print(targetDirectory)
    # print(targetFilename)
    targetFilename = targetDirectory + targetFilename
    # print(targetFilename)
    with open(targetFilename, "w",newline="") as f:
          writer = csv.writer(f)
          writer.writerows(listOfLists)


  def read_csv_file(self, targetDirectory):
    pass

class BrmsApiWrapper:
  """ Instantiate a target BRMS with some sensible defaults"""
  def __init__(self,
  serviceName,
  period = 1,
  settlementDate = str(datetime.datetime.utcnow()),
  dataExportType = "csv"
  ):
    self.serviceName = serviceName
    self.apiKey = API_KEY  # todo this should be loaded from a config somewhere
    # self.targetUrl = baseUrl
    self.period = period
    self.settlementDate = settlementDate
    self.dataExportType = dataExportType  # this is what the exchange calls service type
    if self.serviceName ==  "ImbalancePrices":
      self.targetUrl = f"https://api.bmreports.com/BMRS/B1770/V1?APIKey={self.apiKey}&SettlementDate={self.settlementDate}&Period={self.period}&ServiceType={self.dataExportType }"
    elif self.serviceName ==  "AggregatedImbalanceVolumes":
      self.targetUrl = f"https://api.bmreports.com/BMRS/B1770/V1?APIKey={self.apiKey}&SettlementDate={self.settlementDate}&Period={self.period}&ServiceType={self.dataExportType }"
    else:
      assert  self.targetUrl , "Unknown service Name"

  def fetch(self):
    """ fetch the api data and return as csv

    This endpoint limits to one period per call, there 46-50 periods p/day so loop through each then combine the output into json
    """
    allPeriodsList =[]
    periodsInDay = 50  # this is really the maximum for when clocks change so most of the time you get 48
    for period_int in range(1,periodsInDay):
      # todo fix this so its not defined at  init as well
      self.targetUrl = f"https://api.bmreports.com/BMRS/B1770/V1?APIKey={self.apiKey}&SettlementDate={self.settlementDate}&Period={period_int}&ServiceType={self.dataExportType}"
      response = requests.get(self.targetUrl)
      if not response.ok:
        raise  Exception(f"error in fetching data from BRMS, error msg: {response}")

      try:
        data =response.text
        parseDataList = data.split("\n")
        parseData = data.split("\n")[4]
        commmaSplitParseData = parseData.split(",")
        for idx, elem in enumerate(parseDataList):
          if elem == "<EOF>":
            lastRow = idx

        if period_int ==1:
          headersRow =parseData.split(",")
          allPeriodsList.append(headersRow)
        dataRows = parseDataList[5:lastRow]

        for row in dataRows:
          row_list =row.split(",")
          allPeriodsList.append(row_list)
      except:
        print(f"Failure for period {period_int}")

    return allPeriodsList




# baseUrl = f"https://api.bmreports.com/BMRS/B1770/<VersionNo>?APIKey={self.apiKey}&SettlementDate={self.settlementDate}&Period={self.period}&ServiceType={self.serviceType}")
serviceName = "ImbalancePrices"  # or "AggregatedImbalanceVolumes"
apiKey = API_KEY
settlementDate = "2022-02-04"  # todo make this yesterdays price
period = 10

testBrms = BrmsApiWrapper(serviceName,period,settlementDate)
rawOutputList = testBrms.fetch()  # iterate over by period and return a list of lists with all the days data
smartestEnergyDataWrapperObject =SmartestEnergyDataWrapper()  # use this for reading and writing data

targetDirectory = "data/"
targetFilename = "testoutput.csv"

smartestEnergyDataWrapperObject.write_list_to_csv_file(rawOutputList,targetDirectory, targetFilename)







# test_url =  f"https://api.bmreports.com/BMRS/B1770/<VersionNo>?APIKey={apiKey}&SettlementDate={settlementDate}&Period={period}&ServiceType={serviceType}"

# print(test_url)



# https://api.bmreports.com/BMRS/B1770/<VersionNo>?APIKey=< APIKey>&SettlementDate=<SettlementDate>&Period=<Period>&ServiceType=<xml/csv>


# https://api.bmreports.com/BMRS/B1780/<VersionNo>?APIKey=<APIKey>&SettlementDate=<SettlementDate>&Period=<Period>&ServiceType=<xml/csv>