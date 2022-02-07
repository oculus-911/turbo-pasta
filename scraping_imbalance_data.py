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
  settlementDate,
  dataExportType = "csv"
  ):
    self.serviceName = serviceName
    self.apiKey = API_KEY  # todo this should be loaded from a config somewhere
    # self.baseUrl = baseUrl
    # self.period = period
    self.settlementDate = settlementDate    # settlementDate = "2022-02-04"  # We must match this format for the api
    self.dataExportType = dataExportType  # this is what the exchange calls service type
    self.serviceNameList = ["ImbalancePrices","AggregatedImbalanceVolumes"]
    if self.serviceName ==  "ImbalancePrices":
      self.serviceNameCode = "B1770"
    elif self.serviceName ==  "AggregatedImbalanceVolumes":
      self.serviceNameCode = "B1780"
    else:
      assert  self.serviceName in self.serviceNameList  , "Unknown service Name"

  def fetch(self):
    """ fetch the api data and return as csv

    This endpoint limits to one period per call, there 46-50 periods p/day so loop through each then combine the output into json
    """
    allPeriodsList =[]
    periodsInDay = 50  # this is really the maximum for when clocks change so most of the time you get 48
    for period_int in range(1,periodsInDay):
      # todo fix this so its not defined at  init as well
      targetUrl  = f"https://api.bmreports.com/BMRS/{self.serviceNameCode}/V1?APIKey={self.apiKey}&SettlementDate={self.settlementDate}&Period={period_int}&ServiceType={self.dataExportType}"
      response = requests.get(targetUrl)
      if not response.ok:
        raise  Exception(f"error in fetching data from BRMS, error msg: {response}")

      try:
        data =response.text
        parseDataList = data.split("\n")
        parseData = data.split("\n")[4]
        for idx, elem in enumerate(parseDataList):
          if elem == "<EOF>":
            lastRow = idx

        if period_int ==1:
          headersRow =parseData.split(",")
          # strip non alphanumeric stuff right away
          cleanedHeadersRow = []
          for name in headersRow:
            cleanedHeadersRow.append("".join(chr for chr in name if chr.isalnum()))

          allPeriodsList.append(cleanedHeadersRow)
        dataRows = parseDataList[5:lastRow]

        for row in dataRows:
          row_list =row.split(",")
          allPeriodsList.append(row_list)
      except:
        print(f"Failure for period {period_int}")

    return allPeriodsList




# baseUrl = f"https://api.bmreports.com/BMRS/B1770/<VersionNo>?APIKey={self.apiKey}&SettlementDate={self.settlementDate}&Period={self.period}&ServiceType={self.serviceType}")

apiKey = API_KEY

settlementDate_dt = datetime.datetime.utcnow() - datetime.timedelta(1)
settlementDate  = datetime.datetime.strftime(settlementDate_dt, '%Y-%m-%d')
targetDirectory = "data/"

# get imbalance data first
serviceName = "ImbalancePrices"  # or "AggregatedImbalanceVolumes"
print(f"Fetching {serviceName} data for {settlementDate}")
serviceDataBrms = BrmsApiWrapper(serviceName,settlementDate)
serviceDataList = serviceDataBrms.fetch()  # iterate over by period and return a list of lists with all the days data
smartestEnergyDataWrapperObject =SmartestEnergyDataWrapper()  # use this for reading and writing data

targetFilename = serviceName +"_" +settlementDate +".csv"
smartestEnergyDataWrapperObject.write_list_to_csv_file(serviceDataList,targetDirectory, targetFilename)

# get aggregatedImbalanceVolume

serviceName = "AggregatedImbalanceVolumes"
print(f"Fetching {serviceName} data")
serviceDataBrms = BrmsApiWrapper(serviceName,settlementDate)
serviceDataList = serviceDataBrms.fetch()  # iterate over by period and return a list of lists with all the days data
smartestEnergyDataWrapperObject =SmartestEnergyDataWrapper()  # use this for reading and writing data

targetFilename = serviceName +"_" +settlementDate +".csv"
smartestEnergyDataWrapperObject.write_list_to_csv_file(serviceDataList,targetDirectory, targetFilename)
print("Finished Fetching Raw Data")

# read data from csv into a dataframe
# todo - if  the data already exists then dont bother fetching again
import pandas as pd

serviceName = "ImbalancePrices"  # or "AggregatedImbalanceVolumes"
imbalancePricesFileName = targetDirectory + "/" + serviceName + "_" + settlementDate + ".csv"
imbalancePricesDf = pd.read_csv(imbalancePricesFileName)
print(imbalancePricesDf.columns)
imbalancePricesDf = pd.read_csv(imbalancePricesFileName, usecols=["SettlementDate", "SettlementPeriod", "ImbalancePriceAmount"])
imbalancePricesDf.set_index("SettlementPeriod")

serviceName = "AggregatedImbalanceVolumes"
aggregatedImbalanceVolumesFileName = targetDirectory + "/" + serviceName + "_" + settlementDate + ".csv"


aggregatedImbalanceVolumesDf = pd.read_csv(aggregatedImbalanceVolumesFileName, usecols=["SettlementDate", "SettlementPeriod", "ImbalanceQuantityMAW"])
aggregatedImbalanceVolumesDf.set_index('SettlementPeriod')
mergedPricesVolumesDf =  imbalancePricesDf.merge(aggregatedImbalanceVolumesDf, left_index=True,right_index=True)
print("finish")

# ok now do the various analytics required