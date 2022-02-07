import requests
import datetime
import csv
import pandas as pd

API_KEY ="96fmvyckhforx64"
# todo this should be in a config file somewhere and get loaded at runtime
#

class PowerDfAnalytics:
  """Instantiate an analytics object that accepts a dataframe and contains analytics methods """

  def __init__(self,
  powerDf):
    self.df =powerDf

  def calculate_daily_abs_max_imbalance_volume_hour(self):
    """Returns the HOUR owith the abs maximum imbalance volume as string

    Each trading period is 30 mins, and the trading day can have 23-25 hours
    calculate the absolute imbalance for each hourly session and return the max
    """

    self.df['ImbalanceQuantityMAW_abs'] = self.df['ImbalanceQuantityMAW'].abs()
    imbalanceQuantityMAW_list  = self.df['ImbalanceQuantityMAW_abs'].tolist()
    hourlyPeriodAbsMaxImbalance = 0
    maxHourlyPeriod = 0
    for i in range(0,len(self.df), 2): # increment the hour not the period
      hourlyImbalance =imbalanceQuantityMAW_list[i] + imbalanceQuantityMAW_list[i+1]
      if  hourlyImbalance > hourlyPeriodAbsMaxImbalance:
        hourlyPeriodAbsMaxImbalance =hourlyImbalance
        maxHourlyPeriod = int(i/2)

    return maxHourlyPeriod


  def calculate_sum_daily_imbalance_cost(self):
    """Returns the sum daily imbalance cost by 1) calculating the period cost as Price * Volume and 2) summing, assummption that we are looking for the absolute cost """

    self.df['PeriodImbalanceCost'] = self.df['ImbalanceQuantityMAW'].abs() * self.df['ImbalancePriceAmount']
    self.sumDailyImbalanceCost =self.df['PeriodImbalanceCost'].sum()
    return self.sumDailyImbalanceCost

  def calculate_daily_imbalance_unit_rate(self):
    """Returns the daily imbalance rate as  Sum (Abs cost)/Sum (Abs Volume)  """

    self.df['ImbalanceQuantityMAW_abs'] = self.df['ImbalanceQuantityMAW'].abs()
    self.df['PeriodImbalanceCost'] = self.df['ImbalanceQuantityMAW_abs'] * self.df['ImbalancePriceAmount']
    self.dailyImbalanceUnitRate =  self.df['PeriodImbalanceCost'].sum() / self.df['ImbalanceQuantityMAW_abs'].sum()
    return self.dailyImbalanceUnitRate

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
# clean this date format
settlementDate_dt = settlementDate_dt.replace(hour=0)
settlementDate_dt = settlementDate_dt.replace(minute=0)
settlementDate_dt = settlementDate_dt.replace(second=0)
settlementDate_dt = settlementDate_dt.replace(microsecond=0)
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
serviceName = "ImbalancePrices"  # or "AggregatedImbalanceVolumes"
imbalancePricesFileName = targetDirectory + "/" + serviceName + "_" + settlementDate + ".csv"

imbalancePricesDf = pd.read_csv(imbalancePricesFileName, usecols=["SettlementDate", "SettlementPeriod", "ImbalancePriceAmount","PriceCategory"])
# we need to condense this down as they duplciate the price data
condensedImbalancePricesDf = imbalancePricesDf[imbalancePricesDf.PriceCategory== "Excess balance"]
condensedImbalancePricesDf.set_index("SettlementPeriod")



serviceName = "AggregatedImbalanceVolumes"
aggregatedImbalanceVolumesFileName = targetDirectory + "/" + serviceName + "_" + settlementDate + ".csv"
aggregatedImbalanceVolumesDf = pd.read_csv(aggregatedImbalanceVolumesFileName, usecols=["SettlementDate", "SettlementPeriod", "ImbalanceQuantityMAW"])
aggregatedImbalanceVolumesDf.set_index('SettlementPeriod')
# mergedPricesVolumesDf =  imbalancePricesDf.merge(aggregatedImbalanceVolumesDf, left_index=True,right_index=True)
# mergedPricesVolumesDf =  condensedImbalancePricesDf.merge(aggregatedImbalanceVolumesDf, left_index=True,right_index=True)
mergedPricesVolumesDf =  pd.merge(condensedImbalancePricesDf, aggregatedImbalanceVolumesDf , left_on="SettlementPeriod", right_on="SettlementPeriod")
print("finished merging dataframe")

# ok now do the various analytics required
powerDfAnalytics = PowerDfAnalytics(mergedPricesVolumesDf)
maxHourlyPeriod = powerDfAnalytics.calculate_daily_abs_max_imbalance_volume_hour()
dailyImbalanceCost = powerDfAnalytics.calculate_sum_daily_imbalance_cost()
dailyImbalanceUnitRate = powerDfAnalytics.calculate_daily_imbalance_unit_rate()


settlementDateMaxHour_dt =  settlementDate_dt.replace(hour=maxHourlyPeriod)

print(f"REPORTING: Total Daily Imbalance Cost is £{round(dailyImbalanceCost,2)} ")
print(f"REPORTING: Total Daily Imbalance Unit Rate   £{round(dailyImbalanceUnitRate,2)}/MWH")
print(f"REPORTING: Highest Absolute Volume imbalances were the hour of {settlementDateMaxHour_dt}")
print("Finished")
#todo refactor