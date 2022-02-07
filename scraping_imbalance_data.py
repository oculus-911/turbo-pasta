import requests
import datetime
import csv
import pandas as pd

API_KEY ="96fmvyckhforx64"
# todo this should be in a config file somewhere and get loaded at runtime
#

def get_yesterday_dt():
  """Return a clean datetime object with yesterdays day"""

  settlementDate_dt =datetime.datetime.utcnow() - datetime.timedelta(1)
  settlementDate_dt = settlementDate_dt.replace(hour=0)
  settlementDate_dt = settlementDate_dt.replace(minute=0)
  settlementDate_dt = settlementDate_dt.replace(second=0)
  settlementDate_dt = settlementDate_dt.replace(microsecond=0)
  return settlementDate_dt

def fetch_brcm_service_data_and_save( serviceNameList ,settlementDate,targetDirectory = "data/"):
  """Takes a list of  services and a settlement date, fetches and stores them as csv files"""


  # todo -1) make this target directory param,  if  the data already exists then dont bother fetching again
  for serviceName in serviceNameList:
    serviceDataBrms = BrmsApiWrapper(serviceName,settlementDate)
    serviceDataList = serviceDataBrms.fetch()  # iterate over by period and return a list of lists with all the days data
    smartestEnergyDataWrapperObject =SmartestEnergyDataWrapper()  # use this for reading and writing data
    targetFilename = serviceName +"_" +settlementDate +".csv"
    smartestEnergyDataWrapperObject.write_list_to_csv_file(serviceDataList,targetDirectory, targetFilename)

  return

def csv_data_to_merged_df(serviceNameList,settlementDate,targetDirectory = "data/"):
  """This Fetches the csv data for given services and dates and returns a merged df with the prices and volumes"""
  for serviceName in serviceNameList:
    targetFilename = targetDirectory + serviceName + "_" + settlementDate + ".csv"
    if serviceName == "ImbalancePrices":
      imbalancePricesDf = None
      imbalancePricesDf = pd.read_csv(targetFilename, usecols=["SettlementDate", "SettlementPeriod", "ImbalancePriceAmount", "PriceCategory"])
      imbalancePricesDf = imbalancePricesDf[imbalancePricesDf.PriceCategory == "Excess balance"] # we need to condense this down as they duplciate the price data
      imbalancePricesDf.set_index("SettlementPeriod")

    if serviceName == "AggregatedImbalanceVolumes":
      aggregatedImbalanceVolumesDf = None
      aggregatedImbalanceVolumesDf = pd.read_csv(targetFilename,usecols=["SettlementDate", "SettlementPeriod", "ImbalanceQuantityMAW"])
      aggregatedImbalanceVolumesDf.set_index('SettlementPeriod')

  if imbalancePricesDf is not None and aggregatedImbalanceVolumesDf is not None:
    mergedPricesVolumesDf = pd.merge(imbalancePricesDf, aggregatedImbalanceVolumesDf, left_on="SettlementPeriod",right_on="SettlementPeriod")
    return mergedPricesVolumesDf
  else:
    raise ValueError("Missing csv data to construct dataframe")





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
        if period_int <49: # most days we only have 48 periods
          print(f"Missing data for period {period_int}")
        pass
    return allPeriodsList


def main():

  settlementDate_dt = get_yesterday_dt()
  settlementDate  = datetime.datetime.strftime(settlementDate_dt, '%Y-%m-%d')
  serviceNameList = ["ImbalancePrices" ,"AggregatedImbalanceVolumes"] # these are the services we want
  fetch_brcm_service_data_and_save(serviceNameList,settlementDate)
  mergedPricesVolumesDf =csv_data_to_merged_df(serviceNameList,settlementDate)

  # now do the various analytics required
  powerDfAnalytics = PowerDfAnalytics(mergedPricesVolumesDf)
  maxHourlyPeriod = powerDfAnalytics.calculate_daily_abs_max_imbalance_volume_hour()
  dailyImbalanceCost = powerDfAnalytics.calculate_sum_daily_imbalance_cost()
  dailyImbalanceUnitRate = powerDfAnalytics.calculate_daily_imbalance_unit_rate()

  settlementDateMaxHour_dt =  settlementDate_dt.replace(hour=maxHourlyPeriod) # for some nicer formatting on output
  print(f"REPORTING: Total Daily Imbalance Cost is £{round(dailyImbalanceCost,2)} ")
  print(f"REPORTING: Total Daily Imbalance Unit Rate   £{round(dailyImbalanceUnitRate,2)}/MWH")
  print(f"REPORTING: Highest Absolute Volume imbalances at the hour of {settlementDateMaxHour_dt}")

if __name__ == "__main__":
  # entrypoint
  main()