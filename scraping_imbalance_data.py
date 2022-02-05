import requests
import datetime

API_KEY ="96fmvyckhforx64"
# todo this should be in a config file somewhere and get loaded at runtime
#
# r = requests.get('https://api.binance.com/api/v3/aggTrades',
#   params={
#     "symbol": symbol,
#     "startTime": get_unix_ms_from_date(from_date),
#     "endTime": get_unix_ms_from_date(new_end_date)
#  })

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
      self.targetUrl = f"https://api.bmreports.com/BMRS/B1770/<VersionNo>?APIKey={self.apiKey}&SettlementDate={self.settlementDate}&Period={self.period}&ServiceType={self.dataExportType }"
    elif self.serviceName ==  "AggregatedImbalanceVolumes":
      self.targetUrl = f"https://api.bmreports.com/BMRS/B1770/<VersionNo>?APIKey={self.apiKey}&SettlementDate={self.settlementDate}&Period={self.period}&ServiceType={self.dataExportType }"
    else:
      assert  self.targetUrl , "Unknown service Name"

  def fetch(self):
    """ fetch the api data and return as csv"""


    targetCsv = requests.get(self.targetUrl)
    print(f"targetCsv  -- {targetCsv}")

# baseUrl = f"https://api.bmreports.com/BMRS/B1770/<VersionNo>?APIKey={self.apiKey}&SettlementDate={self.settlementDate}&Period={self.period}&ServiceType={self.serviceType}")
serviceName = "ImbalancePrices"  # or "AggregatedImbalanceVolumes"
apiKey = API_KEY
settlementDate = "2022-02-04"
period = 1

testBrms = BrmsApiWrapper(serviceName,period,settlementDate)

testBrms.fetch()



# test_url =  f"https://api.bmreports.com/BMRS/B1770/<VersionNo>?APIKey={apiKey}&SettlementDate={settlementDate}&Period={period}&ServiceType={serviceType}"

# print(test_url)



# https://api.bmreports.com/BMRS/B1770/<VersionNo>?APIKey=< APIKey>&SettlementDate=<SettlementDate>&Period=<Period>&ServiceType=<xml/csv>


# https://api.bmreports.com/BMRS/B1780/<VersionNo>?APIKey=<APIKey>&SettlementDate=<SettlementDate>&Period=<Period>&ServiceType=<xml/csv>