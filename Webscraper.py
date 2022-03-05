import urllib.request, urllib.parse, urllib.error, ssl
import sqlite3, re, time, json
from bs4 import BeautifulSoup
from datetime import datetime

conn = sqlite3.connect('consolidation.sqlite')
cur = conn.cursor()
cur.executescript('''
CREATE TABLE IF NOT EXISTS DetailsDB (
    id      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    projID              INTEGER,
    name                TEXT,
    backerCount         INTEGER,
    amount              INTEGER,
    currency            TEXT,
    target              INTEGER,
    initCity            CITY,
    category            TEXT,
    fundingPeriodDays   INTEGER,
    pledgeJSON          TEXT,
    countryBackerJSON                TEXT,
    existingBackerCount        INTEGER,
    newBackerCount             INTEGER,
    URL                 TEXT);
''')

cur.executescript('''
CREATE TABLE IF NOT EXISTS errorDB (
    id      INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    projID              INTEGER,
    errorURL                 TEXT);
''')

# a utility function to remove multiple strings
def removeAll(regexExpressionArray, text):
    for regexExpression in regexExpressionArray:
        text = re.sub(regexExpression,"",str(text))
    return text

# Obtain (1) Name
def getProjName(main_soup):
    rawText_name = main_soup.find("a",class_="hero__link")
    return removeAll(["<.*?>"],str(rawText_name))

# Obtain (2) amount, (3) currency backed and (4) total number of backer
def getCampaignStats(main_soup):
    rawText_totalBacked = main_soup.find("div",class_="NS_campaigns__spotlight_stats")
    rawText_totalBacked = removeAll(["<.*?>"," to help bring this project to life.",",","\n"],str(rawText_totalBacked))
    rawText_totalBacked = rawText_totalBacked.split(" backers pledged ")
    proj_backer_count = int(rawText_totalBacked[0])
    rawText_totalBacked = rawText_totalBacked[1]
    rawText_totalBacked = re.split('(\d+)',rawText_totalBacked)
    data_object = {
        "backerCount": proj_backer_count,
        "currency" : str(rawText_totalBacked[0]),
        "amount" : int(rawText_totalBacked[1]),
    }
    return data_object

# Obtain the (5) amount targeted
def getTargetAmount(main_soup):
    rawText_targetAmount = main_soup.find("div",class_="type-12 medium navy-500")
    rawText_targetAmount = removeAll(["<.*?>","\npledged of ","goal\n",","],str(rawText_targetAmount))
    rawText_targetAmount = re.split('(\d+)',rawText_targetAmount)
    return int(rawText_targetAmount[1])

# Obtain the (6) intiating city and (7) product category
def getCityCategory(main_soup):
    rawText_cityCategory = main_soup.find_all("a",class_="grey-dark mr3 nowrap type-12")
    data_object = {
        "city" : removeAll(["<.*?>","\n"],str(rawText_cityCategory[0])),
        "category" : removeAll(["<.*?>","\n"],str(rawText_cityCategory[1]))
    }
    return data_object

# Obtain the (8) length of funding period
def getFundingPeriodLength(main_soup):
    rawText_fundingPeriod = main_soup.find("div",class_="NS_campaigns__funding_period")
    rawText_fundingPeriod = removeAll(["<.*?>","\nFunding period\n"," days\)\n\n"],str(rawText_fundingPeriod))
    rawText_fundingPeriod = re.split(' - ',rawText_fundingPeriod)
    rawText_fundingPeriod = re.split('\n\(',rawText_fundingPeriod[1])
    return rawText_fundingPeriod[1]

# Obtain (9) all pledging options
def getPledgingOptions(main_soup):
    proj_pledgeOption = []
    rawTextList_pledgeOptions = main_soup.find_all("h2", class_= "pledge__amount")
    for rawText_pledgeOption in rawTextList_pledgeOptions:
        rawText_pledgeOption = removeAll(["<.*?>","\n","Pledge "," or more","About ",","],str(rawText_pledgeOption))
        proj_pledgeOptionList = re.split('(\d+)',rawText_pledgeOption)
        proj_pledgeOption.append(int(proj_pledgeOptionList[1]))

    proj_pledgeOptionBacker = []
    rawTextList_pledgeOptionBackers = main_soup.find_all("div", class_ = "pledge__backer-stats")
    for rawText_pledgeOptionBacker in rawTextList_pledgeOptionBackers:
        rawText_pledgeOptionBacker = removeAll(["<.*?>"," backers| backer",","],str(rawText_pledgeOptionBacker))
        proj_pledgeOptionBackerList = re.split('\n',rawText_pledgeOptionBacker)
        for i in range(0, len(proj_pledgeOptionBackerList)):
            if(proj_pledgeOptionBackerList[i].isdigit()):
                proj_pledgeOptionBacker.append(int(proj_pledgeOptionBackerList[i]))

    proj_pledgeDelivery = []
    rawText_fundingPeriod = main_soup.find("div",class_="NS_campaigns__funding_period")
    rawText_fundingPeriod = removeAll(["<.*?>","\nFunding period\n","\n"],str(rawText_fundingPeriod))
    rawText_fundingPeriodRange = re.split(' - ',rawText_fundingPeriod)
    proj_startFundingPeriod = rawText_fundingPeriodRange[0]
    proj_startFundingPeriod = datetime.strptime(proj_startFundingPeriod, '%b %d, %Y')
    proj_startFundingPeriod = proj_startFundingPeriod.date()

    rawTextList_deliveryEstimation = main_soup.find_all("time",class_="js-adjust-time")
    for rawText_deliveryEstimation in rawTextList_deliveryEstimation:
        rawText_deliveryEstimation = removeAll(["<.*?>"],str(rawText_deliveryEstimation))
        try:
            rawText_deliveryEstimation = datetime.strptime(rawText_deliveryEstimation, '%b %Y')
            rawText_deliveryEstimation = rawText_deliveryEstimation.date()
            proj_deliverEstimation = rawText_deliveryEstimation - proj_startFundingPeriod
            proj_pledgeDelivery.append(proj_deliverEstimation.days)
        except:
            continue

    result = []
    for i in range(0, len(proj_pledgeOption)):
        data_object = {
                "pledge_option": proj_pledgeOption[i],
                "pledge_backer": proj_pledgeOptionBacker[i],
                "pledge_deliveryPeriod": proj_pledgeDelivery[i]
            }
        result.append(data_object)
    return result

# Obtain the list of (10) Top countries where backers come from
def getTopCountries(community_soup):
    result = []
    rawText_topBackingCountries = community_soup.find("div",class_="location-list js-locations-countries")
    rawText_topBackingCountries = removeAll(["<.*?>",","], str(rawText_topBackingCountries))
    rawText_topBackingCountriesList = re.split(' backers',rawText_topBackingCountries)
    for raw_Countries in rawText_topBackingCountriesList:
        raw_countryBackerAmount = re.split('[\n]+',raw_Countries)
        try:
            result.append((raw_countryBackerAmount[1], raw_countryBackerAmount[2]))
        except:
            pass
    return result

# Obtain the amount of (11) existing backer
def getExistingBacker(community_soup):
    rawText_existingBackers = community_soup.find("div",class_="existing-backers")
    rawText_existingBackers = removeAll(["<.*?>",","],str(rawText_existingBackers))
    rawText_existingBackers = re.split('(\d+)',rawText_existingBackers)
    try:
        proj_ExistingBacker = int(rawText_existingBackers[1])
    except:
        proj_ExistingBacker = 0
    return proj_ExistingBacker

# Obtain the amount of (12) new backers
def getNewBacker(community_soup):
    rawText_newBackers = community_soup.find("div",class_="new-backers")
    rawText_newBackers = removeAll(["<.*?>",","],str(rawText_newBackers))
    rawText_newBackers = re.split('(\d+)',rawText_newBackers)
    try:
        proj_newBacker = int(rawText_newBackers[1])
    except:
        proj_newBacker = 0
    return proj_newBacker

# Webscraping
def gatherDetails(projID, project_url):
    data_object = {}
    # try:
    main_html = urllib.request.urlopen(project_url, context=ctx).read()
    main_html = main_html.decode()
    main_soup = BeautifulSoup(main_html, 'html.parser')
    time.sleep(2)

    community_html = urllib.request.urlopen(project_url + "/community", context=ctx).read()
    community_html = community_html.decode()
    community_soup = BeautifulSoup(community_html, 'html.parser') 
    
    data_object = {
        "status": 1,
        "projID": projID,
        "name": getProjName(main_soup),
        "backerCount": getCampaignStats(main_soup)["backerCount"],
        "amount": getCampaignStats(main_soup)["amount"],
        "currency": getCampaignStats(main_soup)["currency"],
        "target": getTargetAmount(main_soup),
        "initCity": getCityCategory(main_soup)["city"],
        "category": getCityCategory(main_soup)["category"],
        "fundingPeriodDays": getFundingPeriodLength(main_soup),
        "pledgeJSON": json.dumps(getPledgingOptions(main_soup)),
        "countryBackerJSON": json.dumps(getTopCountries(community_soup)),
        "existingBackerCount": getExistingBacker(community_soup),
        "newBackerCount": getNewBacker(community_soup),
        "URL": project_url,
    }
    # except:
    #     data_object = {
    #         "status": 0,
    #         "projID": projID,
    #         "URL": project_url
    #     }
    return data_object

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

# Reading URLs to be retrieved
cur.execute("SELECT * FROM UrlDB")
urlrows = cur.fetchall()

# Adding a resume function - check if url has been matched
cur.execute("SELECT URL FROM DetailsDB")
readUrlRows = cur.fetchall()
readUrl = []
for readUrlRow in readUrlRows:
    readUrl.append(readUrlRow[0])

# Scrape and add to database
for urlrow in urlrows:
    project_url = urlrow[1]
    if(project_url not in readUrl):
        print("Start Retrieving - ", urlrow[0], " - ", project_url)
        data_object = gatherDetails(urlrow[0], project_url)
        if(data_object["status"] == 1):
            cur.execute("INSERT INTO DetailsDB (projID, name, backerCount, amount, currency, target, initCity, category, fundingPeriodDays, pledgeJSON, countryBackerJSON, existingBackerCount, newBackerCount, URL) VALUES (:projID, :name, :backerCount, :amount, :currency, :target, :initCity, :category, :fundingPeriodDays, :pledgeJSON, :countryBackerJSON, :existingBackerCount, :newBackerCount, :URL)", data_object)
        else:
            cur.execute("INSERT INTO errorDB (errorURL, projID) VALUES (:errorURL, :projID)", data_object)
        conn.commit()
        time.sleep(2)
