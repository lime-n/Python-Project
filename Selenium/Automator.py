#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import re
import statistics


# In[2]:


cv_jobs = pd.read_json('/Users/emiljanmrizaj/Scrapy_Tickets/Items/money/money/cv_jobs2_regions2.jl', lines=True)
cv_jobs = cv_jobs.dropna()
cv_jobs=cv_jobs[~(cv_jobs['salary'].str.contains('hour|day', na=False))]
cv_jobs=cv_jobs.dropna()
cv_jobs['category'] = cv_jobs['category'].str.lower()
cv_jobs=cv_jobs[cv_jobs.salary.str.contains('[0-9]',regex=True)]
cv_jobs["salary"] = cv_jobs["salary"].str.partition("/")[0].str.replace("[^\d-]+", "", regex=True)
def convert_rec(x):
    if isinstance(x, list):
        return list(map(convert_rec, x))
    else:
        return int(x)
salary=list(map(convert_rec, cv_jobs.salary.str.split('-')))
salary1 = []
for i in salary:
    salary1.append(statistics.mean(i))
    
cv_jobs['salary'] = salary1
cv_jobs['salary']=[int(x) for x in cv_jobs.salary]
total_salary = []
for x in cv_jobs.salary:
    if len(str(x)) == 1:
        total_salary.append(x * 40 * 52)
    elif len(str(x)) == 2:
        total_salary.append(x * 5 * 52)
    elif len(str(x)) == 3:
        total_salary.append(x * 5 * 52)
    elif len(str(x)) == 4:
        total_salary.append(x * 12)
    elif x:
        total_salary.append(x)
cv_jobs['salary']=total_salary
df=cv_jobs['salary']<=50000
cv_jobs = cv_jobs[df]


# In[3]:


#Clean the strings and merge with countries
cv_jobs['region']=cv_jobs.region.str.partition('\n')[2].str.strip()
cv_jobs['region']=cv_jobs.region.str.lower()
country = pd.read_csv("/Volumes/Seagate/Work/Tickets/Jobs/Jobs/names_country2.csv")
pattern = re.compile("(moray|highland|perth-and-kinross|darlington|staffordshire-moorlands|pendle|east-ayrshire|clackmannanshire|west-lothian|ealing-london-boro|county-durham|aberdeen-city|angus|midlothian|burnley|oldham|stockport|powys|stirling|dumfries-and-galloway|north-lanarkshire|dundee-city|craven|scottish-borders|glasgow-city|east-riding-of-yorkshire|carlisle|east-lothian|south-ayrshire|aberdeenshire|edinburgh|south-derbyshire|ribble-valley|east-renfrewshire|derbyshire-dales|west-dunbartonshire|west-lindsey|richmondshire|tamworth|middlesbrough|york|west-northamptonshire|rhondda-cynon-taf|hillingdon-london-boro|rossendale|south-lanarkshire|na-h-eileanan-an-iar|leicester|south-norfolk|tameside|redditch|maldon|oxford|westminster|stoke-on-trent|hartlepool|bolton|newcastle-upon-tyne|cheshire-east|gateshead|ryedale|northumberland|falkirk|manchester|doncaster|bracknell-forest|dudley|renfrewshire|stafford|cardiff|south-holland|nottingham|north-somerset|inverclyde|east-cambridgeshire|sandwell|hammersmith-and-fulham|east-suffolk|torridge|warrington|east-dunbartonshire|surrey-heath|milton-keynes|south-lakeland|south-oxfordshire|south-cambridgeshire|chorley|waltham-forest|cotswold|eden|havering|richmond-upon-thames|colchester|wolverhampton|north-devon|canterbury|rochdale|caerffili|fife|reigate-and-banstead|cambridge|lichfield|redbridge|chichester|east-lindsey|gwynedd|coventry|bath-and-north-east-somerset|west-oxfordshire|argyll-and-bute|swindon|mole-valley|cannock-chase|huntingdonshire|buckinghamshire|orkney-islands|chesterfield|hart|west-lancashire|norwich|north-kesteven|bournemouth|horsham|luton|ashfield|selby|mid-sussex|walsall|newark-and-sherwood|lincoln|neath-port-talbot|cheshire|wirral|north-ayrshire|gloucester|worcester|swansea|rutland|isle-of-wight|st.-helens|stevenage|amber-valley|castle-point|derby|gravesham|somerset-west|woking|east-staffordshire|rushcliffe|teignbridge|south-staffordshire|rochford|reading|worthing|north-tyneside|south-kesteven|winchester|trafford|barrow-in-furness|greenwich|bolsover|redcar-and-cleveland|barnet|tewkesbury|hinckley-and-bosworth|north-lincolnshire|fareham|epping-forest|cornwall|bexley|mansfield|salford|southend-on-sea|bedford|bury|crawley|north-norfolk|sedgemoor|hambleton|boston|malvern-hills|haringey|gosport|rushmoor|derbyshire|preston|braintree|county-of-herefordshire|newham|new-forest|harborough|basildon|newcastle-under-lyme|broxtowe|bridgend|sheffield|uttlesford|wigan|lancaster|chelmsford|mendip|vale-of-white-horse|south-ribble|tendring|dorset|stockton-on-tees|pembrokeshire|bristol|rotherham|forest-of-dean|calderdale|torfaen|epsom-and-ewell|blackpool|enfield-london-boro|telford-and-wrekin|erewash|newport|stroud|monmouthshire|scarborough|harlow|south-somerset|gedling|sutton-london-boro|ceredigion|charnwood|blackburn-with-darwen|stratford-on-avon|warwick|camden|copeland|eastbourne|maidstone|south-tyneside|shropshire|wokingham|bromsgrove|allerdale|fenland|folkestone-and-hythe|broxbourne|north-east-lincolnshire|elmbridge|oadby-and-wigston|shetland-islands|blaenau-gwent|lewes|merton-london-boro|west-suffolk|sunderland|runnymede|croydon|nuneaton-and-bedworth|dartford|islington|sevenoaks|lambeth|arun|west-devon|south-hams|north-hertfordshire|harrow|exeter|medway|broadland|dacorum|london|solihull|west-berkshire|the-brighton-and-hove|east-hampshire|wiltshire|three-rivers|hounslow|sefton|ashford|cherwell|hackney|bradford|wyre|east-devon|high-peak|slough|tower-hamlets|adur|wyre-forest|harrogate|isle-of-anglesey|lewisham|rother|southampton|plymouth|peterborough|hastings|breckland|ipswich|cheltenham|north-northamptonshire|thanet|blaby|thurrock|havant|wealden|tandridge|birmingham|fylde|denbighshire|knowsley|isles-of-scilly|merthyr-tydfil|waverley|test-valley|eastleigh|tunbridge-wells|carmarthenshire|brentwood|great-yarmouth|wychavon|the-vale-of-glamorgan|melton|swale|welwyn-hatfield|wandsworth|halton|north-west-leicestershire|conwy|kingston-upon-hull|babergh|torbay|portsmouth|kirklees|wrecsam---wrexham|windsor-and-maidenhead|kensington-and-chelsea|watford|hertsmere|north-warwickshire|kingston-upon-thames|dover|rugby|barking-and-dagenham|liverpool|flintshire|tonbridge-and-malling|wakefield|spelthorne|mid-suffolk|south-gloucestershire|brent-london-boro|basingstoke-and-deane|mid-devon|barnsley|central-bedfordshire|leeds|bassetlaw|east-hertfordshire|hyndburn|king's-lynn-and-west-norfolk|southwark-london-boro|st-albans|bromley-london-boro|guildford)")
import re
cv_jobs['region']=[str(x) for x in cv_jobs.region]
cv_jobs.region=[re.sub(' ','-',x) for x in cv_jobs.region]
cv_jobs.region = [re.sub(',','',x) for x in cv_jobs.region]
cv_jobs.region=cv_jobs.region.str.extract(pattern)
cv_jobs=cv_jobs.merge(country)


# In[4]:


reed = pd.read_json('/Users/emiljanmrizaj/Scrapy_Tickets/Items/money_test/money_test/reed_jobs.jl', lines=True)
reed = reed.dropna()
reed = reed[~reed.salary.str.contains('hour|day', na=False)]
reed=reed[reed.salary.str.contains('[0-9]',regex=True)]
reed['salary'] = reed['salary'].str.extract(r'£(\d+(?:,\d+)*(?:\.\d+)?(?:\s*-\s*£\d+(?:,\d+)*(?:\.\d+)?)?)')[0].str.replace(r'[^\d-]+', '', regex=True)
reed=reed.reset_index()
reed=reed.drop('index',axis=1)
reed=reed.dropna()
def convert_rec(x):
    if isinstance(x, list):
        return list(map(convert_rec, x))
    else:
        return int(x)
salary=list(map(convert_rec, reed.salary.str.split('-')))
salary1 = []
for i in salary:
    salary1.append(statistics.mean(i))
    
reed['salary'] = salary1
reed['salary']=[int(x) for x in reed.salary]
total_salary = []
for x in reed.salary:
    if len(str(x)) == 1:
        total_salary.append(x * 40 * 52)
    elif len(str(x)) == 2:
        total_salary.append(x * 40 * 52)
    elif len(str(x)) == 3:
        total_salary.append(x * 5 * 52)
    elif len(str(x)) == 4:
        total_salary.append(x * 12)
    elif x:
        total_salary.append(x)
reed['salary']=total_salary
df=reed['salary']<=50000
reed=reed[df]


# In[5]:


reed['category']=reed.category.str.lower()
query = 'degree (.*) jobs'
category = []
for i in reed.category:
    category.append(re.search(query, i).group(1))
reed['category']=category
reed=reed.reset_index()
reed=reed.drop('index', axis=1)
pattern = re.compile("(anatomy|physiology|forestry|anthropology|agriculture|accounting|archaeology|aeronautical-engineering|finance|manufacturing-engineering|art|architecture|biology|design|chemical-engineering|classics|business-studies|building|civil-engineering|counselling|media-studies|computer-science|chemistry|creative-writing|criminology|dentistry|drama|economics|education|electrical-engineering|electronic-engineering|english|fashion|film-making|food-science|forensic-science|engineering|geography|environmental-sciences|geology|social-care|health-care|history|hospitality|information-technology|land-and-property-management|law|linguistics|marketing|materials|mathematics|mechanical-engineering|medical|medicine|music|nursing|therapy|pharmacy|philosophy|physics|physiotherapy|psychology|politics|robotics|sociology|statistics|sports-science|veterinary-medicine|youth-work)")
reed['category']=reed.category.str.extract(pattern)



# In[6]:


reed


# In[108]:


reed['region'] = reed.region.str.partition('\n')[2].str.strip().str.lower()
reed.region = [str(x) for x in reed.region]
reed.region = [re.sub(' ','-',x) for x in reed.region]
reed.region = [re.sub(',','',x) for x in reed.region]
pattern = re.compile("(moray|highland|perth-and-kinross|darlington|staffordshire-moorlands|pendle|east-ayrshire|clackmannanshire|west-lothian|ealing-london-boro|county-durham|aberdeen-city|angus|midlothian|burnley|oldham|stockport|powys|stirling|dumfries-and-galloway|north-lanarkshire|dundee-city|craven|scottish-borders|glasgow-city|east-riding-of-yorkshire|carlisle|east-lothian|south-ayrshire|aberdeenshire|edinburgh|south-derbyshire|ribble-valley|east-renfrewshire|derbyshire-dales|west-dunbartonshire|west-lindsey|richmondshire|tamworth|middlesbrough|york|west-northamptonshire|rhondda-cynon-taf|hillingdon-london-boro|rossendale|south-lanarkshire|na-h-eileanan-an-iar|leicester|south-norfolk|tameside|redditch|maldon|oxford|westminster|stoke-on-trent|hartlepool|bolton|newcastle-upon-tyne|cheshire-east|gateshead|ryedale|northumberland|falkirk|manchester|doncaster|bracknell-forest|dudley|renfrewshire|stafford|cardiff|south-holland|nottingham|north-somerset|inverclyde|east-cambridgeshire|sandwell|hammersmith-and-fulham|east-suffolk|torridge|warrington|east-dunbartonshire|surrey-heath|milton-keynes|south-lakeland|south-oxfordshire|south-cambridgeshire|chorley|waltham-forest|cotswold|eden|havering|richmond-upon-thames|colchester|wolverhampton|north-devon|canterbury|rochdale|caerffili|fife|reigate-and-banstead|cambridge|lichfield|redbridge|chichester|east-lindsey|gwynedd|coventry|bath-and-north-east-somerset|west-oxfordshire|argyll-and-bute|swindon|mole-valley|cannock-chase|huntingdonshire|buckinghamshire|orkney-islands|chesterfield|hart|west-lancashire|norwich|north-kesteven|bournemouth|horsham|luton|ashfield|selby|mid-sussex|walsall|newark-and-sherwood|lincoln|neath-port-talbot|cheshire|wirral|north-ayrshire|gloucester|worcester|swansea|rutland|isle-of-wight|st.-helens|stevenage|amber-valley|castle-point|derby|gravesham|somerset-west|woking|east-staffordshire|rushcliffe|teignbridge|south-staffordshire|rochford|reading|worthing|north-tyneside|south-kesteven|winchester|trafford|barrow-in-furness|greenwich|bolsover|redcar-and-cleveland|barnet|tewkesbury|hinckley-and-bosworth|north-lincolnshire|fareham|epping-forest|cornwall|bexley|mansfield|salford|southend-on-sea|bedford|bury|crawley|north-norfolk|sedgemoor|hambleton|boston|malvern-hills|haringey|gosport|rushmoor|derbyshire|preston|braintree|county-of-herefordshire|newham|new-forest|harborough|basildon|newcastle-under-lyme|broxtowe|bridgend|sheffield|uttlesford|wigan|lancaster|chelmsford|mendip|vale-of-white-horse|south-ribble|tendring|dorset|stockton-on-tees|pembrokeshire|bristol|rotherham|forest-of-dean|calderdale|torfaen|epsom-and-ewell|blackpool|enfield-london-boro|telford-and-wrekin|erewash|newport|stroud|monmouthshire|scarborough|harlow|south-somerset|gedling|sutton-london-boro|ceredigion|charnwood|blackburn-with-darwen|stratford-on-avon|warwick|camden|copeland|eastbourne|maidstone|south-tyneside|shropshire|wokingham|bromsgrove|allerdale|fenland|folkestone-and-hythe|broxbourne|north-east-lincolnshire|elmbridge|oadby-and-wigston|shetland-islands|blaenau-gwent|lewes|merton-london-boro|west-suffolk|sunderland|runnymede|croydon|nuneaton-and-bedworth|dartford|islington|sevenoaks|lambeth|arun|west-devon|south-hams|north-hertfordshire|harrow|exeter|medway|broadland|dacorum|london|solihull|west-berkshire|the-brighton-and-hove|east-hampshire|wiltshire|three-rivers|hounslow|sefton|ashford|cherwell|hackney|bradford|wyre|east-devon|high-peak|slough|tower-hamlets|adur|wyre-forest|harrogate|isle-of-anglesey|lewisham|rother|southampton|plymouth|peterborough|hastings|breckland|ipswich|cheltenham|north-northamptonshire|thanet|blaby|thurrock|havant|wealden|tandridge|birmingham|fylde|denbighshire|knowsley|isles-of-scilly|merthyr-tydfil|waverley|test-valley|eastleigh|tunbridge-wells|carmarthenshire|brentwood|great-yarmouth|wychavon|the-vale-of-glamorgan|melton|swale|welwyn-hatfield|wandsworth|halton|north-west-leicestershire|conwy|kingston-upon-hull|babergh|torbay|portsmouth|kirklees|wrecsam---wrexham|windsor-and-maidenhead|kensington-and-chelsea|watford|hertsmere|north-warwickshire|kingston-upon-thames|dover|rugby|barking-and-dagenham|liverpool|flintshire|tonbridge-and-malling|wakefield|spelthorne|mid-suffolk|south-gloucestershire|brent-london-boro|basingstoke-and-deane|mid-devon|barnsley|central-bedfordshire|leeds|bassetlaw|east-hertfordshire|hyndburn|king's-lynn-and-west-norfolk|southwark-london-boro|st-albans|bromley-london-boro|guildford)")
reed.region = reed.region.str.extract(pattern).dropna()
reed = reed.merge(country)


# In[109]:


total_data = pd.concat([cv_jobs, reed], axis=0)
df=total_data['salary']<=50000
total_data=total_data[df]
total_data=total_data.reset_index()
total_data=total_data.drop('index', axis=1)
total_data=total_data.drop_duplicates()
total_data=total_data.reset_index()
total_data = total_data.drop('index', axis=1)


# In[110]:


total_data['interest_rate'] = 0.09


# In[111]:


tuition = []
for i in total_data.Country:
    if i == "England":
        tuition.append(9250)
    elif i == "Scotland":
        tuition.append(1820)
    elif i == "Wales":
        tuition.append(9000)


# In[112]:


total_data['tuition'] = tuition
tuition =[]
for i,j in zip(total_data.category,total_data.tuition):
    if i == 'medicine':
        tuition.append(j*5)
    elif i != 'medicine':
        tuition.append(j*3)
    


# In[113]:


total_data['tuition'] = tuition
total_data['monthly_salary']=[x/12.0 for x in total_data.salary]
total_data.monthly_salary=total_data.monthly_salary.round(0)
total_data.monthly_salary=[int(x) for x in total_data.monthly_salary]
total_data['interest'] = 4.4


# In[114]:


ranges = []
for i in total_data.salary:
    if (i >= 27228) and (i <= 30000):
        ranges.append("£27288 - £30000")
    elif (i >= 0) and (i <= 27288):
        ranges.append("£27288>")
    elif (i >= 30000) and (i <= 40000):
        ranges.append("£30000 - £40000")
    elif (i >= 40000) and (i <= 50000):
        ranges.append("£40000 - £50000")
    #elif (i >= 50000) and (i <= 60000):
    #    ranges.append("50000 - 60000")
    #elif (i >= 60000):
    #    ranges.append("£60000<")


# In[115]:


total_data['ranges']=ranges


# In[116]:


names = pd.read_csv("/Users/emiljanmrizaj/Scrapy_Tickets/Items/indeed/indeed/degree_names2.csv")


# In[117]:


names.upper=names.upper.str.lower()


# In[118]:


names=names.drop(['degrees', 'degree_type'], axis=1)
names=names.rename(columns={'upper':'category', 'degrees':'degrees','Sector':'Sector', 'degree_type':'degree_type'})
total_data=total_data.merge(names, how='inner',on='category')


# In[119]:


data_organisation=total_data.set_index(
    [#'category', 
               'Country','Sector', #'organisation',
     #'ranges'
               ]).groupby(
    [#'category', 
             'Country','Sector', #'organisation',
     #'ranges'
             ], group_keys=False).agg(
    {
            'salary':'mean',
            'tuition':'mean',
            'title':'count',
            'interest':'mean'
})


# In[120]:


data_organisation1=data_organisation.reset_index()


# In[121]:


data_organisation1.dtypes


# In[122]:


df = data_organisation1['title'] > 10


# In[123]:


data_organisation1=data_organisation1[df]


# In[124]:


data_organisation1


# In[51]:


data_organisation_E_W = data_organisation1[data_organisation1.Country.str.contains('England|Wales', na=False)]
data_organisation_S = data_organisation1[data_organisation1.Country.str.contains('Scotland', na=False)]


# In[32]:


data_organisation1.to_csv("sector_organisation.csv")


# In[124]:


#Range 0 - 30k
df=total_data['salary']<= 30000
total_data1=total_data[df]

#Range 30k - 40k
df=total_data['salary'].between(30000, 40000, inclusive = True)
total_data2=total_data[df]

#Range 40k - 50k
df=total_data['salary'].between(40000, 50000, inclusive = True)
total_data3=total_data[df]

#Range 50k - 60k
df=total_data['salary'].between(50000, 60000, inclusive = True)
total_data4=total_data[df]

#Range 60k+
df=total_data['salary']>= 60000
total_data5=total_data[df]


# In[130]:


data_without_organisation1=total_data1.set_index(
    ['category', 
               'Country'#, 'organisation'
               ]).groupby(
    ['category', 
             'Country'#, 'organisation'
             ], group_keys=False).agg(
    {
            'salary':'mean',
            'tuition':'mean',
            'title':'count'
})

data_without_organisation2=total_data2.set_index(
    ['category', 
               'Country'#, 'organisation'
               ]).groupby(
    ['category', 
             'Country'#, 'organisation'
             ], group_keys=False).agg(
    {
            'salary':'mean',
            'tuition':'mean',
            'title':'count'
})

data_without_organisation3=total_data3.set_index(
    ['category', 
               'Country'#, 'organisation'
               ]).groupby(
    ['category', 
             'Country'#, 'organisation'
             ], group_keys=False).agg(
    {
            'salary':'mean',
            'tuition':'mean',
            'title':'count'
})

data_without_organisation4=total_data4.set_index(
    ['category', 
               'Country'#, 'organisation'
               ]).groupby(
    ['category', 
             'Country'#, 'organisation'
             ], group_keys=False).agg(
    {
            'salary':'mean',
            'tuition':'mean',
            'title':'count'
})

data_without_organisation5=total_data5.set_index(
    ['category', 
               'Country'#, 'organisation'
               ]).groupby(
    ['category', 
             'Country'#, 'organisation'
             ], group_keys=False).agg(
    {
            'salary':'mean',
            'tuition':'mean',
            'title':'count'
})

data_with_organisation1=total_data1.set_index(
    ['category', 
               'Country'#, 'organisation'
               ]).groupby(
    ['category', 
             'Country'#, 'organisation'
             ], group_keys=False).agg(
    {
            'salary':'mean',
            'tuition':'mean',
            'title':'count'
})

data_with_organisation2=total_data2.set_index(
    ['category', 
               'Country'#, 'organisation'
               ]).groupby(
    ['category', 
             'Country'#, 'organisation'
             ], group_keys=False).agg(
    {
            'salary':'mean',
            'tuition':'mean',
            'title':'count'
})

data_with_organisation3=total_data3.set_index(
    ['category', 
               'Country'#, 'organisation'
               ]).groupby(
    ['category', 
             'Country'#, 'organisation'
             ], group_keys=False).agg(
    {
            'salary':'mean',
            'tuition':'mean',
            'title':'count'
})

data_with_organisation4=total_data4.set_index(
    ['category', 
               'Country'#, 'organisation'
               ]).groupby(
    ['category', 
             'Country'#, 'organisation'
             ], group_keys=False).agg(
    {
            'salary':'mean',
            'tuition':'mean',
            'title':'count'
})

data_with_organisation5=total_data5.set_index(
    ['category', 
               'Country'#, 'organisation'
               ]).groupby(
    ['category', 
             'Country'#, 'organisation'
             ], group_keys=False).agg(
    {
            'salary':'mean',
            'tuition':'mean',
            'title':'count'
})


# In[131]:


data_without_organisation1=data_without_organisation1.reset_index()
data_without_organisation2=data_without_organisation2.reset_index()
data_without_organisation3=data_without_organisation3.reset_index()
data_without_organisation4=data_without_organisation4.reset_index()
data_without_organisation5=data_without_organisation5.reset_index()

data_with_organisation1=data_with_organisation1.reset_index()
data_with_organisation2=data_with_organisation2.reset_index()
data_with_organisation3=data_with_organisation3.reset_index()
data_with_organisation4=data_with_organisation4.reset_index()
data_with_organisation5=data_with_organisation5.reset_index()


# In[132]:


data_without_organisation1


# In[94]:


data_without_organisation.groupby(['category']).describe()


# In[17]:


total_data.to_csv("total_data.csv")


# In[32]:


total_data.to_csv('total_data.csv')


# In[ ]:


data_organisation_E_W
data_organisation_S


# # MULTITHREADING CAPABILITY TO GET LOAN REPAYMENT

# In[53]:


from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait 
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import pandas as pd
from collections import defaultdict
from multiprocessing.pool import ThreadPool
import time
import threading
import gc
import timeit

data = pd.read_csv("/Users/emiljanmrizaj/Downloads/total_data-3.csv")

debt1 = []
salary1 = []
loan1 = []
plan21 = []
button1 = []
advanced = []
interest = []


for i in range(0, len(data)):
    i
    debt1.append("//input[@id='debt']")
    salary1.append("//input[@id='salary']")
    loan1.append("//select[@id='loan-type']")
    plan21.append("//select[@id='loan-type']/option[2]")
    button1.append("//button[@class='btn btn-primary calculate-button']")
    advanced.append("//input[@id='advanced-options-checkbox']")
    interest.append("//input[@id='interest-rate']")

class Driver:
    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        # suppress logging:
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.driver = webdriver.Chrome(options=options)
        print('The driver was just created.')

    def __del__(self):
        self.driver.quit() # clean up driver when we are cleaned up
        print('The driver has terminated.')


threadLocal = threading.local()

def create_driver():
    the_driver = getattr(threadLocal, 'the_driver', None)
    if the_driver is None:
        the_driver = Driver()
        setattr(threadLocal, 'the_driver', the_driver)
    return the_driver.driver


def get_title(tpl):
    start = timeit.default_timer()
    # Unpack tuple
    idx, salary, tuition,interest, sector,title, country, deb, sal, plan, lo,adv,intr, but = tpl
    driver = create_driver()
    driver.get("https://www.student-loan-calculator.co.uk/")     
    driver.find_element(By.XPATH, sal
                            ).clear()
    driver.find_element(By.XPATH, sal
                            ).send_keys(salary)
    driver.find_element(By.XPATH, deb
                            ).clear()
    driver.find_element(By.XPATH, deb
                            ).send_keys(str(tuition))
    driver.find_element(By.XPATH, lo).click()
    driver.find_element(By.XPATH, plan).click()
    driver.find_element(By.XPATH, adv).click()
    driver.find_element(By.XPATH, intr
                            ).clear()
    driver.find_element(By.XPATH, intr
                            ).send_keys(interest)
    
    driver.find_element(By.XPATH, but).click()
    source = pd.read_html(driver.page_source)[0].assign(Country = title, Category = country, Count = sector,  interest = interest)
    stop = timeit.default_timer()
    print(f"You're on this country: {country} and this row number {idx + 1}", 'And the total time is:', stop - start)
    # Return the results back to the main thread so that they
    # will be appended in the correct, that is to say, task submission order:
    return source, category, country



tables_organisation4 = defaultdict(list)
with ThreadPool(10) as pool:
    # The imap method allws us to (1) process the results as they are returned one-by-one and there is
    # also no need to turn zip into a list:
    for result in pool.imap(get_title, zip(range(len(data)),
                                           data_organisation_S.salary,
                                           data_organisation_S.tuition,
                                           data_organisation_S.Country,
                                           data_organisation_S.Sector,
                                           data_organisation_S.title,
                                           #data_organisation_E_W.ranges,
                                           data_organisation_S.interest,
                                           debt1,
                                           salary1,
                                           loan1,
                                           plan21,
                                           advanced,
                                           interest,
                                           button1)
                       ):
        # Unpack result tuple:
        source, sector, country = result
        tables_organisation4['table'].append(source)
        tables_organisation4['category'].append(sector)
        tables_organisation4['country'].append(country)
        
    # must be done before terminate is explicitly or implicitly called on the pool:
    del threadLocal
    gc.collect()


# In[55]:


tables_organisation1


# # A SLOWER ALTERNATIVE EXAMPLE

# In[34]:


from multiprocessing.pool import ThreadPool
from bs4 import BeautifulSoup
from selenium import webdriver
import threading
import gc

class Driver:
    def __init__(self):
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        # suppress logging:
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.driver = webdriver.Chrome(options=options)
        print('The driver was just created.')

    def __del__(self):
        self.driver.quit() # clean up driver when we are cleaned up
        print('The driver has terminated.')


threadLocal = threading.local()

def create_driver():
    the_driver = getattr(threadLocal, 'the_driver', None)
    if the_driver is None:
        the_driver = Driver()
        setattr(threadLocal, 'the_driver', the_driver)
    return the_driver.driver


def get_title(url):
    driver = create_driver()
    i = 0
    #driver = webdriver.Chrome()
    tables_debt = defaultdict(list)
    while i < len(data):
        for salary,tuition,category,country,deb, sal, plan, lo, but in zip(data.salary,data.tuition,data.category, data.Country,debt1,salary1, loan1, plan21, button1):
            start = timeit.default_timer()
            driver.get(url)     
            driver.find_element(By.XPATH, sal
                                    ).clear()
            driver.find_element(By.XPATH, sal
                                    ).send_keys(salary)
            driver.find_element(By.XPATH, deb
                                    ).clear()
            driver.find_element(By.XPATH, deb
                                    ).send_keys(str(tuition))
            driver.find_element(By.XPATH, lo).click()
            driver.find_element(By.XPATH, plan).click()
            driver.find_element(By.XPATH, but).click()
            driver2 = driver.page_source
            tables_debt['table'].append(pd.read_html(driver2)[0].assign(Category=category, Country=country))
            i+= 1
            stop = timeit.default_timer()
            print(f"You're on this country: {country} and this row number {i}", 'And the total time is:', stop - start)



# just 2 threads in our pool for demo purposes:
with ThreadPool(4000) as pool:
    urls = [
        "https://www.student-loan-calculator.co.uk/"
    ]
    pool.map(get_title, urls)
    # must be done before terminate is explicitly or implicitly called on the pool:
    del threadLocal
    gc.collect()
# pool.terminate() is called at exit of with block


# # SLOWEST EXAMPLE

# In[ ]:


i = 0
driver = webdriver.Chrome()
tables_debt = defaultdict(list)
while i < len(data):
    for salary,tuition,category,country,deb, sal, plan, lo, but in zip(data.salary,data.tuition,data.category, data.Country,debt1,salary1, loan1, plan21, button1):
        
        start = timeit.default_timer()
        driver.get("https://www.student-loan-calculator.co.uk/")     
        driver.find_element(By.XPATH, sal
                                ).clear()
        driver.find_element(By.XPATH, sal
                                ).send_keys(salary)
        driver.find_element(By.XPATH, deb
                                ).clear()
        driver.find_element(By.XPATH, deb
                                ).send_keys(str(tuition))
        driver.find_element(By.XPATH, lo).click()
        driver.find_element(By.XPATH, plan).click()
        driver.find_element(By.XPATH, but).click()
        driver2 = driver.page_source
        
        tables_debt['table'].append(pd.read_html(driver2)[0].assign(Category=category, Country=country))
        print(tables_debt)

        i+= 1
        stop = timeit.default_timer()
        print(f"You're on this country: {country} and this row number {i}", 'And the total time is:', stop - start)


# # FILTERING DONE HERE

# In[6]:


more_data.to_csv("loan_repayment.csv")


# In[23]:


import re
more_data = pd.concat([pd.concat([x]) for x in tables_debt['table']])
more_data=more_data.drop('Unnamed: 9', axis=1)
more_data.Salary=[re.sub('£|,','',x) for x in more_data.Salary]
more_data.Debt=[re.sub('£|,','',x) for x in more_data.Debt]
more_data['Paid This Year']=[re.sub('£|,','',x) for x in more_data['Paid This Year']]
more_data['Interest This Year']=[re.sub('£|,','',x) for x in more_data['Interest This Year']]
more_data['Total Paid']=[re.sub('£|,','',x) for x in more_data['Total Paid']]
more_data['Total Interest']=[re.sub('£|,','',x) for x in more_data['Total Interest']]


# In[29]:


more_data.Salary=[int(x) for x in more_data.Salary]


# In[36]:


for salary, years in zip(more_data.Salary, more_data['#']):
    if years == 1:
        print(f'The first year has this salary: {salary}')


# # Without an Organisation

# In[138]:


tables_without_organisation1=pd.concat([pd.concat([x]) for x in tables_without_organisation1['table']])
tables_without_organisation2=pd.concat([pd.concat([x]) for x in tables_without_organisation2['table']])
tables_without_organisation3=pd.concat([pd.concat([x]) for x in tables_without_organisation3['table']])
tables_without_organisation4=pd.concat([pd.concat([x]) for x in tables_without_organisation4['table']])
#tables_without_organisation5=pd.concat([pd.concat([x]) for x in tables_without_organisation5['table']])




# In[90]:


#tables_organisation1=pd.concat([pd.concat([x]) for x in tables_organisation1['table']])
#tables_organisation2=pd.concat([pd.concat([x]) for x in tables_organisation2['table']])
#tables_organisation3=pd.concat([pd.concat([x]) for x in tables_organisation3['table']])
#tables_organisation4=pd.concat([pd.concat([x]) for x in tables_organisation4['table']])



tables_organisation=pd.concat([tables_organisation1,tables_organisation2], axis=0)
#tables_organisation= pd.concat([tables_organisation3,tables_organisation4], axis=0)
import re
tables_organisation=tables_organisation.drop('Unnamed: 9', axis=1)
tables_organisation.Salary=[re.sub('£|,','',x) for x in tables_organisation.Salary]
tables_organisation.Debt=[re.sub('£|,','',x) for x in tables_organisation.Debt]
tables_organisation['Paid This Year']=[re.sub('£|,','',x) for x in tables_organisation['Paid This Year']]
tables_organisation['Interest This Year']=[re.sub('£|,','',x) for x in tables_organisation['Interest This Year']]
tables_organisation['Total Paid']=[re.sub('£|,','',x) for x in tables_organisation['Total Paid']]
tables_organisation['Total Interest']=[re.sub('£|,','',x) for x in tables_organisation['Total Interest']]


tables_organisation.Salary = [int(x) for x in tables_organisation.Salary]


tables_organisation.Debt = [int(x) for x in tables_organisation.Debt]

tables_organisation['Total Paid'] = [int(x) for x in tables_organisation['Total Paid']]

tables_organisation['Total Interest'] = [int(x) for x in tables_organisation['Total Interest']]

tables_organisation['Int. Rate %'] = [int(x) for x in tables_organisation['Int. Rate %']]



# In[91]:


import numpy as np


# In[92]:


tables_organisation


# In[93]:


tables_organisation_t=tables_organisation.set_index(
    ['interest','Count']).groupby(
    ['interest','Count']).agg(
    {
        'Salary':[np.max, np.min],
        'Debt':'max',
        'Total Paid':'max',
        'Total Interest':'max',
        'Country':'max',
        'Year':'max'
        
    })


# In[94]:


tables_organisation_t


# In[95]:


data=tables_organisation_t.reset_index()


# In[96]:


data


# In[97]:


data.to_csv("degree2_data_median2.csv")


# In[269]:


data.set_index(['Category', 'Count', 'Ranges']).groupby(['Category', 'Count', 'Ranges']).agg({
    'Debt':'max',
    'Year':'max',
    'Salary':'max',
    'Total Paid':'max',
    'Country':'max'
}).reset_index().to_csv("Sector_data.csv")


# In[148]:


data.to_csv("test_sector.csv")


# In[139]:


import re
tables_without_organisation1=tables_without_organisation1.drop('Unnamed: 9', axis=1)
tables_without_organisation1.Salary=[re.sub('£|,','',x) for x in tables_without_organisation1.Salary]
tables_without_organisation1.Debt=[re.sub('£|,','',x) for x in tables_without_organisation1.Debt]
tables_without_organisation1['Paid This Year']=[re.sub('£|,','',x) for x in tables_without_organisation1['Paid This Year']]
tables_without_organisation1['Interest This Year']=[re.sub('£|,','',x) for x in tables_without_organisation1['Interest This Year']]
tables_without_organisation1['Total Paid']=[re.sub('£|,','',x) for x in tables_without_organisation1['Total Paid']]
tables_without_organisation1['Total Interest']=[re.sub('£|,','',x) for x in tables_without_organisation1['Total Interest']]

import re
tables_without_organisation2=tables_without_organisation2.drop('Unnamed: 9', axis=1)
tables_without_organisation2.Salary=[re.sub('£|,','',x) for x in tables_without_organisation2.Salary]
tables_without_organisation2.Debt=[re.sub('£|,','',x) for x in tables_without_organisation2.Debt]
tables_without_organisation2['Paid This Year']=[re.sub('£|,','',x) for x in tables_without_organisation2['Paid This Year']]
tables_without_organisation2['Interest This Year']=[re.sub('£|,','',x) for x in tables_without_organisation2['Interest This Year']]
tables_without_organisation2['Total Paid']=[re.sub('£|,','',x) for x in tables_without_organisation2['Total Paid']]
tables_without_organisation2['Total Interest']=[re.sub('£|,','',x) for x in tables_without_organisation2['Total Interest']]


tables_without_organisation3=tables_without_organisation3.drop('Unnamed: 9', axis=1)
tables_without_organisation3.Salary=[re.sub('£|,','',x) for x in tables_without_organisation3.Salary]
tables_without_organisation3.Debt=[re.sub('£|,','',x) for x in tables_without_organisation3.Debt]
tables_without_organisation3['Paid This Year']=[re.sub('£|,','',x) for x in tables_without_organisation3['Paid This Year']]
tables_without_organisation3['Interest This Year']=[re.sub('£|,','',x) for x in tables_without_organisation3['Interest This Year']]
tables_without_organisation3['Total Paid']=[re.sub('£|,','',x) for x in tables_without_organisation3['Total Paid']]
tables_without_organisation3['Total Interest']=[re.sub('£|,','',x) for x in tables_without_organisation3['Total Interest']]


tables_without_organisation4=tables_without_organisation4.drop('Unnamed: 9', axis=1)
tables_without_organisation4.Salary=[re.sub('£|,','',x) for x in tables_without_organisation4.Salary]
tables_without_organisation4.Debt=[re.sub('£|,','',x) for x in tables_without_organisation4.Debt]
tables_without_organisation4['Paid This Year']=[re.sub('£|,','',x) for x in tables_without_organisation4['Paid This Year']]
tables_without_organisation4['Interest This Year']=[re.sub('£|,','',x) for x in tables_without_organisation4['Interest This Year']]
tables_without_organisation4['Total Paid']=[re.sub('£|,','',x) for x in tables_without_organisation4['Total Paid']]
tables_without_organisation4['Total Interest']=[re.sub('£|,','',x) for x in tables_without_organisation4['Total Interest']]


tables_without_organisation5=tables_without_organisation5.drop('Unnamed: 9', axis=1)
tables_without_organisation5.Salary=[re.sub('£|,','',x) for x in tables_without_organisation5.Salary]
tables_without_organisation5.Debt=[re.sub('£|,','',x) for x in tables_without_organisation5.Debt]
tables_without_organisation5['Paid This Year']=[re.sub('£|,','',x) for x in tables_without_organisation5['Paid This Year']]
tables_without_organisation5['Interest This Year']=[re.sub('£|,','',x) for x in tables_without_organisation5['Interest This Year']]
tables_without_organisation5['Total Paid']=[re.sub('£|,','',x) for x in tables_without_organisation5['Total Paid']]
tables_without_organisation5['Total Interest']=[re.sub('£|,','',x) for x in tables_without_organisation5['Total Interest']]


# In[151]:


tables_without_organisation1.dtypes


# In[152]:


tables_without_organisation1.Salary = [int(x) for x in tables_without_organisation1.Salary]
tables_without_organisation2.Salary = [int(x) for x in tables_without_organisation2.Salary]
tables_without_organisation3.Salary = [int(x) for x in tables_without_organisation3.Salary]
tables_without_organisation4.Salary = [int(x) for x in tables_without_organisation4.Salary]
tables_without_organisation5.Salary = [int(x) for x in tables_without_organisation5.Salary]

tables_without_organisation1.Debt = [int(x) for x in tables_without_organisation1.Debt]
tables_without_organisation2.Debt = [int(x) for x in tables_without_organisation2.Debt]
tables_without_organisation3.Debt = [int(x) for x in tables_without_organisation3.Debt]
tables_without_organisation4.Debt = [int(x) for x in tables_without_organisation4.Debt]
tables_without_organisation5.Debt = [int(x) for x in tables_without_organisation5.Debt]

tables_without_organisation1['Total Paid'] = [int(x) for x in tables_without_organisation1['Total Paid']]
tables_without_organisation2['Total Paid'] = [int(x) for x in tables_without_organisation2['Total Paid']]
tables_without_organisation3['Total Paid'] = [int(x) for x in tables_without_organisation3['Total Paid']]
tables_without_organisation4['Total Paid'] = [int(x) for x in tables_without_organisation4['Total Paid']]
tables_without_organisation5['Total Paid'] = [int(x) for x in tables_without_organisation5['Total Paid']]

tables_without_organisation1['Total Interest'] = [int(x) for x in tables_without_organisation1['Total Interest']]
tables_without_organisation2['Total Interest'] = [int(x) for x in tables_without_organisation2['Total Interest']]
tables_without_organisation3['Total Interest'] = [int(x) for x in tables_without_organisation3['Total Interest']]
tables_without_organisation4['Total Interest'] = [int(x) for x in tables_without_organisation4['Total Interest']]
tables_without_organisation5['Total Interest'] = [int(x) for x in tables_without_organisation5['Total Interest']]

tables_without_organisation1['Int. Rate %'] = [int(x) for x in tables_without_organisation1['Int. Rate %']]
tables_without_organisation2['Int. Rate %'] = [int(x) for x in tables_without_organisation2['Int. Rate %']]
tables_without_organisation3['Int. Rate %'] = [int(x) for x in tables_without_organisation3['Int. Rate %']]
tables_without_organisation4['Int. Rate %'] = [int(x) for x in tables_without_organisation4['Int. Rate %']]
tables_without_organisation5['Int. Rate %'] = [int(x) for x in tables_without_organisation5['Int. Rate %']]


# In[154]:


tables_without_organisation2


# In[159]:


tables_without_organisation1=tables_without_organisation1.set_index(
    ['Country','Count','Year']).groupby(
    ['Country','Count','Year']).agg(
    {
        'Salary':'mean',
        'Debt':'mean',
        'Total Paid':'mean',
        'Total Interest':'mean',
        'Category':'mean'
        
    })

tables_without_organisation2=tables_without_organisation2.set_index(
    ['Country','Count','Year']).groupby(
    ['Country','Count','Year']).agg(
    {
        'Salary':'mean',
        'Debt':'mean',
        'Total Paid':'mean',
        'Total Interest':'mean',
        'Category':'mean'
        
    })

tables_without_organisation3=tables_without_organisation3.set_index(
    ['Country','Count','Year']).groupby(
    ['Country','Count','Year']).agg(
    {
        'Salary':'mean',
        'Debt':'mean',
        'Total Paid':'mean',
        'Total Interest':'mean',
        'Category':'mean'
        
    })

tables_without_organisation4=tables_without_organisation4.set_index(
    ['Country','Count','Year']).groupby(
    ['Country','Count','Year']).agg(
    {
        'Salary':'mean',
        'Debt':'mean',
        'Total Paid':'mean',
        'Total Interest':'mean',
        'Category':'mean'
        
    })

tables_without_organisation5=tables_without_organisation5.set_index(
    ['Country','Count','Year']).groupby(
    ['Country','Count','Year']).agg(
    {
        'Salary':'mean',
        'Debt':'mean',
        'Total Paid':'mean',
        'Total Interest':'mean',
        'Category':'mean'
        
    })


# In[160]:


tables_without_organisation1 = tables_without_organisation1.reset_index()
tables_without_organisation2 = tables_without_organisation2.reset_index()
tables_without_organisation3 = tables_without_organisation3.reset_index()
tables_without_organisation4 = tables_without_organisation4.reset_index()
tables_without_organisation5 = tables_without_organisation5.reset_index()


# In[166]:


tables_without_organisation1['range'] = "£27288 - £30000"
tables_without_organisation2['range'] = "£30000 - £40000"
tables_without_organisation3['range'] = "£40000 - £50000"
tables_without_organisation4['range'] = "£50000 - £60000"
tables_without_organisation5['range'] = "£60000+"


# In[168]:


full_tables=pd.concat([tables_without_organisation5,tables_without_organisation4, tables_without_organisation3, tables_without_organisation2, tables_without_organisation1], axis=0)


# In[163]:


tables_without_organisation1.to_csv('table1.csv')
tables_without_organisation2.to_csv('table2.csv')
tables_without_organisation3.to_csv('table3.csv')
tables_without_organisation4.to_csv('table4.csv')
tables_without_organisation5.to_csv('table5.csv')


# In[169]:


full_tables.to_csv("full_loan.csv")


# In[50]:




