#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import re
import statistics

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
