from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from pyotp import TOTP
# from breeze_connect import BreezeConnect
import time as tm
import os
import json
import platform 

# key_secret = json.load(open('config.json', 'r'))

# ICICI Auto Logon 
def ic_autologon():
    # icici = BreezeConnect(api_key=json.load(open('config.json', 'r'))['ICICI_API_KEY'])
    icici_session_url = json.load(open('config.json', 'r'))['ICICI_SESSION_URL']
    
    service = webdriver.chrome.service.Service('./chromedriver.exe' if platform.system()=='Windows' else './chromedriver')
    service.start()
    
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    
    driver = webdriver.Chrome(options=options)
    driver.get(icici_session_url)
    driver.implicitly_wait(10)
    username = driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div/div[2]/div/div[1]/input')
    password = driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div/div[2]/div/div[3]/div/input')
    
    icici_uname = json.load(open('config.json', 'r'))['ICICI_USER_NAME']
    icici_pwd = json.load(open('config.json', 'r'))['ICICI_PWD']
    username.send_keys(icici_uname)
    password.send_keys(icici_pwd)
    
    driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div/div[2]/div/div[4]/div/input').click()
    driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div/div[2]/div/div[5]/input').click()
    
    tm.sleep(10)
    totp = TOTP(json.load(open('config.json', 'r'))['ICICI_GOOGLE_AUTHENTICATOR'])
    token = totp.now()
    
    t1 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[1]/input')
    t2 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[2]/input')
    t3 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[3]/input')
    t4 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[4]/input')
    t5 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[5]/input')
    t6 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[6]/input')
    
    t1.send_keys(token[0])
    t2.send_keys(token[1])
    t3.send_keys(token[2])
    t4.send_keys(token[3])
    t5.send_keys(token[4])
    t6.send_keys(token[5])
    
    driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[4]/input[1]').click()
    
    tm.sleep(10)
    
    session_id = driver.current_url.split('apisession=')[1]
    json_data = json.load(open('config.json', 'r'))
    json_data['ICICI_API_SESSION'] = session_id
    with open('config.json', 'w') as the_file:
        json.dump(json_data, the_file, indent=4)
    driver.quit()
    
    return session_id
    
print(ic_autologon())