from selenium import webdriver
from selenium.webdriver.common.by import By
from pyotp import TOTP
from breeze_connect import BreezeConnect
import time
import os
import json

# browser = webdriver.Chrome()

key_secret = json.load(open('config.json', 'r'))
# print(key_secret['DHAN_CLIENT_ID'])

icici_api = BreezeConnect(api_key=json.load(open('config.json', 'r'))['ICICI_API_KEY'])
icici_session_url = json.load(open('config.json', 'r'))['ICICI_SESSION_URL']

service = webdriver.chrome.service.Service('./chromedriver')
service.start()
driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div[1]')
# options = webdriver.ChromeOptions()
# options.add_argument()
# options.add_argument('--headless')
# print(options)
# print(type(options))
# options.to_capabilities()
# options = options.to_capabilities()
# print(options.capabilities)
driver = webdriver.Remote(service.service_url)
driver.get(icici_session_url)
driver.implicitly_wait(10)
username = driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div/div[2]/div/div[1]/input')
password = driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div/div[2]/div/div[3]/div/input')

username.send_keys('mchoudhary977')
password.send_keys('Jagdish@977')


driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div/div[2]/div/div[4]/div/input').click()
driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div/div[2]/div/div[5]/input').click()
print(driver.session_id)

guid = driver.current_window_handle
# driver.switch_to.frame("getotp")
# driver.switch_to.active_element
# driver.switch_to.parent_frame()
driver.switch_to.active_element
print(driver.switch_to.active_element.parent)
print(driver.session_id)
print(driver.window_handles)
print(driver.current_window_handle)
print(driver.find_element(By.ID, 'dvgetotp'))
# driver.switch_to.new_window()
# driver.switch_to.parent_frame()
# print("Switched to Frame : "+driver.switch_to.parent_frame())
# print('Page Source'+driver.page_source)


totp = TOTP('IZXWI5TDIFUHSNKBLJBGGWLBLA')
token = totp.now()

# t1 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[1]/input')
# t2 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[2]/input')
# t3 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[3]/input')
# t4 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[4]/input')
# t5 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[5]/input')
# t6 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[6]/input')
#
# t1.send_keys(token[0])
# t2.send_keys(token[1])
# t3.send_keys(token[2])
# t4.send_keys(token[3])
# t5.send_keys(token[4])
# t6.send_keys(token[5])


# request_token=driver.current_url.split('request_token=')[1][:32]
# new_url = driver.current_url
# print('Current Window Handle: '+driver.current_window_handle)
# print('All Window Handles:-')
# print(driver.window_handles)
# guid = driver.current_window_handle
# print(driver.switch_to)
# print(driver.switch_to_window(guid))
# print(icici_session_url)
# print(new_url)
# print(driver.get_cookie('OTP'))
# element = driver.find_element_by_xpath("//div[2]")
# print(element)
# driver.get(new_url)
# driver = driver.current_url
# /html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[1]/input
# t1 = driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[1]/input')
# t1 = driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[1]/input')
# t2 = driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[2]/input')
# t3 = driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[3]/input')
# t4 = driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[4]/input')
# t5 = driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[5]/input')
# t6 = driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[6]/input')
# print(driver.find_element(By.ID, 'txtuid'))

# tmp = driver.find_element(By.ID, 'pnlOTP')
# print(tmp)
# driver.get('https://api.icicidirect.com/apiuser/tradelogin/goforotp')
# print(driver.find_element(By.CLASS_NAME, 'form-control-2 h-auto lh-base text-center'))
# print('Page Print'+driver.print_page())
# print('Page Source'+driver.page_source)
# print(driver.get_log('client'))
# totp = TOTP('IZXWI5TDIFUHSNKBLJBGGWLBLA')
# token = totp.now()
# print(driver.get_credentials)
# print(driver.get_network_conditions())
# print(driver.get_screenshot_as_base64())
# driver.switch_to(driver.get_screenshot_as_base64())
# print(driver.switch_to.active_element.get_attribute("title"))
# driver.switch_to.parent_frame()
# print('Page Source'+driver.page_source)
# print(driver.get_window_position('current'))
# print(driver.get_window_rect())
print(token)
# print(token[0])
# print(token[1])
# print(token[2])
# print(token[3])
# print(token[4])
# print(token[5])

# t1.send_keys(token[0])
# t2.send_keys(token[1])
# t3.send_keys(token[2])
# t4.send_keys(token[3])
# t5.send_keys(token[4])
# t6.send_keys(token[5])

# driver.find_element(By.XPATH, '/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[4]/input[1]').click()


# pin = driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/input')




# /html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[1]/input
# /html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[3]/div/div[2]/input
#
# /html/body/form/div[2]/div/div/div[2]/div/div[2]/div[2]/div[4]/input[1]

# pin.send_keys(token)
# driver.find_element(By.XPATH,'/html/body/form/div[2]/div/div/div[2]/div/div[2]/button').click()
#
# driver.implicitly_wait(10)

# options = webdriver.ChromeOptions()

# options.add_argument('--headless')
# options = options.to_capabilities()
# driver = webdriver.Remote(service.service_url, options)

# driver.get(icici_session_url)

# icici_session_url = json.load(open('config.json', 'r'))['ICICI_SESSION_URL']
# print(icici_session_url)
# browser = webdriver.Firefox()
# browser.get('http://selenium.dev/')