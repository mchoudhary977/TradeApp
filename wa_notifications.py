import json 
import requests
import http.client

# WhatsApp Meesage Module
def send_whatsapp_msg(mtitle='TRADE-APP', mtext='Welcome to TradeApp!'):
    tkn = 'Bearer ' + json.load(open('config.json', 'r'))['WA_TKN']
    url = 'https://graph.facebook.com/v16.0/108228668943284/messages'
    headers = {
        'Authorization': tkn,
        'Content-Type': 'application/json'
    }
    # phone_list = ['919673843177','917721841919']
    phone_list = ['919673843177']
    # payload = {"messaging_product"1: "whatsapp", "to": "919673843177", "type": "template", "template": { "name": "hello_world", "language": { "code": "en_US" } } }
    
    for i in phone_list:
        payload = {
            "messaging_product":"whatsapp",
            "recipient_type":"individual",
            "to":i,
            "type":"template",
            "template":{
                "name":"app_msg",
                "language":{
                    "code":"en"
                    },
                "components":[
                    {
                        "type":"header",
                        "parameters":[
                            {
                                "type":"text",
                                "text":mtitle
                                }
                            ]
                        },
                    {
                        "type":"body",
                        "parameters":[
                            {
                                "type":"text",
                                "text":mtext
                                }
                            ]
                        }
                    ]
                }
            }
    
        # Send the POST request
        response = requests.post(url, headers=headers, json=payload)
    
        # Check the response
        if response.status_code != 200:
            return {'status':'ERROR','msg':f"{i} - {response.text}"}
    return {'status':'SUCCESS','msg':f"{response.json()}"}