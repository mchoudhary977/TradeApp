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
    # payload = {"messaging_product": "whatsapp", "to": "919673843177", "type": "template", "template": { "name": "hello_world", "language": { "code": "en_US" } } }


    payload = {
        "messaging_product":"whatsapp",
        "recipient_type":"individual",
        "to":"919673843177",
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
    if response.status_code == 200:
        # return render_template('index.html',alert_msg = 'WhatsApp Alert Trigerred')
        return {'status':'SUCCESS','msg':response.json()}
    else:
        # return render_template('index.html',alert_msg = response.text)
        return {'status':'ERROR','msg':response.text}