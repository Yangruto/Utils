import os
import json
import requests
import datetime
import configparser

# class TeamsWebhookException(Exception):
#     pass

class TEAMSBOT:
    def __init__(self, config_name:str):
        dir_path = os.path.dirname(os.path.abspath(__file__))
        self.config = configparser.ConfigParser()
        self.config.read(dir_path + '/Teamsbot_config.ini')
        self.success_key = self.config[config_name]['Success_Key']
        self.failed_key = self.config[config_name]['Failed_Key']
        self.title = self.config[config_name]['Title']

    def set_card(self, content:list):
        self.json_card = {
            "@type": "MessageCard",
            "@context": "http://schema.org/extensions",
            "themeColor": "0076D7",
            "title": self.title,
            "summary": self.title,
            "sections": [{
            "facts": content,
            "markdown": False
        }]}

    def success_message(self):
        r = requests.post(self.success_key, json=self.json_card)
        if r.status_code == 200:
            print('Send Message Success')
        else:
            raise Exception(r.reason)
    
    def failed_message(self):
        r = requests.post(self.failed_key, json=self.json_card)
        if r.status_code == 200:
            print('Send Message Success')
        else:
            raise Exception(r.reason)

# How to use
"""
from Teamsbot import TEAMSBOT

# bot content
content = []
execute_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y/%m/%d %H:%M:%S') 
content.append({"name": "Execute Time:", "value": f"{execute_time}"})
content.append({"name": "Message:", "value": "Hi, this is frank testing."})

# call bot
bot = TEAMSBOT(BOT_CONFIG)
bot.set_card(content)
bot.success_message() or bot.failed_message()

"""