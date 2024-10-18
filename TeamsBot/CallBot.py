import datetime
from Teamsbot import TEAMSBOT

if __name__ == "__main__":
    # bot content
    content = []
    execute_time = datetime.datetime.strftime(datetime.datetime.now(), '%Y/%m/%d %H:%M:%S') 
    content.append({"name": "Execute Time:", "value": f"{execute_time}"})
    content.append({"name": "Message:", "value": "Hi, this is frank testing."})

    # call bot
    bot = TEAMSBOT('BOT_CONFIG')
    bot.set_card(content)
    bot.success_message()