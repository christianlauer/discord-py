import pandas
import requests
from apscheduler.schedulers.blocking import BlockingScheduler

TOKEN = '5148599763:AAHgVsnPUFRXGvVP4fcrL5PqHe80HSuyQDc'
CHAT_ID = '1386463297'

def send_msg(text):
    url_req = "https://api.telegram.org/bot" + TOKEN + "/sendMessage" + "?chat_id=" + CHAT_ID + "&text=" + text + "&parse_mode=HTML"
    try:
        results = requests.get(url_req)
    except requests.exceptions.RequestException as e:
        raise SystemExit(e)


sched = BlockingScheduler()

@sched.scheduled_job('interval', minutes=60)
def timed_job():
    
    history = pandas.read_csv('history.csv')

    for index, row in history.iterrows():
        transactions_url = 'https://blockchain.info/rawaddr/' + row.address
        df = pandas.read_json(transactions_url)
        transactions = df['txs']
        last_time = transactions[0]['time']
        last_amount = transactions[0]['result']

        if last_time != row.time:
            history.iat[index, 1] = last_time
            history.iat[index, 2] = last_amount

            if int(last_amount) > 0:
                direction = "accumulating"
            elif int(last_amount) < 0:
                direction = "dumping"

            btc_amount = int((float(abs(last_amount)) / 100000000))
            print(f'{row.address} is {direction} {btc_amount} BTC')
            send_msg(f'Whale Alert: {row.address} is {direction} {btc_amount} BTC')

    history.to_csv('history.csv', index=False)

sched.start()