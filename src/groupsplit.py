import os
import re
import csv
import sys
import json
import pickle
import pprint
import urllib
import hashlib
import logging
import optparse
import requests
import subprocess
import webbrowser
import oauthlib.oauth1
from money import Money
from pprint import pprint
from datetime import datetime
from tabulate import tabulate

LOGGING_DISABELED = 100
log_levels = [LOGGING_DISABELED, logging.CRITICAL, logging.ERROR,
              logging.WARNING, logging.INFO, logging.DEBUG]

logger = logging.getLogger(__name__)
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)
logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p')

def split(total, num_people):
    base = total * 100 // num_people / 100
    extra = total - num_people * base
    return base, extra

def do_hash(msg):
    m = hashlib.md5()
    m.update(msg.encode('utf-8'))
    return m.hexdigest()

class Splitwise:
    def __init__(self, api_client='oauth_client.pkl'):
        if os.path.isfile(api_client):
            with open(api_client, 'rb') as oauth_pkl:
                self.client = pickle.load(oauth_pkl)
        else:
            self.get_client()

    def get_client_auth(self):
        if os.path.isfile("consumer_oauth.json"):
            with open("consumer_oauth.json", 'rb') as oauth_file:
                consumer = json.load(oauth_file)
                self.ckey = consumer['consumer_key']
                self.csecret = consumer['consumer_secret']
        else:
            exit("Please ensure consumer_oauth.json exists with your keys.")

    def get_client(self):
        self.get_client_auth()
        client = oauthlib.oauth1.Client(self.ckey, client_secret=self.csecret)
        uri, headers, body = client.sign("https://secure.splitwise.com/api/v3.0/get_request_token", http_method='POST')
        r = requests.post(uri, headers=headers, data=body)
        resp = r.text.split('&')
        oauth_token = resp[0].split('=')[1]
        oauth_secret = resp[1].split('=')[1]
        uri = "https://secure.splitwise.com/authorize?oauth_token=%s" % oauth_token
        webbrowser.open_new(uri)
        verifier_input = input('Copy the oauth verifier from the browser: ')
        client = oauthlib.oauth1.Client(self.ckey, client_secret=self.csecret, resource_owner_key=oauth_token, resource_owner_secret=oauth_secret, verifier=verifier_input)
        uri, headers, body = client.sign("https://secure.splitwise.com/api/v3.0/get_access_token", http_method='POST')
        resp = requests.post(uri, headers=headers, data=body)
        tokens = resp.text.split('&')
        oauth_token = tokens[0].split('=')[1]
        oauth_secret = tokens[1].split('=')[1]
        client = oauthlib.oauth1.Client(self.ckey, client_secret=self.csecret, resource_owner_key=oauth_token, resource_owner_secret=oauth_secret, verifier=verifier_input)
        with open('oauth_client.pkl', 'wb') as pkl:
            pickle.dump(client, pkl)
        self.client = client

    def api_call(self, url, http_method):
        uri, headers, body = self.client.sign(url, http_method=http_method)
        resp = requests.request(http_method, uri, headers=headers, data=body)
        return resp.json()

    def get_id(self):
        if not hasattr(self, "my_id"):
            resp = self.api_call("https://secure.splitwise.com/api/v3.0/get_current_user", 'GET')
            self.my_id = resp['user']['id']
        return self.my_id

    def get_groups(self):
        resp = self.api_call("https://secure.splitwise.com/api/v3.0/get_groups", 'GET')
        return resp['groups']

    def post_expense(self, uri):
        resp = self.api_call(uri, 'POST')
        if resp.get("errors"):
            print(f"Error: {resp['errors']}")
        else:
            sys.stdout.write(".")
            sys.stdout.flush()

class CsvSettings():
    def __init__(self, rows):
        print("First two rows of CSV:")
        print("\n".join([str(t) for t in rows[0:2]]))
        self.date_col = input("Date column index? (0 for date): ")
        self.amount_col = input("Amount column index? (2 for Amount): ")
        self.desc_col = input("Desc column index? (1 for item): ")
        self.has_title_row = input("Titles in first row? [Y/n]").lower() != 'n'
        self.local_currency = input("Currency? (e.g. AED): ").upper()
        self.remember = input("Remember settings? [Y/n]").lower() != 'n'

    def __del__(self):
        if hasattr(self, 'remember') and self.remember:
            with open("csv_settings.pkl", "wb") as pkl:
                pickle.dump(self, pkl)

class SplitGenerator():
    def __init__(self, options, args, api):
        self.api, self.options, self.args = api, options, args
        with open(args[0], 'r') as csvfile:
            reader = csv.reader(csvfile)
            self.rows = [x for x in reader]

        if os.path.isfile(options.csv_settings):
            with open(options.csv_settings, 'rb') as f:
                self.csv = pickle.load(f)
        else:
            self.csv = CsvSettings(self.rows)

        if self.csv.has_title_row:
            self.header = self.rows[0]
            self.rows = self.rows[1:]

        self.make_transactions()
        self.get_group(args[1])
        self.splits = []
        self.ask_for_splits()

    def make_transactions(self):
        # UPDATED: Matches the DD/MM/YYYY format in your allconsol.csv
        csvDateFormat = "%d/%m/%Y"
        self.transactions = []
        for i, r in enumerate(self.rows):
            if r[int(self.csv.amount_col)] and float(r[int(self.csv.amount_col)]) > 0:
                self.transactions.append({
                    "date": datetime.strptime(r[int(self.csv.date_col)], csvDateFormat).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "amount": Money(r[int(self.csv.amount_col)], self.csv.local_currency),
                    "desc": re.sub(r'\s+', ' ', r[int(self.csv.desc_col)]),
                    "original_row_index": i
                })

    def get_group(self, name):
        groups = self.api.get_groups()
        for group in groups:
            if group['name'].lower() == name.lower():
                self.gid = group['id']
                return
        exit(f"Group '{name}' not found.")

    def ask_for_splits(self):
        for i, t in enumerate(self.transactions):
            if self.options.yes or input(f"{i}: {t['date']} {t['desc']} {t['amount']}. Split? [y/N] ").lower() == 'y':
                self.splits.append(t)
        print(tabulate(self.splits, headers="keys"))
        assert self.options.yes or input("Confirm? [y/N] ").lower() == 'y', "Canceled."

    def __getitem__(self, index):
        s = self.splits[index]
        one_cent = Money("0.01", self.csv.local_currency)
        
        # MAP COLUMN INDEXES TO IDS FROM YOUR TERMINAL OUTPUT
        ID_MAP = {
            4: 123456,  # Replace with Nikhil's ID
            5: 789012,  # Replace with Rose's ID
            6: 345678   # Replace with Austin's ID
        }
        
        active_members = []
        row = self.rows[s["original_row_index"]]
        for col, sw_id in ID_MAP.items():
            if row[col].strip().lower() == 'yes':
                active_members.append(sw_id)

        num_people = len(active_members) + 1
        base, extra = split(s['amount'], num_people)
        
        params = {
            "payment": 'false',
            "cost": s["amount"].amount,
            "description": s["desc"],
            "date": s["date"],
            "group_id": self.gid,
            "currency_code": self.csv.local_currency,
            "users__0__user_id": self.api.get_id(),
            "users__0__paid_share": s["amount"].amount,
            "users__0__owed_share": base.amount,
        }
        for i, mid in enumerate(active_members):
            params[f'users__{i+1}__user_id'] = mid
            params[f'users__{i+1}__paid_share'] = 0
            params[f'users__{i+1}__owed_share'] = (base + one_cent).amount if extra.amount > 0 else base.amount
            extra -= one_cent
            
        return f"https://secure.splitwise.com/api/v3.0/create_expense?{urllib.parse.urlencode(params)}"

def main():
    usage = "groupsplit.py [options] <csv> <group>"
    parser = optparse.OptionParser(usage=usage)
    parser.add_option('-y', default=False, action='store_true', dest='yes')
    parser.add_option('-d', '--dryrun', default=False, action='store_true', dest='dryrun')
    parser.add_option('--csv-settings', default='csv_settings.pkl', dest='csv_settings')
    parser.add_option('--api-client', default='oauth_client.pkl', dest='api_client')
    options, args = parser.parse_args()
    
    sw = Splitwise(options.api_client)
    
    # PRINT IDs TO TERMINAL SO YOU CAN UPDATE ID_MAP ABOVE
    groups = sw.get_groups()
    for g in groups:
        if g['name'].lower() == args[1].lower():
            print(f"\n--- Members of {g['name']} ---")
            for m in g['members']:
                print(f"{m['first_name']}: {m['id']}")
            print("---------------------------\n")

    gen = SplitGenerator(options, args, sw)
    for uri in gen:
        if options.dryrun:
            print(uri)
        else:
            sw.post_expense(uri)

if __name__ == "__main__":
    main()
