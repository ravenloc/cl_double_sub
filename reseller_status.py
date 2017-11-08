import sys
import psycopg2
import psycopg2.extras
import ConfigParser
import csv
from collections import namedtuple
from functools import partial
from time import ctime
from time import time


Config = namedtuple('Config', [
    'database',
    'user',
    'password',
    'db_host',
    'port',
    'host_ip'
])

def config_from_file(path):
    """
    Parses config file and returns Config instance
    :param path: full path to config file
    :type path: str
    :return: Config instance
    :rtype: config.Config
    """
    with open(path, 'r') as f:
        config = ConfigParser.ConfigParser()
        config.readfp(f)

        get_env_var = partial(config.get, 'environment')

        database = get_env_var('DB_NAME')
        user = get_env_var('DB_USER')
        password = get_env_var('DB_PASSWD')
        db_host = get_env_var('DB_HOST')
        port = get_env_var('DB_PORT')
        host_ip = get_env_var('HOST_IP')

        return Config(database, user, password, db_host, port, host_ip)

class DBConnection(object):

    def __init__(self, config):
        """
        Context manager managing connection to PostgreSQL
        :param config: parsed config file
        :type config: config.Config
        """
        self.database = config.database
        self.user = config.user
        self.password = config.password
        self.db_host = config.db_host
        self.port = config.port

    def __enter__(self):
        self.conn = psycopg2.connect(
            database=self.database,
            user=self.user,
            password=self.password,
            host=self.db_host,
            port=self.port
        )
        self.cursor = self.conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.conn.close()

    def exec_query(self, query):
        try:
            self.cursor.execute(query)
            return self.cursor.fetchall()
        except:
            return None


    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

def collect_data(db_conn):
    query = """select "AccountID", "VendorAccountID", "AStatus", "CompanyName", "DateArc" from "Account" where "Type" = 2  and "AStatus" in (10,11,12) and "VendorAccountID" <> 1"""
    arc_query = """select "AccountID", "AStatus", "CompanyName", "DateArc" from "Account" where "AccountID" = {} union select "AccountID", "AStatus", "CompanyName", "DateArc" from "AccountArc" where "AccountID" = {}  order by "DateArc" desc  """

    accounts = db_conn.exec_query(query)
    result_accounts = []
    for account in accounts:
        arc_accounts = db_conn.exec_query(arc_query.format(str(account['AccountID']) ,str(account['AccountID'])))
        i = 0
        while arc_accounts[i]['AStatus'] in (10,11,12):
            i += 1
        days_on_hold = time() - arc_accounts[i-1]['DateArc']
        days_on_hold = days_on_hold / 86400.0
        #days_on_hold = ctime(float(arc_accounts[i-1]['DateArc']))
        result_accounts.append((account['VendorAccountID'], account['AccountID'],account['CompanyName'],days_on_hold))
    return result_accounts

def list_to_csv(filename, headers, data):
    with open(filename, 'wb') as csv_file:
        wr = csv.writer(csv_file, delimiter = ',')
        wr.writerow(headers)
        for row in data:
            wr.writerow(row)


def main():
    config = config_from_file('/usr/local/bm/etc/ssm.conf.d/global.conf')
    outfile = 'account_days_on_hold.csv'
    with DBConnection(config) as db_conn:
        accounts = collect_data(db_conn)
        list_to_csv(outfile,['L1','L2','Company Name', 'Days on hold'],accounts)

if __name__ == "__main__":
    main()
