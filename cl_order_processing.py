from __future__ import print_function
import re
import hashlib
import psycopg2
import psycopg2.extras
import ConfigParser
import sys
import xmlrpclib
from collections import namedtuple
from functools import partial

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

    def commit(self):
        self.conn.commit()

    def rollback(self):
        self.conn.rollback()

class Proxy(object):
    """
    Wrapper around xmlrpclib.ServerProxy with platform API methods
    """
    def __init__(self, server_proxy):
        """
        :param server_proxy: XMLRPC server proxy
        :type server_proxy: xmlrpclib.ServerProxy
        """
        self.proxy = server_proxy

class BAapi(Proxy):
    """
    Class processing BA API methods
    """
    def api_call(self, server, method, params):
        """
        Wrapper for api execution.
        :param server: Method server
        :type server: str
        :param method: PBA API Method
        :type method: str
        :param params: Array of parameters
        :type params: list
        :return: response
        :rtype: dict
        """
        try:
            response = self.proxy.Execute({
                'methodName': 'Execute',
                'Server': server,
                'Method': method,
                'Params': params
            })
        except xmlrpclib.Fault as e:
            #
            #LOGGER
            print e
            #
            response = False

        return response

    def get_order_signature(self, order_id):
        """
        Makes order signature for given order_id
        param order_id: Order ID
        :type order_id: int, long
        :return: signature
        :rtype: str
        """
        response = self.api_call('BM','GetOrder_API',[order_id])
        if not response:
            #
            #LOGGER
            #
            return False

        result = response['Result']
        order_id = str(result[0][0])
        order_number = str(result[0][1])
        creation_time = str(result[0][6])
        order_total = str(result[0][8])
        description = str(result[0][12])
        currency = result[0][-2]
        precision = 2

        try:
            with open('/usr/local/stellart/share/currencies.txt', 'r') as settings_file:
                for line in settings_file:
                    if re.match(currency, line):
                        precision = int(line.split()[2])
        except (IOError, ValueError):
            pass

        if re.match(r'\d{,10}\.\d{1}$', order_total) is not None:
            order_total += '0' * (precision - 1)
        else:
            order_total_regex = '\d{,10}\.?\d{,%s}' % precision
            order_total = re.search(order_total_regex, order_total).group()

        # Concatenate signature parts
        signature_part1 = ''.join([order_id, order_number, creation_time, currency])
        signature_part2 = ''.join([order_total, description])

        # Truncate space at the end
        signature = ' '.join([signature_part1, signature_part2]).rstrip()

        # Generate md5sum
        sigres = hashlib.md5(signature.encode('utf-8')).hexdigest()
        return sigres

    def get_order_status(self, order_id):
        order_id = int(order_id)
        response = self.api_call('BM','GetOrder_API',[order_id])
        if response['Result'][0][4]:
            return response['Result'][0][4]
        else:
            #
            # LOGGER
            #
            return False

    def restart_order(self, order_id, target_status='PD'):
        signature = self.get_order_signature(order_id)
        if signature:
            return self.api_call('BM', 'OrderStatusChange_API', [order_id, signature, target_status])
        return False

    def trigger_event(self, ekid, oiid, sid, message='EventProcessing'):

        params_map = {
            'Creation Completed': 'OrderItemID={}; SubscrID={}; IssuedSuccessfully=1; Message={}'.format(oiid, sid, message),
            'Deletion Completed': 'OrderItemID={}; IssuedSuccessfully=1; Message={}.'.format(oiid, message)
        }

        params = params_map.get(ekid)

        if not params:
            #
            #LOGGER
            #
            return False
        return self.api_call('TASKMAN','PostEvent',[ekid, params, 0])

class OAapi(Proxy):
    """
    Class processing OA API requests
    """
    def add_sub(self, acc_id, st_id, sub_id=None):
        """
        Adding subscription on specified account within given service template without provisioning.
        :param acc_id: Account subscription belongs to
        :param st_id: Service template to use for creation
        :param sub_id: Optional. Desired ID for subscription.
        :return: dict
        """
        params = {
            'account_id': int(acc_id),
            'service_template_id': int(st_id),
        }
        if sub_id:
            params['subscription_id'] = int(sub_id)

        response = self.pem.addSubscription(params)

        if response['status'] != 0:
            #print("Subscription creation API failed: {}".format(res['error_message']), file=sys.stderr)
            #LOGGER
            #
            return None
        return response

    def get_sub(self, subscription_id):
        params = {
            'subscription_id': int(subscription_id),
            'get_resources': False
        }
        response = self.pem.getSubscription(params)
        return response

    def rm_sub(self, sub_id):
        """
        Removing subscription from OA
        :param sub_id: subscription ID
        :type sub_id: int, long
        :return: dict
        """
        res = self.pem.removeSubscription({'subscription_id': int(sub_id)})

        if res['status'] == 0:
            return res


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

def get_poa_api_address(cursor):
    query = 'SELECT "PEMAddress", "PEMPort" FROM "PEMOptions"'
    cursor.execute(query)
    result = None

    for record in cursor.fetchall():
        ip_addr = ''.join(record['PEMAddress'].split())
        port = str(record['PEMPort'])
        result = 'http://{}:{}'.format(ip_addr, port)
    return result
