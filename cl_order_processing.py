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
