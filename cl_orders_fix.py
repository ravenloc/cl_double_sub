#!/usr/bin/python
# -*- coding: utf-8 -*-
    
import re
import psycopg2
import psycopg2.extras
import csv
import sys
import hashlib
import xmlrpclib
from time import sleep

reload(sys)
sys.setdefaultencoding("utf-8")

def connectDatabase():

    global conn
    global cur
    global pbaAPI
    config=file("/usr/local/bm/etc/ssm.conf.d/global.conf")
    DBConf = {}
    HOSTConf = {}
    for conf_line in config:    
        if 'DB_' in conf_line:
            conf_line=conf_line.rstrip()
            DBConf[conf_line.split(' = ')[0]]=conf_line.split(' = ')[1]
        if 'HOST_IP' in conf_line:
            conf_line=conf_line.rstrip()
            HOSTConf[conf_line.split(' = ')[0]]=conf_line.split(' = ')[1]
            pbaAPI = "http://%s:5224/RPC2" % HOSTConf['HOST_IP']
    try:    
        conn=psycopg2.connect(database=DBConf["DB_NAME"], user=DBConf["DB_USER"], host=DBConf["DB_HOST"],password=DBConf["DB_PASSWD"])
    except:
        print "Cannot connect to database"
        sys.exit(1)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

def InitScript():

    global poaAPI
    global poa
    global pba
    
    connectDatabase()
    cur.execute("""select "PEMAddress","PEMPort" from "PEMOptions" """)
    data = cur.fetchall()
    if len(data) > 0:
        for record in data:
            record['PEMAddress'] = ''.join( record['PEMAddress'].split()) #Removing whitespaces
            record['PEMPort'] = str(record['PEMPort'])
            poaAPI = "http://%s:%s" % (record['PEMAddress'],record['PEMPort'])
    else:
        print "POA API relation was not found"
        sys.exit(1)
    poa = xmlrpclib.ServerProxy(poaAPI)
    pba = xmlrpclib.ServerProxy(pbaAPI)


def CleanUp():
    cur.close()
    conn.close()

    
def get_order_status(order_id):
    try:
        response = pba.Execute({'methodName':'Execute','Server':'BM','Method':'GetOrder_API','Params':[order_id]})
        #[[id, number, var, cx, status, type, cr_time, or_date, total, tax_ttl, dsc_ttl, merch_ttl, comment, exp_date, promo, sales_br, sales_pr, currency, completed_date]]
        return(response['Result'][0][4])
    except xmlrpclib.Fault as e:
        print "Failed to gather order #%s status: " % order_id
        print str(e)
        sys.exit(1)


def orderSignature(order_id):
    try:
        response = pba.Execute({'methodName':'Execute','Server':'BM','Method':'GetOrder_API','Params':[order_id]})
    except:
        print "Failed to gather order parameters"
        sys.exit(1)
    OrderID = str(response['Result'][0][0])
    #print OrderID
    OrderNumber = str(response['Result'][0][1])
    #print OrderNumber
    CreationTime = str(response['Result'][0][6])
    #print CreationTime
    OrderTotal = str(response['Result'][0][8])
    #print OrderTotal
    Descr = str(response['Result'][0][12])
    #print Descr
    Curr = response['Result'][0][-2]
    #print Curr
    try:
        settings_file = open('/usr/local/stellart/share/currencies.txt','r')
        for line in settings_file:
            if re.match(Curr, line):
                precision = int(line.split()[2])
    except:
        precision = 2
    if re.match(r'\d{,10}\.\d{1}$', OrderTotal) is not None:
        OrderTotal += '0' * (precision - 1)
    else:
        order_total_regex = '\d{,10}\.?\d{,%s}' % precision
        OrderTotal = re.search(order_total_regex, OrderTotal).group()
    # Concatenate signature parts
    signature_part1 = ''.join([OrderID,OrderNumber,CreationTime,Curr])
    signature_part2 = ''.join([OrderTotal,Descr])
    # Truncate space at the end
    signature = ' '.join([signature_part1,signature_part2]).rstrip()
    # Generate md5sum
    sigres = hashlib.md5(signature.encode('utf-8')).hexdigest()
    return sigres
    
def OrderStatusChange(order_id, order_signature, status = None):
    if status is None:
        status = 'PD'        
    response = pba.Execute({'methodName':'Execute','Server':'BM','Method':'OrderStatusChange_API','Params':[order_id, status, order_signature]})
    return response

def getSubscription(subscription_id, get_resources = None):
    params = {
        'subscription_id':int(subscription_id),
    }
    if get_resources:
        params['get_resources'] = get_resources
    else:
        params['get_resources'] = False
    res = poa.pem.getSubscription(params)
    return res
    
def createSub(acc_id,st_id,sub_id = None):
    params = {
        'account_id':int(acc_id),
        'service_template_id':int(st_id),
    }
    if sub_id:
        params['subscription_id'] = int(sub_id)
    res = poa.pem.addSubscription(params)
    if res['status'] == 0:
        return res
    else:
        print "Subscription creation API failed: %s" % res['error_message']
        sys.exit(1)
    return res
    

def restartOrder(order_id, target_status = None):
    if target_status is None:
        status = 'PD'
    else:
        status = target_status
    try:
        sign = orderSignature(order_id)
    except xmlrpclib.Fault as e:
        print "Failed to get signature:"
        print str(e)
        return -1
    try:
        res = OrderStatusChange(order_id,sign,status)
        return res
    except xmlrpclib.Fault as e:
        print str(e)
    return -1

def triggerEvent(ekid,oiid,sid,message = None):
    if message is None:
        msg = 'EventProcessing'
    else:
        msg = message
    if ekid == "Creation Completed":
        params = "OrderItemID=%s; SubscrID=%s; IssuedSuccessfully=1; Message=%s" % (oiid,sid,msg)
    elif ekid == "Deletion Completed":
        params = "OrderItemID=%s; IssuedSuccessfully=1; Message=%s." % (oiid,message)
    else:
        print "Not supported EKID"
        return -1
    res = pba.Execute({'methodName':'Execute','Server':'TASKMAN','Method':'PostEvent','Params':[ekid,params,0]})
    return res
    
def removeSubscription(sub_id):
    res = poa.pem.removeSubscription({'subscription_id':int(sub_id)})
    if res['status'] == 0:
        return res
    return -1
        
def complete_oiid_with_trigger(sub_id, oiid, aid, st_id):
    try:
        createSub(aid, st_id, sub_id)
    except:
        print "Could not create subscription for completion order item %s by event" % oiid
        return -1
    try:
        triggerEvent('Deletion Completed', oiid, sub_id, 'complete_oiid_with_trigger')
    except:
        print "Could not trigger event for completion order item %s by event" % oiid
        return -1
    try:
        removeSubscription(sub_id)
    except:
        print "Could not remove subscription %s after triggering event" % sub_id

def order_followup(order_id, target_status = None):
    sleep_time = 5
    cur.execute("""select s."Status", s."ServStatus", "subscriptionID", "OrderDocOrderID","serviceTemplateID","AccountID","OIID" from "OItem" oi join "Subscription" s using ("subscriptionID") where "OrderDocOrderID" = %s""",[order_id])
    data = cur.fetchall()
    if len(data) > 0:
        for record in data: 
            #record['subscriptionID']
            #record['OrderDocOrderID']
            #record['OIID']
            #record['Status']
            #record['ServStatus']
            #record['serviceTemplateID']
            #record['AccountID']
            if record['ServStatus'] == 70 or record['ServStatus'] == 60:
                cur.execute("""update "Subscription" set "ServStatus" = 30 where "subscriptionID" = %s """, [record['subscriptionID']])
                if cur.rowcount == 1:
                    print "Changing BA serv status of subscription %s to stopped in DB to resubmit order" % record['subscriptionID']
                    conn.commit()
                else:
                    print "Incorrect number of rows updated during fixing subscription status"
                    conn.rollback()
                    CleanUp()
                    sys.exit(1)
            if restartOrder(record['OrderDocOrderID'], target_status) == -1:
                print "Failed to restart order"
                CleanUp()
                exit(1)
            print "Order %s was restarted" % (record['OrderDocOrderID'])
            sleep(sleep_time)
            order_status = get_order_status(record['OrderDocOrderID'])
            subscr_check = getSubscription(record['subscriptionID'])
            if order_status == 'CP':
                print "Order %s is completed" % (record['OrderDocOrderID'])
                print "______________"
                return 0
            elif order_status == 'PF':
                print "Order %s failed, will be checked with failed orders." % (record['OrderDocOrderID'])
                print "______________"
                return 0
            elif subscr_check['status'] != 0 and "does not exist" in subscr_check['error_message'] and order_status == 'PR':
                print "Order was not completed. Will try to trigger event manually."
                complete_oiid_with_trigger(record['subscriptionID'], record['OIID'], record['AccountID'], record['serviceTemplateID'])
                print "Event triggered. Subscription removed from POA."
                print "______________"
                return 0
            else:
                if target_status == 'PD' and order_status == 'RB':
                    print "Order %s dropped to CPC status will be checked in next block" % record['OrderDocOrderID']
                    return 0
                print "Status %s is unexpected for order %s under account %s or subscription was not removed in OA please chek manually" % (order_status, record['OrderDocOrderID'], record['AccountID'])
                return -1


def process_cnbs_orders():
    print "Checking failed 'can not be stopped'/'Service_timeout' CL Orders"
    print "______________"
    cur.execute("""select distinct("subscriptionID"), "OrderDocOrderID", "OIID", s."Status", s."ServStatus", "ProcessingComment" from "OItem" io join "Subscription" s using("subscriptionID") where "OrderDocOrderID" in (select "OrderID" from "SalesOrder" where "OrderTypeID" = 'CF' and "OrderStatusID" in ('PF')) and ("ProcessingComment" ~ 'Stopping service of Order Item' or "ProcessingComment"  ~ 'Service Creation Timeout Exceeded')  and s."Status" != 60 and s."ServStatus" != 90""")
    data = cur.fetchall()
    if len(data) > 0:
        for record in data:
            #record['subscriptionID']
            #record['OrderDocOrderID']
            #record['OIID']
            #record['Status']
            #record['ServStatus']
            subscription = getSubscription(record['subscriptionID'])
            if subscription['status'] == 0:
                print "Order %s could not stop service of subscription %s will try to remove manually then resubmit and process the order." % (record['OrderDocOrderID'], record['subscriptionID'])
#                if raw_input("Go?") != '1':
#                    CleanUp()
#                    sys.exit(1)
                print "Removing Subscription %s..." % record['subscriptionID']
                try:
                    removeSubscription(record['subscriptionID'])
                except xmlrpclib.Fault as e:
                    print "Failed to remove subscription:"
                    print str(e)
                    print "Skipping Order"
                    print "______________"
                    continue
                print "Subscription %s successfully removed from OA" % record['subscriptionID']
                order_followup(record['OrderDocOrderID'],'PD')
            else:
                print "Order %s is failed despite subscription %s does not exist in OA will try resubmit and process it" % (record['OrderDocOrderID'], record['subscriptionID'])
                order_followup(record['OrderDocOrderID'], 'PD')


def ProcessRBOrders():
    print "Checking CPC CL Orders"
    print "______________"
    cur.execute("""select distinct("subscriptionID"), "OrderDocOrderID", "OIID", s."Status", s."ServStatus", s."AccountID", s."serviceTemplateID" from "OItem" io join "Subscription" s using("subscriptionID") where "OrderDocOrderID" in (select "OrderID" from "SalesOrder" where "OrderTypeID" = 'CF' and "OrderStatusID" in ('RB')) and s."Status" != 60 and s."ServStatus" != 90 """)
    data = cur.fetchall()
    if len(data) > 0:
        for record in data:
            #record['subscriptionID']
            #record['OrderDocOrderID']
            #record['OIID']
            #record['Status']
            #record['ServStatus']
            #record['AccountID']
            #record['serviceTemplateID']
            subscription = getSubscription(record['subscriptionID'])
            if subscription['status'] != 0 and "does not exist" in subscription['error_message']:
                print "Found order %s in checking provisioning coditions without subscription in OA when it is supposed to be." % (record['OrderDocOrderID'])
                print "Adding subscription %s to operations for Order %s" % (record['subscriptionID'],record['OrderDocOrderID'])
                try:
                    print "Creating subscription with parameters %s %s %s" % (record['AccountID'], record['serviceTemplateID'], record['subscriptionID'])
                    createSub(record['AccountID'], record['serviceTemplateID'], record['subscriptionID'])
                except xmlrpclib.Fault as e:
                    print str(e)
                    CleanUp()
                    exit(1)
                print "Restarting Order %s" % (record['OrderDocOrderID'])
                order_followup(record['OrderDocOrderID'], 'I4')


def process_sdne_orders():
    print "Checking failed 'subscription does not exist' CL Orders"
    print "______________"



def main(order_id = None):
    InitScript()
    process_cnbs_orders()
    ProcessRBOrders()
    process_sdne_orders()
    CleanUp()
    
if __name__ == "__main__":
    main()