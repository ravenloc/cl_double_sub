#!/usr/bin/python
# -*- coding: utf-8 -*-

import re
import psycopg2
import psycopg2.extras
import csv
import sys
import hashlib
import xmlrpclib

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
			print pbaAPI
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

	
	
def orderSignature(order_id):
	try:
		response = pba.Execute({'methodName':'Execute','Server':'BM','Method':'GetOrder_API','Params':[order_id]})
	except:
		print "Failed to gather order parameters"
		sys.exit(0)
	print response['Result']
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
	
	if not status:
		status = 'PD'		
	response = pba.Execute({'methodName':'Execute','Server':'BM','Method':'OrderStatusChange_API','Params':{[order_id],order_signature,status}})
	return response

def getSubscription(subscription_id, get_resources = None):
	params = {
		'subscription_id':int(subscription_id),
	}
	if get_resources:
		params['get_resources'] = get_resources
	else:
		params['get_resources'] = false
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
        print "Subscriptions %s was created" % sub_id
    else:
        print "Subscription creation API failed: %s" % res['error_message']
		sys.exit(1)
	return res
	

def restartOrder(order_id, target_status = None):
	if target_status:
		status = order_status
	else
		status = 'PD'
	try:
		sign = orderSignature(order_id)
	except xmlrpclib.Fault as e:
		print str(e)
	try:
		res = OrderStatusChange(order_id,sign,status)
		return res
	except xmlrpclib.Fault as e:
		print str(e)
	return -1

def triggerEvent(ekid,oiid,sid,message = None):
	if message:
		msg = message
	else:
		msg = 'EventProcessing'
	if ekid == "Creation Completed":
		params = "OrderItemID=%s; SubscrID=%s; IssuedSuccessfully=1; Message=%s" % (oiid,sid,msg)
	elif ekid == "Deletion Completed":
		params = "OrderItemID=%s; IssuedSuccessfully=1; Message=%s." % (oiid,message)
	else:
		print "Not supported EKID"
		return -1
	res = pba.Execute({'methodName':'Execute','Server':'TASKMAN','Method':'PostEvent','Params':[EKID,Params,0]})
	return res
	
		

def ProcessRBOrders():

	print "Checking CPC CL Orders"
	cur.execute("""select distinct("subscriptionID"), "OrderDocOrderID",s."Status", s."ServStatus" from "OItem" io join "Subscription" s using("subscriptionID") where "OrderDocOrderID" in (select "OrderID" from "SalesOrder" where "OrderTypeID" = 'CF' and "OrderStatusID" in ('RB')) and s."Status" != 60""")
	status_data = cur.fetchall()
	if len(data) > 0:
		for record in data:
			#record['subscriptionID']
			#record['OrderDocOrderID']
			#record['Status']
			#record['ServStatus']
			subscription = getSubscription(record['subscriptionID'])
			if subscription['status'] != 0 and "does not exist" in subscription['error_message']:
				print "Adding subscription %s to operations for Order %s" % (record['subscriptionID'],record['OrderDocOrderID'])
				cur.execute("""select "serviceTemplateID","AccountID", "subscriptionID" from "Subscription" where "subscriptionID" = %s """, [record['subscriptionID']])
				sub_data = cur.fetchall()
				account = ''
				st = ''
				subid = ''
				if len(sub_data) > 0:
					for sub_params in sub_data:
						#sub_params['AccountID']
						#sub_params['serviceTemplateID']
						#sub_params['subscriptionID']
				else:
					print "Error retrieving subscription data"
					exit(1)
				try:
					createSub(sub_params[account, st, subid)
				except xmlrpclib.Fault as e:
					print str(e)
					exit(1)
				print "Restarting Order %s" % (record['OrderDocOrderID'])
				if restartOrder(record['OrderDocOrderID'],'I4') = -1:
					print "Failed to restart order"
					exit(1)
				print "Order was restarted"
				
				
			else:
				print "go further"
				sys.exit(1)

def main(sub_id):
	InitScript()
	ProcessRBOrders()
	CleanUp()
	
if __name__ == "__main__":
	main(sys.argv[1])







