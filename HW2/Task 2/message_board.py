import pymongo
import constants
from mongo_connect import connectMongo
import json
import pprint
import redis

rds = redis.Redis()
collection = connectMongo()

topic = ''
sub = False

while True:

	if sub:
		print("subscribing..")
		while sub:	
			try:
				msg = pub.get_message()
			except:
				sub = False
				pub.unsubscribe(topic)
			if msg:
				print('Message received: ',msg)
		
	cmd = raw_input('Enter command: ')
	cmds = cmd.split(' ')
	if cmds[0] == "select":
		topic = cmds[1]
	elif cmds[0] == "listen":
		if topic == '':
			print "ERROR! No board selected"
		else:
			sub = True;
			pub = rds.pubsub()
			result = pub.subscribe([topic]) 
			print (result)
	elif cmds[0] == "stop":
		print "ERROR! Not listening"
	elif cmds[0] == "read":
		if topic == '':
			print "ERROR! No board selected"
		else:
			results = collection.find({ "boardName": topic })
			for item in results:
				pprint.pprint(item["message"])
	elif cmds[0] == "write":
		if topic == '':
			print "ERROR! No board selected"
		else:
			msg = ' '.join(cmds[1:])
			result = rds.publish(topic, msg) 
			print(result)
			collection.insert({"boardName": topic,"message": msg})
	else:
		print "Invalid input" ;
