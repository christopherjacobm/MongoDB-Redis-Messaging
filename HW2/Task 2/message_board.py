import pymongo
import constants
from mongo_connect import connectMongo
import json
import pprint
import redis

rds = redis.Redis()
collection = connectMongo()

topic = ''

while True:
	cmd = raw_input('Enter command: ')
	cmds = cmd.split(' ')
	if cmds[0] == "select":
		topic = cmds[1]
	elif cmds[0] == "listen":
		pass
	elif cmds[0] == "stop":
		pass
	elif cmds[0] == "read":
		results = collection.find({ "boardName": topic })
		for item in results:
			pprint.pprint(item["message"])
	elif cmds[0] == "write":
		msg = ' '.join(cmds[1:])
		result = rds.publish(topic, msg) 
		print(result)
		collection.insert({"boardName": topic,"message": msg})