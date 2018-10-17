from mongo_connect import connectMongo
import constants
import pymongo
import json
import pprint


collection = connectMongo()

##### FIND ALL ENTRIES IN THE DATABASE #####
# Assuming RQ0 is the query to find all entries in the database
RQ0 = collection.find()
for data in RQ0:
	pprint.pprint(data)
	
# WQ1: Adding documents from dummy-fitness.json file
file = open("dummy-fitness.json","r")
dummy_documents_list = json.loads(file.read())

collection.insert_many(dummy_documents_list)

# WQ2: Updating user 1001 with contents of user1001-new.json
file = open("user1001-new.json","r")
user1001_doc = json.loads(file.read())	
	
collection.update_one({'uid':user1001_doc['uid']}, {"$set":user1001_doc}, upsert=False)

######## FIND ENTRIES WITH CONDITION #######
######## collection.find(CONDITION) #######
######## E.g., collection.find({"Name" : "Alice"}) #######

######## UPDATE ENTRIES WITH CONDITION ########
######## collection.update_one(CONDITION, _update_) #######
######## collection.update_many(CONDITION, _update_)
######## E.g., collection.find({"Name" : "Alice"}, {"$inc" : {"age" : 1} })

######## DELETE ENTRIES WITH CONDITION ########
######## collection.delete_one(CONDITION) #######
######## collection.delete_many(CONDITION)
######## E.g., collection.find({"Name" : "Alice"})

######## AGGREGATE ENTRIES WITH PIPELINE ########
######## collection.aggregate(PIPELINE) ########

