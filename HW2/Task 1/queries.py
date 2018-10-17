from mongo_connect import connectMongo
import constants
import pymongo
import json
import pprint


collection = connectMongo()

##### FIND ALL ENTRIES IN THE DATABASE #####
# Assuming RQ0 is the query to find all entries in the database
#RQ0 = collection.find()
#for data in RQ0:
#	pprint.pprint(data)
	
# WQ1: Adding documents from dummy-fitness.json file
file = open("dummy-fitness.json","r")
dummy_documents_list = json.loads(file.read())

collection.insert_many(dummy_documents_list)

# WQ2: Updating user 1001 with contents of user1001-new.json
file = open("user1001-new.json","r")
user1001_doc = json.loads(file.read())	
	
collection.update_one({'uid':user1001_doc['uid']}, {"$set":user1001_doc}, upsert=False)

# RQ1: Getting the count of employees
print
print "----- RQ1 - Count: -----"
print collection.count_documents({})
print "------------------------"
print

# RQ2: Retrieving employees who have been tagged as 'irregular'
RQ2 = collection.find({'tags':'irregular'})
print 
print "----- RQ2 - Irregular employees: -----"
print
for data in RQ2:
	pprint.pprint(data)
	print
print "--------------------------------------"
print

# RQ3: Retrieving employees that have a goal step count less than or equal to 1500 steps.
RQ3 = collection.find({'goal.stepGoal':{'$lte':1500}})
print 
print "----- RQ3 - Goal step count <= 1500 -----"
print
for data in RQ3:
	pprint.pprint(data)
	print
print "--------------------------------------"
print

# RQ4: Aggregating the total activity duration for each employee
pipeline = [{"$project": {"_id": "$uid", "totalActivityDuration": {"$sum" : {"$ifNull": ["$activityDuration", 0]}}}}]
RQ4 = collection.aggregate(pipeline)
print 
print "----- RQ4 - Aggregated activity duration: -----"
print
for data in RQ4:
	pprint.pprint(data)
	print
print "--------------------------------------"
print

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

