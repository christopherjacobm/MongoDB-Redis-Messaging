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

print "WQ1 - Query for inserting all the entries from json file"
print "----------------------------------------"
# # Query for inserting all the entries from json file
# # ----------------------------------------
page = open('dummy-fitness.json', 'r');
parsed = json.loads(page.read());
for item in parsed:
	collection.insert(item); #WQ1
# # ----------------------------------------
print "----------------------------------------"


print "WQ2 - Query for updating the entries from json file"
print "----------------------------------------"
# # Query for updating the entries from json file
# # ----------------------------------------
pageMod = open('user1001-new.json', 'r');
jsonData = json.loads(pageMod.read());
for key, value in jsonData.iteritems():
	collection.update({'uid': jsonData['uid']},{"$set": {key: value}});
# # ----------------------------------------
print "----------------------------------------"


print "RQ1 - Query for counting all the entries"
print "----------------------------------------"
# # Query for counting all the entries
# # ----------------------------------------
RQ1 = collection.count()
print(RQ1);
# # ----------------------------------------
print "----------------------------------------"


print "RQ2 - Query for printing all the entries with active tag"
print "----------------------------------------"
# # Query for printing all the entries with active tag
# # ----------------------------------------
RQ2 = collection.find({'tags' : "active"})
for data in RQ2:
	pprint.pprint(data)
# # ----------------------------------------
print "----------------------------------------"


print "RQ3 - Query for printing all the entries with step goal greater than 5000"
print "----------------------------------------"
# # Query for printing all the entries with step goal greater than 5000
# # ----------------------------------------
RQ3 = collection.find({ "goal.stepGoal" : {"$gt" : 5000} })
for data in RQ3:
	pprint.pprint(data)
# # ----------------------------------------
print "----------------------------------------"


print "RQ4 - Query for printing the total Step counts of all users"
print "----------------------------------------"
# # Query for printing the total Step counts of all users
# # ----------------------------------------
pipeline = [{"$project": {"_id": "$_id", "totalStepCount": {"$sum" : {"$ifNull": ["$stepCount", 0]}}}}]
RQ4 = collection.aggregate(pipeline)
for data in RQ4:
	pprint.pprint(data)
# # ----------------------------------------
print "----------------------------------------"


# print "----------------------------------------"
# # # ----------------------------------------
# # collection.delete_many({"uid": {"$gt" : 1003}})
# # # ----------------------------------------
# print "----------------------------------------"


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

