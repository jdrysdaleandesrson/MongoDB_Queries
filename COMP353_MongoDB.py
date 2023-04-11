import pymongo
from pymongo import MongoClient
import pprint
client = MongoClient("")

#Getting a database called test_database
db = client.sample_airbnb

#Getting a collection called posts
posts = db.listingsAndReviews

"""List properties that are waterfront or have a wide entryway and a wide doorway 
(Waterfront OR (Wide entryway and Wide doorway)). 
Sort by the number of beds in descending order. Return the first 10 records."""

query = {"$or": [{"amenities": "Waterfront"},{"$and":[{"amenities": "Wide entryway"},
                                                      {"amenities":"Wide doorway"}]}]}

for post in posts.find(query,{"name","property_type","beds"}).sort("beds", -1).limit(10):
    pprint.pprint(post)
    
"""List the properties that have more than 50 reviews and have a score of 10 in cleanliness. 
Sort by the number of reviews in descending order. List the first 5 records."""

query = {"$and": [{"number_of_reviews" :{"$gt":50}},{"review_scores.review_scores_cleanliness": 10}]}

for post in posts.find(query,{"name","number_of_reviews",
                              "review_scores.review_scores_cleanliness"}).sort("number_of_reviews",-1).limit(5):
    pprint.pprint(post)
    
""" List the 5 hosts with the largest number of listed properties (host_listings_count) who also
have less than 90% of response rate (host_response_rate). Sort by the number of listed properties in
descending order. You can identify just the single host with the largest number of listed properties for
partial credit (10 points)."""


pipeline = [
  {"$match": {"host.host_response_rate": {"$lt": 90}}},
  {"$group": {"_id": "$host.host_id", "host_name": {"$first": "$host.host_name"}, 
              "host_response_rate": {"$first": "$host.host_response_rate"}, 
              "host_listings_count": {"$max": "$host.host_listings_count"}}},
  {"$sort": {"host_listings_count": -1}},
  {"$limit": 5},
  {"$project": {"_id": 0, "host_id": "$_id", "host_name": 1, "host_response_rate": 1, "host_listings_count": 1}}
]

result = posts.aggregate(pipeline)
for post in result:
    pprint.pprint(post)
    
    
    



    
