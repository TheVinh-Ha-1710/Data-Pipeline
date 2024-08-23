import pymongo

# Connect to MongoDB and access collection:
client = pymongo.MongoClient("localhost", 27017)
db = client["Assignment1"]
col = db["Song"]

# Creating output:
output = []
for doc in col.find():
    artist = doc["Artist"]
    year = doc["Year"]
    sales = doc["Sales"]
    triplet = f"{artist},{year},{sales}"
    output.append(triplet)

# Saving output:
output_path = (
    "C:/Users/HATHEVENT/Desktop/Big Data/Assignment 1/Task1/Output_Task1/output1_1.txt"
)
with open(output_path, "w") as file:
    for song in output:
        file.write(song + "\n")
