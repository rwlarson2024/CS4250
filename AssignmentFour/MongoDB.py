from pymongo import MongoClient
def connectionDataBase():
    client = MongoClient("mongodb://localhost:27017")
    db = client["professors"]
    return db
def createDocument(col,doc):
    insert_result = col.insert_one(doc)
    return