#-------------------------------------------------------------------------
# AUTHOR: Ryan Larson
# FILENAME: index_mongo.py
# SPECIFICATION: Connection to MongoDB to create a date base of
# FOR: CS 4250- Assignment #3
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
def connectDataBase():

    # Create a database connection object using pymongo
    client = MongoClient("mongodb://localhost:27017/")
    db = client["documents"]
    return db

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    indexed_terms = "".join(char if char.isalpha() or char.isspace() else '' for char in docText)
    indexed_terms_lower = indexed_terms.lower()
    indexed_terms_list = indexed_terms_lower.split()

    term_count = {}
    for term in indexed_terms_list:
        term = term.strip()
        if term:
            term_count[term] = term_count.get(term,0) + 1
    # create a list of objects to include full term objects. [{"term", count, num_char}]
    # --> add your Python code here
    term_objects = []
    for term, count in term_count.items():
        term_object = {"term" : term, "count": count, "num_char": len(term)}
        term_objects.append(term_object)
    # produce a final document as a dictionary including all the required document fields
    document = {
        "_id" : docId,
        "title" : docTitle,
        "text" : docText,
        "num_char" : len(docText),
        "date" : docDate,
        "category" : docCat,
        "terms": term_objects
    }

    # insert the document
    insert_result = col.insert_one(document)
    return print("inserted document ID:", insert_result.inserted_id)
def deleteDocument(col, docId):
    # Delete the document from the database
    delete_result = col.delete_one({"_id": docId})
    if delete_result.deleted_count == 1:
        return print("Document deleted successfully.")
    else:
        return print("Document not found or not deleted.")

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    deleteDocument(col,docId)

    # Create the document with the same id
    createDocument(col, docId, docText, docTitle, docDate, docCat)
    return
def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    pipeline = [
        {"$unwind": "$terms"},
        {"$group": {"_id": "$terms.term", "titles": {"$addToSet": "$title"}, "count": {"$sum": "$terms.count"}}},
        {"$project": {"_id": 0, "term": "$_id", "titles": 1, "count": 1}}
    ]
    result = list(col.aggregate(pipeline))
    output = {item["term"]:f"{' , '.join(item['titles'])}:{item['count']}" for item in result}

    return output