#-------------------------------------------------------------------------
# AUTHOR: Ryan Larson
# FILENAME: db_connection.py
# SPECIFICATION: Simple PGAdmin connection program to add data to tables, query the tables, change the data, and create an inverted index
# FOR: CS 4250- Assignment #2
# TIME SPENT: 6:00 PM - 1:00 AM
#-----------------------------------------------------------*/


#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
import psycopg2
from psycopg2.extras import RealDictCursor
import re
from collections import defaultdict
from collections import Counter
def connectDataBase():

    DB_NAME = "CPP"
    DB_USER = "postgres"
    DB_PASS = "123"
    DB_HOST = "localhost"
    DB_PORT = "5432"
    try:
        conn = psycopg2.connect(database=DB_NAME,
                                user=DB_USER,
                                password=DB_PASS,
                                host=DB_HOST,
                                port=DB_PORT,
                                cursor_factory=RealDictCursor)
        return conn

    except:
        print("Database not connected successfully")

def createCategory(cur, catId, catName):
    # Insert a category in the database
    sql = "INSERT INTO categories (id, name) VALUES (%s,%s)"
    recset = [catId, catName]
    cur.execute(sql, recset)

def createDocument(cur, docId, docText, docTitle, docDate, docCat):

    # 1 Get the category id based on the informed category name
    cur.execute("SELECT id FROM categories WHERE name = %s", (docCat,))
    category_id = cur.fetchone()

    # 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    sql = "INSERT INTO document (doc, text, title, num_chars, date, id_cat) VALUES (%s,%s,%s,%s,%s,%s)"

    num_chars = len("".join(char for char in docText if char.isalpha()))
    recset = [docId, docText, docTitle, num_chars, docDate, category_id['id']]
    cur.execute(sql,recset)

    # 3 Update the potential new terms.
    # 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    # 3.2 For each term identified, check if the term already exists in the database
    # 3.3 In case the term does not exist, insert it into the database

    #3.1
    newTerms = "".join(char if char.isalpha() or char.isspace() else ' ' for char in docText)
    newTermsLower = newTerms.lower()
    newTerms = newTermsLower.split()
    #allTerms = newTerms
    #3.2
    cur.execute("SELECT term FROM terms WHERE term IN %s", (tuple(newTerms),))
    term_results = cur.fetchall()
    #3.3
    existing_terms = []
    for row in term_results:
        term = row["term"] if 'term' in row else None
        if term is not None:
            existing_terms.append(term)

    new_terms = set(newTerms) - set(existing_terms)

    for term in new_terms:
        num_chars_term = len(term)
        cur.execute("INSERT INTO terms (term,num_chars) VALUES (%s,%s) returning term",(term,num_chars_term))
        term_row = cur.fetchone()
        if term_row:
            term_id = term_row['term']
            existing_terms.append(term_id)
        else:
            print("Error: Failed to retrieve term ID for term:", term)

    # 4 Update the index
    # 4.1 Find all terms that belong to the document
    all_results = re.findall(r'\b\w+\b', docText.lower())
    term_counts = defaultdict(int)
    for term in all_results:
        term_counts[term] +=1
    for term, count in term_counts.items():
        cur.execute("INSERT INTO term_registration (id_doc, id_term, term_count) "
                    "VALUES (%s, (SELECT term FROM terms WHERE term = %s), %s)",
                    (docId, term, count))

def deleteDocument(cur, docId):
    # 1 Query the index based on the document to identify terms
    # 1.1 For each term identified, delete its occurrences in the index for that document
    # 1.2 Check if there are no more occurrences of the term in another document. If this happens, delete the term from the database.
    cur.execute("SELECT id_term FROM term_registration WHERE id_doc = %s",(docId,))
    term_occurrences = cur.fetchall()

    for term_occurrence in term_occurrences:
        id_term = term_occurrence['id_term']  # Access the value using the key 'id_term'

        cur.execute("DELETE FROM term_registration WHERE id_term = %s AND id_doc = %s", (id_term, docId))

        cur.execute("SELECT COUNT(*) FROM term_registration WHERE id_term = %s", (id_term,))
        term_occurrences_count_row = cur.fetchone()

        # Check if there are any rows returned and the value is not None
        if term_occurrences_count_row is not None and term_occurrences_count_row['count'] is not None:
            term_occurrences_count = term_occurrences_count_row['count']
        else:
            term_occurrences_count = 0

        if term_occurrences_count == 0:
            cur.execute("DELETE FROM terms WHERE term = %s", (id_term,))
    # 2 Delete the document from the database
    cur.execute("DELETE FROM document WHERE doc = %s", (docId,))
def updateDocument(cur, docId, docText, docTitle, docDate, docCat):
    # 1 Delete the document
    deleteDocument(cur, docId)

    # 2 Create the document with the same id
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):
    term_occurance = {}
    cur.execute("SELECT distinct term FROM terms")
    terms = cur.fetchall()
    for term_row in terms:
        term = term_row["term"]
        cur.execute("SELECT document.title AS document_name, COUNT(*) AS count " \
                    "FROM term_registration " \
                    "JOIN document ON term_registration.id_doc = document.doc " \
                    "WHERE term_registration.id_term = %s " \
                    "GROUP BY term_registration.id_doc, document.title",
                    (term,))
        document_counts = cur.fetchall()
        term_occurance[term] = ','.join([f"{doc_count['document_name']}:{doc_count['count']}" for doc_count in document_counts])
    return term_occurance
    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here

def createTables(cur, conn):
    try:
        sql = "create table categories (id integer not null, name character varying(255)not null, " \
              "constraint categories_pk primary key (id))"
        cur.execute(sql)

        sql = "create table terms (term character varying(255)not null, num_chars integer not null," \
              "constraint terms_PK primary key (term))"
        cur.execute(sql)

        sql = "create table document(doc integer not null, text character varying(255)not null," \
              "title character varying(255), num_chars integer not null," \
              "date date not null, id_cat integer not null," \
              "constraint doc_PK primary key (doc)," \
              "constraint doc_categories_id_FK foreign key(id_cat) references categories (id))"
        cur.execute(sql)

        sql = "create table term_Registration (id_doc integer not null, id_term character varying(255) not null," \
              "term_count integer not null," \
              "constraint registration_PK primary key(id_doc,id_term)," \
              "constraint doc_id_Fk foreign key(id_doc) references document (doc), " \
              "constraint term_id_FK foreign key(id_term) references terms (term))"
        cur.execute(sql)

        conn.commit()

    except:
        conn.rollback()
        print("There was a problem during the datebase creation or the database already exists.")