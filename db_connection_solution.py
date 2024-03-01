#-------------------------------------------------------------------------
# AUTHOR: Andrew Perez
# FILENAME: db_connection_solution.py
# SPECIFICATION: provides functionality for functions in index.py
# FOR: CS 4250- Assignment #2
# TIME SPENT: how long it took you to complete the assignment
#-----------------------------------------------------------*/
#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays
#importing some Python libraries
# --> add your Python code here
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import Error

def connectDataBase():
# Create a database connection object using psycopg2
# --> add your Python code here
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
    cur.execute("""INSERT INTO "Categories" (id_cat, name) VALUES (%s, %s)""", (catId, catName))

def createDocument(cur, docId, docText, docTitle, docDate, docCat):
# 1 Get the category id based on the informed category name
    cur.execute("""SELECT id_cat FROM "Categories" WHERE name = %s""", (docCat,))
    
# 2 Insert the document in the database. For num_chars, discard the spaces and punctuation marks.
    num_chars = len([char for char in docText if char.isalnum()])
    cur.execute("""INSERT INTO "Documents" (doc, text, title, num_chars, date, doc_cat) VALUES (%s, %s, %s, %s, %s, %s)""",
                (docId, docText, docTitle, num_chars, docDate, docCat))
    
# 3 Update the potential new terms.
# 3.1 Find all terms that belong to the document. Use space " " as the delimiter character for terms and Remember to lowercase terms and remove punctuation marks.
    terms = [term.strip().lower().strip("""!;,.?""") for term in docText.split()]
# 3.2 For each term identified, check if the term already exists in the database
    for term in terms:
        cur.execute("""SELECT * FROM "Terms" WHERE term = %s""", (term,))
        existing_term = cur.fetchone()
# 3.3 In case the term does not exist, insert it into the database
        if not existing_term:
            num_chars = len(term)
            cur.execute("""INSERT INTO "Terms" (term, num_chars) VALUES (%s, %s)""", (term, num_chars))
    
# 4 Update the index
# 4.1 Find all terms that belong to the document
# 4.2 Create a data structure the stores how many times (count) each term appears in the document
    term_counts = {}
    for term in terms:
        if term not in term_counts:
            term_counts[term] = 1
        else:
            term_counts[term] += 1
# 4.3 Insert the term and its corresponding count into the database
    for term, count in term_counts.items():
        cur.execute("""INSERT INTO "DocumentTerms" (doc_id, term, term_count) VALUES (%s, %s, %s)""",
                    (docId, term, count))

    cur.connection.commit()

def deleteDocument(cur, docId):
    # 1 Query the index based on the document to identify terms
    cur.execute("""SELECT term FROM "DocumentTerms" WHERE doc_id = %s""", (docId,))
    terms = cur.fetchall()

    # 1.1 For each term identified, delete its occurrences in the index for that document
    for term in terms:
        cur.execute("""DELETE FROM "DocumentTerms" WHERE doc_id = %s AND term = %s""", (docId, term['term']))

        # 1.2 Check if there are no more occurrences of the term in another document.
        # get all doc_id for a each term in this document, if the only doc_id for a term is this document, delete this term from Term tabel
        cur.execute("""SELECT COUNT(*) FROM "DocumentTerms" WHERE term = %s""", (term['term'],))
        count = cur.fetchone()
        if count['count'] == 0:
            # If no more occurrences of the term exist in another document, delete the term from the database
            cur.execute("""DELETE FROM "Terms" WHERE term = %s""", (term['term'],))

    # 2 Delete the document from the database
    cur.execute("""DELETE FROM "Documents" WHERE doc = %s""", (docId,))

    # Commit the transaction
    cur.connection.commit()

def updateDocument(cur, docId, docText, docTitle, docDate, docCat):
# 1 Delete the document
# --> add your Python code here
    deleteDocument(cur, docId)

# 2 Create the document with the same id
# --> add your Python code here
    createDocument(cur, docId, docText, docTitle, docDate, docCat)

def getIndex(cur):
# Query the database to return the "Documents" where each term occurs with theircorresponding count. Output example:
#{'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
# ...
# --> add your Python code here
    index = {}
    # Query the database to return the "Documents" where each term occurs with their corresponding count
    cur.execute("""SELECT term, title, term_count FROM "DocumentTerms" INNER JOIN "Documents" ON "Documents".doc = "DocumentTerms".doc_id""")
    cols = cur.fetchall()
    for col in cols:
        term = col['term']
        name = col['title']
        term_count = col['term_count']
        if term in index:
            index[term] += f",{name}: {term_count}"
        else:
            index[term] = f"{name}: {term_count}"
    return index
