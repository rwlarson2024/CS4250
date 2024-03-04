#-------------------------------------------------------------------------
# AUTHOR: your name
# FILENAME: title of the source file
# SPECIFICATION: description of the program
# FOR: CS 4250- Assignment #2
# TIME SPENT: 6:00 PM to 1:00AM
#-----------------------------------------------------------*/

#importing some Python libraries
from db_connection import *

if __name__ == '__main__':

    # Connecting to the database
    conn = connectDataBase()

    # Getting a cursor
    cur = conn.cursor()
    createTables(cur, conn)

    #print a menu
    print("")
    print("######### Menu ##############")
    print("#a - Create a category.")
    print("#b - Create a document")
    print("#c - Update a document")
    print("#d - Delete a document.")
    print("#e - Output the inverted index.")
    print("#q - Quit")

    option = ""
    while option != "q":

          print("")
          option = input("Enter a menu choice: ")

          if (option == "a"):

              catId = input("Enter the ID of the category: ")
              catName = input("Enter the name of the category: ")

              createCategory(cur, catId, catName)
              conn.commit()

          elif (option == "b"):

              docId = input("Enter the ID of the document: ")
              docText = input("Enter the text of the document: ")
              docTitle = input("Enter the title of the document: ")
              docDate = input("Enter the date of the document: ")
              docCat = input("Enter the category of the document: ")

              createDocument(cur, docId, docText, docTitle, docDate, docCat)
              conn.commit()

          elif (option == "c"):

              docId = input("Enter the ID of the document: ")
              docText = input("Enter the text of the document: ")
              docTitle = input("Enter the title of the document: ")
              docDate = input("Enter the date of the document: ")
              docCat = input("Enter the category of the document: ")

              updateDocument(cur, docId, docText, docTitle, docDate, docCat)

              conn.commit()

          elif (option == "d"):

              docId = input("Enter the document id to be deleted: ")

              deleteDocument(cur, docId)

              conn.commit()

          elif (option == "e"):

              index = getIndex(cur)
              print(index)

          elif (option == "q"):

               print("Leaving the application ... ")

          else:

               print("Invalid Choice.")