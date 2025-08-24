from pymongo import MongoClient
from pymongo.errors import PyMongoError
from bson.objectid import ObjectId
from typing import Dict, Any

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, username, password):
        # Initializing the MongoClient. This helps to 
        # access the MongoDB databases and collections.
        # This is hard-wired to use the aac database, the 
        # animals collection, and the aac user.
        # Definitions of the connection string variables are
        # unique to the individual Apporto environment.
        #
        # Connection Variables
        #
        USER = 'aacuser'
        PASS = 'aacuserpwd'
        HOST = 'nv-desktop-services.apporto.com'
        PORT = 34167
        DB = 'AAC'
        COL = 'animals'
        #
        # Initialize Connection
        #
        self.client = MongoClient('mongodb://%s:%s@%s:%d' % (USER,PASS,HOST,PORT))
        self.database = self.client['%s' % (DB)]
        self.collection = self.database['%s' % (COL)]

    def create(self, data:Dict[Any, Any]):
        """
        Method to create and insert a document into the database that accepts a dictionary as an argument. 
        Returns True if insertion is successful.
        """
        
        if isinstance(data, Dict) and data is not None:
            try:
                result = self.database.animals.insert_one(data)  # data should be dictionary  
                return True if result.inserted_id else False
            except PyMongoError as error:
                print('Insert error: ', error)
                return False
        else:
            raise Exception('Error: Data must be a dictionary.')

    def read(self, query:Dict[Any, Any]):
        """
        Method to find a document in the database that accepts a dictionary as an argument.
        Returns the document as a list if successful, otherwise returns an empty list.
        """
        
        if isinstance(query, Dict) and query is not None:
            try:
                return list(self.database.animals.find(query))
            except PyMongoError as error:
                print('Find error: ', error)
                return []
        else:
            raise Exception('Error: Query must be a dictionary.')
            
    def update(self, findData:Dict[Any, Any], updateData:Dict[Any, Any]):
        """
        Method to search for and update documents in the database.
        This accepts a dictionary as the first argument to search,
        and another dictionary for the update data as the second argument.
        ***NOTE*** The updateData argument must include the '%set' operator.
                   The second argument format must be {'$set': {'key':'value'}}
        Returns the number of documents updated.
        """
        
        if isinstance(findData, Dict) and isinstance(updateData, Dict) and findData is not None and updateData is not None:
            try:
                return self.database.animals.update_many(findData, updateData).modified_count
            except PyMongoError as error:
                print('Update error: ', error)
                return 0
        else:
            raise Exception('Error: updateData and findData must both be dictionaries.')
                
    def delete(self, deleteData:Dict[Any, Any]):
        """
        Method to search for a delete documents that already exist in the database. Accepts a dictionary for an argument.
        Returns the number of documents deleted.
        """
        if isinstance(deleteData, Dict) and deleteData is not None:
            try:
                return self.database.animals.delete_many(deleteData).deleted_count
            except PyMongoError as error:
                print('Delete error: ', error)
                return 0
        else:
            raise Exception('Error: deleteData must be a dictionary.')
        # return number of documents deleted
        