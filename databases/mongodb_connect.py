from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

class MongoDBConnect:
    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name
        self.Client = None
        self.db = None

    def connect(self):
        try:
            self.Client = MongoClient(self.mongo_uri)
            self.Client.server_info() # test
            self.db = self.Client[self.db_name]

            print(f"--connected to MongoDB: {self.db_name}--")
            return self.db
        except ConnectionFailure as e:
            raise Exception(f"--failed to connect MongoDB: {e}--")
    def close(self):
        if self.Client:
            self.Client.close()
        print("--MongoDB connection closed--")

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()