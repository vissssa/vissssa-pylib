from pymongo import MongoClient


class EasyMongoClient:
    def __init__(self, url):
        self.client = MongoClient(url)

    def get_database(self, db):
        return self.client.get_database(db)
