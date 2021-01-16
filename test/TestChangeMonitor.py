# import os
import pymongo

# client = pymongo.MongoClient(os.environ['CHANGE_STREAM_DB'])
client = pymongo.MongoClient("mongodb://root:mongopass@127.0.0.1/admin?retryWrites=true")
client.clientx.insert({"__id": 1234 ,"nome": "Cliente A"})
# db.clientex.insert({"__id": 1234 ,"nome": "Cliente A", dataNascimento : new Date(), renda: { valor: 123, anoRef: 2020}})
# print(client.changestream.collection.insert_one({"hello": "world"}).inserted_id)