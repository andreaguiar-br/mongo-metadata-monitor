import os
import pymongo
from bson.json_util import dumps

# pipeline a ser usado futuramente pra filtrar eventos a serem monitorados e databases.
pipeline = [
    {'$match': {'operationType': {'$in': ['insert', 'delete', 'replace', 'update', 'rename']}}}
]
client = pymongo.MongoClient(os.environ['CHANGE_STREAM_DB'])
# change_stream = client.changestream.collection.watch()
change_stream = client.watch(full_document='updateLookup')

# Campos a verificar: 
# - fullDocument (p/insert, replace, update)
# - to (p/ rename)
print('** Monitorando alterações na instalação MongoDB **')
for change in change_stream:
    # print (dumps(change))
    if change["operationType"]=='delete':
        print('Operação:',change["operationType"],' BD: ', change["ns"]["db"], ' Colection:', change["ns"]["coll"])
    else:
        print('Operação:',change["operationType"],' BD: ', change["ns"]["db"], ' Colection:"', change["ns"]["coll"],' Documento: ', change["fullDocument"])
    
