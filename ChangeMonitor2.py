import pymongo
from bson.json_util import dumps
from metadataFunctions import *

## tratar padrões BSON -> Json
from bson.json_util import DEFAULT_JSON_OPTIONS
DEFAULT_JSON_OPTIONS.json_mode = 1   #relaxed

# # pipeline a ser usado futuramente pra filtrar eventos a serem monitorados e databases.
# pipeline = [
#     {'$match': {'operationType': {'$in': ['insert', 'delete', 'replace', 'update', 'rename']}}}
# ]
client = pymongo.MongoClient("mongodb://root:mongopass@127.0.0.1/admin?retryWrites=true")
# client = pymongo.MongoClient("mongodb://root:mongopass@mongodb/admin?retryWrites=true")
change_stream = client.watch(full_document='updateLookup')  #retorna sempre o documento completo atualizado

import pprint 
# Campos a verificar: 
# - fullDocument (p/insert, replace, update)
# - to (p/ rename)
print('** Monitorando alterações na instalação MongoDB **')
for change in change_stream:

    # print ( doctipo )
    print('--------------')
    #builder = SchemaBuilder()
    #builder.add_schema({"type": "object", "properties": {}})
    ##builder.add_object(dumps(change["fullDocument"]))
    #builder.add_object(eval(dumps(change["fullDocument"])))
    print(change["operationType"], '- servidor:',change["ns"])
    if change["operationType"] in ['delete', 'drop']:
        print('Operação:',change["operationType"],' BD: ', change["ns"]["db"], ' Colection:', change["ns"]["coll"])
    else:
        print('Operação:',change["operationType"],' BD: ', change["ns"]["db"], ' Colection:', change["ns"]["coll"],'\nDocumento: ', dumps(change["fullDocument"]))
        print('#### Datatypes ###')
        doctipo = getTypeDoc(change["fullDocument"]) 
        pprint.pprint(doctipo)
   

    
