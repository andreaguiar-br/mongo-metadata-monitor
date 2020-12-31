import pymongo
from bson.json_util import dumps
from metadataFunctions import getMetadados, atualizaMetadadosCollection
# import logging

## tratar padrões BSON -> Json
# from bson.json_util import DEFAULT_JSON_OPTIONS
# DEFAULT_JSON_OPTIONS.json_mode = 1   #relaxed

# logging = logging.getLogger(__name__)
# TODO: pegar o servidor mongo a partir de variavel ambiente os.environ['CHANGE_STREAM_DB']
client = pymongo.MongoClient("mongodb://root:mongopass@127.0.0.1/admin?retryWrites=true")

# pipeline a ser usado futuramente pra filtrar eventos a serem monitorados e databases.
filtroWatch = [
    {'$match': {'operationType': {'$in': ['insert', 'delete', 'replace', 'update', 'rename', 'drop']}}},
    {'$match': {'ns.db': {'$ne': 'mdbmmd'}, 'ns.coll': {'$ne': 'colecaoMongo'}}}
]

# client = pymongo.MongoClient("mongodb://root:mongopass@mongodb/admin?retryWrites=true")
change_stream = client.watch(full_document='updateLookup', pipeline=filtroWatch)  #retorna sempre o documento completo atualizado

# import pprint 
# Campos a verificar: 
# - fullDocument (p/insert, replace, update)
# - to (p/ rename)
print('** Monitorando alterações na instalação MongoDB **')
for change in change_stream:

    if change["operationType"] in ['delete', 'drop']:
       print('Operação:',change["operationType"],' BD: ', change["ns"]["db"], ' Colection:', change["ns"]["coll"])
       # tratar drop de database ou collection para atualizar o estado no metadado
    else:
        print('Operação:',change["operationType"],' BD: ', change["ns"]["db"], ' Colection:', change["ns"]["coll"],'\nDocumento: ', dumps(change["fullDocument"]))
        # print('#### Datatypes ###')
        # doctipo = getTypeDoc(change["fullDocument"]) 
        # # pprint.pprint(doctipo)
        # # print("### Gravando Metadados ###")
        # docMetadados = {"host":client.HOST,
        #     "db": change["ns"]["db"],
        #     "collection": change["ns"]["coll"],
        #     "estrutura": doctipo 
        #     }
        docMetadados = getMetadados(change)
        resGrav = atualizaMetadadosCollection(client,docMetadados)
        print(resGrav)
   

    
