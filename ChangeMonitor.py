import pymongo
from util import getMongoWatch, mongoServerAddress
from bson.json_util import dumps
from metadataFunctions import getMetadados, atualizaMetadadosCollection
import pprint
# import urllib.parse
# import logging

## tratar padrões BSON -> Json
# from bson.json_util import DEFAULT_JSON_OPTIONS
# DEFAULT_JSON_OPTIONS.json_mode = 1   #relaxed

# # logging = logging.getLogger(__name__)
# # TODO: pegar o servidor mongo a partir de variavel ambiente os.environ['CHANGE_STREAM_DB']
# # username = urllib.parse.quote_plus('root')
# # password = urllib.parse.quote_plus('mongopass')
# username = "root"
# password = "mongopass"
# servidorMongo = os.environ['CHANGE_STREAM_DB']
# print('*** Conectando..',"mongodb://"+username+":'password'@"+os.environ['CHANGE_STREAM_DB']+"/admin?retryWrites=true")

# client = pymongo.MongoClient("mongodb://root:mongopass@127.0.0.1/admin?retryWrites=true")
# client = pymongo.MongoClient("mongodb://root:mongopass@"+os.environ['CHANGE_STREAM_DB']+"/admin?retryWrites=true")

# # pipeline a ser usado futuramente pra filtrar eventos a serem monitorados e databases.
# filtroWatch = [
#     {'$match': {'operationType': {'$in': ['insert', 'delete', 'replace', 'update', 'rename', 'drop']}}},
#     {'$match': {'ns.db': {'$ne': 'mdbmmd'}, 'ns.coll': {'$ne': 'colecaoMongo'}}}
# ]

# client = pymongo.MongoClient("mongodb://root:mongopass@mongodb/admin?retryWrites=true")
# client = pymongo.MongoClient(
#     host = servidorMongo, # <-- IP and port go here
#     serverSelectionTimeoutMS = 6000, # 3 second timeout
#     username=username,
#     password=password,
#     authSource='admin',
#     authMechanism='SCRAM-SHA-256'
# )

mongoChangeStream = getMongoWatch()


#retorna sempre o documento completo atualizado
# mongoChangeStream = client.watch(full_document='updateLookup', pipeline=FILTRO_MONGO_WATCH)  

# import pprint 
# Campos a verificar: 
# - fullDocument (p/insert, replace, update)
# - to (p/ rename)
print('** Monitorando alterações na instalação MongoDB **')
for change in mongoChangeStream:

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
        docMetadados = getMetadados(mongoServerAddress, change)
        resGrav = atualizaMetadadosCollection(docMetadados)
        print(resGrav)
   

    
