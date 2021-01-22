# assuming loglevel is bound to the string value obtained from the
# command line argument. Convert to upper case to allow the user to
# specify --log=DEBUG or --log=debug
import os
import logging

levels = {'CRITICAL' : logging.CRITICAL,
    'ERROR' : logging.ERROR,
    'WARNING' : logging.WARNING,
    'INFO' : logging.INFO,
    'DEBUG' : logging.DEBUG,
    'NOTSET' : logging.NOTSET
}

loglevel=os.getenv('LOG_LEVEL','INFO')
# numeric_level = getattr(logging, loglevel.upper(), None)
numeric_level = levels[loglevel]
# if not isinstance(numeric_level, int):
#     numeric_level = logging.INFO
#     loglevel = "INFO"


print('',"*"*60,
    '\n ***      Monitorando Alterações no Cluster MongoDB       \n',
    "***      LOGGING LEVEL =",loglevel,'\n',
    "*"*60)


logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s', level=numeric_level)


import pymongo
import json
from connection import getMongoWatch, mongoServerAddress
from monitor import getMetadados
from bson.json_util import dumps
from metadataFunctions import atualizaMetadadosCollection
import pprint

## Recupera objeto changeStream para iteração
mongoChangeStream = getMongoWatch()


#retorna sempre o documento completo atualizado
# mongoChangeStream = client.watch(full_document='updateLookup', pipeline=FILTRO_MONGO_WATCH)  

# import pprint 
# Campos a verificar: 
# - fullDocument (p/insert, replace, update)
# - to (p/ rename)
# print('',"*"*60,'\n ***      Monitorando Alterações no Cluster MongoDB       ***\n',"*"*60)

for change in mongoChangeStream:

    if change["operationType"] in ['delete', 'drop']:
       print('Operação:',change["operationType"],' BD: ', change["ns"]["db"], ' Colection:', change["ns"]["coll"])
       # tratar drop de database ou collection para atualizar o estado no metadado
    else:
        print('Operação:',change["operationType"],' BD: ', change["ns"]["db"], ' Colection:', change["ns"]["coll"],'\nDocumento: ', dumps(change["fullDocument"]))

        docMetadados = getMetadados(mongoServerAddress, change)
        resGrav = atualizaMetadadosCollection(docMetadados)
        logging.info(json.dumps(resGrav))
   

    
