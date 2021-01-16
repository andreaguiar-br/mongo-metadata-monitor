import pymongo
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
print('',"*"*60,'\n ***      Monitorando Alterações no Cluster MongoDB       ***\n',"*"*60)
for change in mongoChangeStream:

    if change["operationType"] in ['delete', 'drop']:
       print('Operação:',change["operationType"],' BD: ', change["ns"]["db"], ' Colection:', change["ns"]["coll"])
       # tratar drop de database ou collection para atualizar o estado no metadado
    else:
        print('Operação:',change["operationType"],' BD: ', change["ns"]["db"], ' Colection:', change["ns"]["coll"],'\nDocumento: ', dumps(change["fullDocument"]))

        docMetadados = getMetadados(mongoServerAddress, change)
        resGrav = atualizaMetadadosCollection(docMetadados)
        print(resGrav)
   

    
