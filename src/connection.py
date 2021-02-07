'''
Utilitários para conexão com o Mongo usado para monitoração (ChangeStream)
'''
import logging
import os
import pymongo
import pprint
# import connection
from datetime import datetime, timedelta
from bson import timestamp

#####################################################################
## Funções
#####################################################################
# def getConnection() -> pymongo.MongoClient :
#     ''' Retorna a conexão com o MongoDB utilizado para ser monitorado '''
#     return _conexao

def getSchemaDBConnection() -> pymongo.MongoClient :
    ''' Retorna a conexão com o MongoDB utilizado para ser monitorado '''
    return _conexaoSchemaDB

def getMongoWatch():
    ''' 
        Retorna a objeto ChangeStream do MongoDB utilizada para iteração 
    '''
    #TODO: Revisar lógica pensando em multiplas instancias e versões - compatibilidade de tokens.
    if int(_conexaoServerVersion[0]) >= 4 :
        if _resumeToken is None:
            return _conexao.watch(full_document='updateLookup', pipeline=FILTRO_MONGO_WATCH, start_at_operation_time=TIMESTAMP_INICIO_WATCH)  
        else:
            if int(_conexaoServerVersion[1]) >= 2 :  #startAfter só funciona a partir da versão 4.2
                return _conexao.watch(full_document='updateLookup', pipeline=FILTRO_MONGO_WATCH, start_after=_resumeToken) 
            else: 
                return _conexao.watch(full_document='updateLookup', pipeline=FILTRO_MONGO_WATCH, resume_after=_resumeToken)  
    else:
        if _resumeToken is None:
            return _conexao.watch(full_document='updateLookup', pipeline=FILTRO_MONGO_WATCH)  
        else:
            return _conexao.watch(full_document='updateLookup', pipeline=FILTRO_MONGO_WATCH, resume_after=_resumeToken)   
    

def _conectWatchDB(mongoServerAddress: str ) -> pymongo.MongoClient:
    ''' 
        Conecta e retorna a conexão com o servidor mongoDB a ser monitorado (por ChangeStream )
    '''
 
    # logging = logging.getLogger(__name__)
    # TODO: pegar o servidor mongo a partir de variavel ambiente os.environ['CHANGE_STREAM_DB']
    # username = urllib.parse.quote_plus('root')
    # password = urllib.parse.quote_plus('mongopass')
    _username = "root"
    _password = "mongopass"


    logging.debug('*** Conectando [CHANGE_STREAM_DB] -> mongodb://%s:*******@%s/admin?retryWrites=true',_username,mongoServerAddress)
    # print('*** Conectando [CHANGE_STREAM_DB]..',"mongodb://"+_username+":'password'@"+mongoServerAddress+"/admin?retryWrites=true")
    # client = pymongo.MongoClient("mongodb://root:mongopass@127.0.0.1/admin?retryWrites=true")
    # client = pymongo.MongoClient("mongodb://root:mongopass@"+os.environ['CHANGE_STREAM_DB']+"/admin?retryWrites=true")

    return pymongo.MongoClient(
        host = mongoServerAddress, # <-- IP and port go here
        serverSelectionTimeoutMS = 6000, # 3 second timeout
        username=_username,
        password=_password,
        authSource='admin',
        authMechanism='SCRAM-SHA-256')

def _conectMetadataDB( ) -> pymongo.MongoClient:
    ''' 
        Conecta e retorna a conexão com o servidor mongoDB a ser usado para gravar os Metadados dos Schemas
    '''
 
    # logging = logging.getLogger(__name__)
    # TODO: pegar o servidor mongo a partir de variavel ambiente os.environ['CHANGE_STREAM_DB']
    # username = urllib.parse.quote_plus('root')
    # password = urllib.parse.quote_plus('mongopass')
    _usernameSchemaDB = "root"
    _passwordSchemaDB = "mongopass"

    logging.debug('*** Conectando [SCHEMA_DB] -> mongodb://%s:*******@%s/admin?retryWrites=true',_usernameSchemaDB,_mongoServerSchemaDB)
    # print('*** Conectando [SCHEMA_DB]..',"mongodb://"+_usernameSchemaDB+":'password'@"+_mongoServerSchemaDB+"/admin?retryWrites=true")
    # client = pymongo.MongoClient("mongodb://root:mongopass@127.0.0.1/admin?retryWrites=true")
    # client = pymongo.MongoClient("mongodb://root:mongopass@"+os.environ['CHANGE_STREAM_DB']+"/admin?retryWrites=true")

    return pymongo.MongoClient(
        host = _mongoServerSchemaDB, # <-- IP and port go here
        serverSelectionTimeoutMS = 6000, # 3 second timeout
        username=_usernameSchemaDB,
        password=_passwordSchemaDB,
        authSource='admin', # TODO Mudar para o BD do schema, após criação de usuário específico autorizado para atualizar esse database/colleciton
        authMechanism='SCRAM-SHA-256')


def getSavedResumeToken():
    controlCollection = getSchemaDBConnection()["mdbmmd"]["controleProcessamento"] 
    # TODO controlKey = ID da sessão de watch (pegar parametro de env) para usar com chave de atualização e busca
    docControle = controlCollection.find_one(filter={"_id": 1})
    if not docControle :
        return None
    else:
        if "resumeToken" in docControle:
            return docControle["resumeToken"]
        else:
            return None   

def setSavedResumeToken(resumeToken):
    controlCollection = getSchemaDBConnection()["mdbmmd"]["controleProcessamento"] 
    # TODO controlKey = ID da sessão de watch (pegar parametro de env) para usar com chave de atualização e busca
    infoCmdDB = controlCollection.update_one({"_id": 1},{"$set":{"resumeToken":resumeToken}},upsert=True)
    if infoCmdDB.modified_count == 0 :
        # print('insert estrutura:', infoInsert.raw_result)    
        return {'codigo': 000, 'message':"ResumeToken não salvo"}
    else:
        return {'codigo': 000, 'message':"ResumeToken atualizado"}




##############################################################
# EXECUÇÃO INICIAL
##############################################################


# pipeline a ser usado futuramente pra filtrar eventos a serem monitorados e databases. 
# TODO: (rever o match de ignorar o BD da colecao mongo)
FILTRO_MONGO_WATCH = [
    {'$match': {'operationType': {'$in': ['insert', 'delete', 'replace', 'update', 'rename', 'drop']}}},
     {'$match': {'ns.db': {'$ne': 'mdbmmd'}, 'ns.coll': {'$ne': 'colecaoMongo'}}}
]


logging.info("*"*40)
logging.info ("*** Configurando a Conexão ")
logging.info ("*"*40)

if not 'CHANGE_STREAM_DB' in os.environ:
    logging.error('Variável de ambiente CHANGE_STREAM_DB não localizada')
    raise OSError('Variável de ambiente CHANGE_STREAM_DB não localizada')

if not 'SCHEMA_DB' in os.environ:
    logging.error('Variável de ambiente SCHEMA_DB não localizada')
    raise OSError('Variável de ambiente SCHEMA_DB não localizada')

# Identificação das configurações de conexão
mongoServerAddress = os.environ['CHANGE_STREAM_DB'] 
logging.info ("*** CHANGE_STREAM_DB=%s",mongoServerAddress)
_mongoServerSchemaDB = os.environ['SCHEMA_DB'] 
logging.info ("*** SCHEMA_DB=%s",_mongoServerSchemaDB)

# conectando no BD de atualização dos schemas
_conexaoSchemaDB = _conectMetadataDB()

#conectando no BD de Observação do Change Stream
_conexao  = _conectWatchDB(mongoServerAddress)
logging.info("*** Conectado. Escutando servidor %s:%s",str(_conexao.address[0]),str(_conexao.address[1]))

# Identificando versão do BD de Observação
_conexaoServerVersion = tuple(_conexao.server_info()['version'].split('.'))
logging.info("*** Versão identificada [CHANGE_STREAM_DB]: %s",str(_conexaoServerVersion))

# Parametro para o comando 'watch' só disponivel para versão mongo acima da 4.0 
_diasReinicio = os.getenv("CHANGE_STREAM_DIAS_REINICIO","0")
logging.info("*** CHANGE_STREAM_DIAS_REINICIO=%s",_diasReinicio)

TIMESTAMP_INICIO_WATCH=timestamp.Timestamp(datetime.utcnow() - timedelta(days=int(_diasReinicio)),0)  #retroage 3 dias na busca de dados
# TIMESTAMP_INICIO_WATCH=_conexao.server_info()["$clusterTime"]["clusterTime"]
_resumeToken = getSavedResumeToken()
if _resumeToken is None:
    logging.info("*** Iniciando o Watch a partir de %s (UTC)",str(TIMESTAMP_INICIO_WATCH.as_datetime()))
else:
    logging.info("*** Iniciando o Watch a partir do token %s ",str(_resumeToken))

# recupera o servidor mongoDB conectado para Change Stream
mongoConnectedServer = str(_conexao.address[0])



print('\n',"*"*60,'\n **     MongoDB Server Information [CHANGE_STREAM_DB]     ***\n',"*"*60)
pprint.pprint(_conexao.server_info())

print('\n',"*"*60,'\n **        MongoDB Server Information [SCHEMA_DB]         ***\n',"*"*60)
pprint.pprint(_conexaoSchemaDB.server_info())