''' Utilitários para monitoração de ChangeStream do MongoDB '''
import os
import pymongo
import pprint

__all__ = ["getMongoWatch", "mongoServerAddress"]


#####################################################################
## Funções
#####################################################################
def getMongoWatch():
    ''' 
        Retorna a conexão do MongoDB utilizada para ser monitorado (por ChangeStream )
    '''
    return _conexao.watch(full_document='updateLookup', pipeline=FILTRO_MONGO_WATCH)  
    

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


    print('*** Conectando..',"mongodb://"+_username+":'password'@"+mongoServerAddress+"/admin?retryWrites=true")
    # client = pymongo.MongoClient("mongodb://root:mongopass@127.0.0.1/admin?retryWrites=true")
    # client = pymongo.MongoClient("mongodb://root:mongopass@"+os.environ['CHANGE_STREAM_DB']+"/admin?retryWrites=true")

    return pymongo.MongoClient(
        host = mongoServerAddress, # <-- IP and port go here
        serverSelectionTimeoutMS = 6000, # 3 second timeout
        username=_username,
        password=_password,
        authSource='admin',
        authMechanism='SCRAM-SHA-256')


## Variáveis do módulo



mongoServerAddress = os.environ['CHANGE_STREAM_DB'] 
if not mongoServerAddress:
    raise '## "/''mongoServerAddress/'' não informado'

# pipeline a ser usado futuramente pra filtrar eventos a serem monitorados e databases. 
# TODO: (rever o match de ignorar o BD da colecao mongo)
FILTRO_MONGO_WATCH = [
    {'$match': {'operationType': {'$in': ['insert', 'delete', 'replace', 'update', 'rename', 'drop']}}},
     {'$match': {'ns.db': {'$ne': 'mdbmmd'}, 'ns.coll': {'$ne': 'colecaoMongo'}}}
]

_conexao  = _conectWatchDB(mongoServerAddress)
print("** MongoDB Server Information ***")
pprint.pprint(_conexao.server_info())