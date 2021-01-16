''' Funções de tratamento de documentos para extração de metadados
'''
import json
import pymongo
from datetime import datetime, timezone
import pytz


PYTHON2MONGO_DATATYPE = {
    "bool" : "bool",
    "int"  : "int32",
    "long" : "long",
    "Int64" : "long",
    "float" : "double",
    "str"   : "string",
    "dict"  : "object",
    "list"  : "array",
    "timestamp": "timestamp",
    "date"  : "date",
    "datetime": "datetime",
    "ObjectId": "objectId",
    "Binary" : "binData",
    "Decimal128": "decimal",
    "NoneType" : "null",
    "DBRef" : "dbref",
    "Byte" : "binData"

}
def getMetadados( mongoServerAddress: str, docChangeStream : dict ) -> dict:
    '''
    Gera documento JSON com metadados extraídos do documento gerado pelo Change Stream do MongoDB.

    parametros:
    servidorMongo - Servidor mongoDB usado para conexão e observação dos metadados
    docChamStream - Documento gerado pelo evento de ChangeStream e que terá seus metadados extraídos

    retorna:
    documento com os metadados em padrão pre-definido.
    '''
  

    def getTypeDoc( docParm ):
        docTipo = {}
        for campo, valor in docParm.items() :
            if type(valor).__name__ == 'dict':
                docTipo[campo] =  PYTHON2MONGO_DATATYPE[type(valor).__name__ ]
                subDoc = getTypeDoc(valor)
                for campoSubDoc, valorSubDoc in subDoc.items():
                    docTipo[campo+'.'+campoSubDoc]= valorSubDoc
            elif type(valor).__name__ == 'list':
                docArrayTipo = getTypeArrayDoc(valor)
                for campoDocArray, valorDocArray in docArrayTipo.items():
                    docTipo[campo+''+campoDocArray]= valorDocArray
            else:
                docTipo[campo] =  PYTHON2MONGO_DATATYPE[type(valor).__name__ ]
                pass 
        return docTipo

    def getTypeArrayDoc( arrayParm ):
        arrayTipo = []
        docArray = {'[]': [] }
        for elemento in arrayParm :
            if type(elemento).__name__ == 'dict':
                subDoc = getTypeDoc(elemento)
                # arrayTipo.append(subDoc)
                for campoSubDoc, valorSubDoc in subDoc.items():
                    docArray['[].'+campoSubDoc]= valorSubDoc
            elif type(elemento).__name__ == 'list':
                subDocArray = getTypeArrayDoc(elemento)
                # arrayTipo.append(subArray)
                for campoSubDoc, valorSubDoc in subDocArray.items():
                    if '[]'+campoSubDoc in docArray :
                        # se já existe, adiciona o tipo encontrato a lista de documentos
                        if type(docArray['[]'+campoSubDoc]).__name__=='list' :
                            try:
                                docArray['[]'+campoSubDoc].extend(valorSubDoc) 
                                #removendo duplicatas
                                docArray['[]'+campoSubDoc]=list(dict.fromkeys(docArray['[]'+campoSubDoc]))    
                            except ValueError:
                                pass
                        else:
                            #transforma valor existente em lista
                            # print('11111', "['"+docArray['[]'+campoSubDoc]+"','"+ valorSubDoc+"']")
                            docArray['[]'+campoSubDoc]=[ docArray['[]'+campoSubDoc] , valorSubDoc ] 
                    else:
                        # primeiro registro do array, cria registro no documento
                        docArray['[]'+campoSubDoc]= valorSubDoc
            else:
                pass
            try:
                arrayTipo.index(PYTHON2MONGO_DATATYPE[type(elemento).__name__])
            except ValueError:
                arrayTipo.append(PYTHON2MONGO_DATATYPE[type(elemento).__name__])
        # print(arrayTipo)
        if len(arrayTipo) > 1 :
            docArray['[]']= arrayTipo
        else:
            docArray['[]']= arrayTipo[0]
        return docArray


    doctipo = getTypeDoc(docChangeStream["fullDocument"]) 

 
    if  mongoServerAddress == '' :
        raise "Parametro 'servidorMongo' não localizado"

    if "schema_version" in docChangeStream["fullDocument"]:
        versaoEstrutura=docChangeStream["fullDocument"]["schema_version"]
    else:
        versaoEstrutura=None

    return {"host":mongoServerAddress,
        "db": docChangeStream["ns"]["db"],
        "collection": docChangeStream["ns"]["coll"],
        "estrutura": doctipo,
        "versao": versaoEstrutura
        }
