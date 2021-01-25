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
    
    Retorna:
        Documento no formato:

        {
            "host":<string>,
            "db": <string>,
            "collection": <string>,
            "estrutura": docMetadados*,
            "versao": <valor>|null
        }

        * docMetadados - documento no padrão:
        {
            "nomeDoCampo1":"dataType",                          <-- Campo Simples
            "nomeDoCampoDocumento2":"Object",                   <-- Campo do Tipo Documento recebem dataType "Object"
            "nomeDoCampoDocumento2.nomeDoCampo1": "dataType",   <-- Campos de um documento recebem o nome do campo pai
            "nomeDoCampoArray1[]": "dataType",                  <-- Campos no formato array recebem o simbolo "[]" ao final.
            "nomeDoCampoArray2[]": "Object",                    <-- Campo Array de Objeto
            "nomeDoCampoArray2[].nomeDoCampo1": "dataType"      <-- Campos de um array de documento, possuem "[]" no nome do campo pai
        }
    '''


  

    def getTypeDoc( docParm ):
        
        '''
        Extrai typos de dados de um documento.
        
        Parmametros:
            - docParm: variável no formato JSON
        Retorna:
            - documento com os metadados em padrão pre-definido.
        {
            "nomeDoCampo1":"dataType",                          <-- Campo Simples
            "nomeDoCampoDocumento2":"Object",                   <-- Campo do Tipo Documento recebem dataType "Object"
            "nomeDoCampoDocumento2.nomeDoCampo1": "dataType",   <-- Campos de um documento recebem o nome do campo pai
            "nomeDoCampoArray1[]": "dataType",                  <-- Campos no formato array recebem o simbolo "[]" ao final.
            "nomeDoCampoArray2[]": "Object",                    <-- Campo Array de Objeto
            "nomeDoCampoArray2[].nomeDoCampo1": "dataType"      <-- Campos de um array de documento, possuem "[]" no nome do campo pai
        }
        '''

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
            if len(arrayTipo) == 1 :
                docArray['[]']= arrayTipo[0]
            else:
                docArray['[]']= 'null'
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
