import json

typePy2mongo = {
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

def getTypeDoc( docParm ):
    docTipo = {}
    for campo, valor in docParm.items() :
        if type(valor).__name__ == 'dict':
            docTipo[campo] =  typePy2mongo[type(valor).__name__ ]
            subDoc = getTypeDoc(valor)
            for campoSubDoc, valorSubDoc in subDoc.items():
                docTipo[campo+'.'+campoSubDoc]= valorSubDoc
        # elif type(valor).__name__ == 'list2':
        #     array1dTipo = type_array(valor)
        #     arrayTipoAdd = []
        #     tipoElem = ''
        #     for elem in array1dTipo :
        #         if type(elem).__name__ == 'dict' :
        #             tipoElem = 'dict'
        #             for campoArray, valorArray in elem.items() :
        #                 docTipo[campo+'[].'+campoArray] = valorArray
        #         elif type(elem).__name__ == 'list' :
        #             tipoElem = 'list'
        #             if len(elem) > 1 :
        #                 docTipo[campo+'[][]'] = elem
        #             else:
        #                 docTipo[campo+'[][]'] = elem[0]
        #             # docTipo[campo+'[][]'] = elem
        #         else:
        #             tipoElem = elem

        #         try:
        #             arrayTipoAdd.index(tipoElem)
        #         except ValueError:
        #             arrayTipoAdd.append(tipoElem)
        #         if len(arrayTipoAdd) > 1 :
        #             docTipo[campo+'[]'] = arrayTipoAdd
        #         else:
        #             docTipo[campo+'[]'] = arrayTipoAdd[0]
        elif type(valor).__name__ == 'list':
            docArrayTipo = getTypeArrayDoc(valor)
            for campoDocArray, valorDocArray in docArrayTipo.items():
                docTipo[campo+''+campoDocArray]= valorDocArray
        else:
            docTipo[campo] =  typePy2mongo[type(valor).__name__ ]
            pass 
    return docTipo

## ajustar navegação de arrays para refletir níveis de profundidade nos nomes dos campos. 
# def type_array( arrayParm ):
#     arrayTipo = []
#     docArray = {'[]': [] }
#     for elemento in arrayParm :
#         if type(elemento).__name__ == 'dict':
#             subDoc = type_doc(elemento)
#             arrayTipo.append(subDoc)
#             # for campoSubDoc, valorSubDoc in subDoc.items():
#             #     docArray['[].'+campoSubDoc]= valorSubDoc
#         elif type(elemento).__name__ == 'list':
#             subArray = type_array(elemento)
#             arrayTipo.append(subArray)
#             # for campoSubDoc, valorSubDoc in subArray.items():
#             #     docArray['[][]'+campoSubDoc]= valorSubDoc
#         else:
#             try:
#                 arrayTipo.index(type(elemento).__name__)
#             except ValueError:
#                 arrayTipo.append(type(elemento).__name__)
#     print(arrayTipo)
#     return arrayTipo

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
            arrayTipo.index(typePy2mongo[type(elemento).__name__])
        except ValueError:
            arrayTipo.append(typePy2mongo[type(elemento).__name__])
    # print(arrayTipo)
    if len(arrayTipo) > 1 :
        docArray['[]']= arrayTipo
    else:
        docArray['[]']= arrayTipo[0]
    return docArray