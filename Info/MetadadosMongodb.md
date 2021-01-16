# Estudo de Volumetria 

## Relação entre entidades

1000 BD  
(1-n)  
1000 Coleção(nome lógico, descrição) *entidade* possui um schema (versionável)  
(1-n)  
100 Schemas (versão, quantidade de documentos com a versão) (se não explicitada a mudança apenas adiciona campos novos)  
(n-n)   
100 campos-campos/documentos/*subentidades* (nome do campo ou subentidade pode mudar, ou campo pode passar para uma subentidade) - pode ser controlado por versão do schema e identificação do caminho do atributo (como referenciar no documento, como "endereço.logradouro")  
(1-n)  
10 atributos (nome lõgico, tipo-logico/fisico[], tamanho-logico/fisico[], descrição) (rastrear exclusões)  

## Atualização
- Maior por campos e tipos (tendencia a maior consulta no processo de atualização para identificar se já existe)
- 2nd maior por Schema (estrutura)
- Inclusão de nova collection mais rara (pode haver poucas situações de sistemas com criação frequente de collections)
- BD geralmente é criado quando há criação de novo microsserviço ou sistema usando o MongoDB

## Consultas:
- por nome/descrição do campo
- por nome/descrição/sistema da Coleção

## Apresentação:
- schema de uma coleção com tipos, descrição de campos e da coleção 
