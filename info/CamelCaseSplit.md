# Identificar nomes em formado camelCase e segregar termos, em python
A partir da identificação dos metadados (campos e tipos de dados a eles associados), é possível verificar a adoção de regras padronizadas.
Uma das regras trata de adoção de nomenclaturas dos dados, que facilitem o entendimento do que representam.

## Glossário de termos e padronização de nomenclaturas de dados
É interessante que os metadados sigam um vocabulario comum, geralmente definido em um glossário de dados. Assim, quando se registra um campo como "Operação", dependendo do contexto do negócio, ou definição corporativa, fica claro se trata de um procedimento cirurgico, uma operação de crédito, ou uma ação tático-militar, por exemplo. Daí é fundamental ter um glossário, e verificar em que contexto o sistema irá tratar o glossário. Neste caso, se o sistema tiver contexto médico, a definição do termo "Operação" no glossário deve deixar claro o que se entende por "Operação".

## Código pythoon pra separar termos que estão em formato camelCase
* Veja outros formatos de representação de varáveis [aqui](https://medium.com/better-programming/string-case-styles-camel-pascal-snake-and-kebab-case-981407998841)

```
import re
re.sub('([a-z])([A-Z])', r'\1 \2', 'ThisIsCamelCase').split()
Out[11]: ['This', 'Is', 'Camel', 'Case']
```

outro exemplo

```
import re

name = 'CamelCaseTest123'
splitted = re.sub('([A-Z][a-z]+)', r' \1', re.sub('([A-Z]+)', r' \1', name)).split()
```
retorna
```
['Camel', 'Case', 'XYZ']
```

Baseado nessa técnica, é possível identificar padrões de estruturação de nomes de campos registrados nos documentos json e implementar verificações de adoção de padrões de montagem de nomes, além de identificação termos claros e definidos (em glossário de dados)