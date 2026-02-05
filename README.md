1️⃣ Raw

Foi desenvolvido um script responsável por realizar uma chamada HTTP em uma API pública desenvolvida pelo https://github.com/guto-alves, utilizando especificamente o endpoint da Lotofácil, que é o jogo de interesse para este estudo.

Inicialmente, o script valida o retorno da API verificando o status_code da requisição. Caso o retorno seja diferente de 200, a execução é interrompida e o erro é exibido.

Em seguida, o JSON retornado pela API é convertido em um DataFrame pandas, onde são aplicados tratamentos iniciais. Esses tratamentos garantem que colunas que deveriam conter listas (dezenas, dezenasOrdemSorteio, premiacoes e localGanhadores) não apresentem valores nulos, substituindo-os por listas vazias, evitando falhas durante o processo de normalização.

Após isso, são removidas colunas consideradas irrelevantes para o contexto da ingestão e é iniciado o processo de normalização dos dados. Para isso, utiliza-se a função explode, transformando estruturas de arrays em múltiplas linhas e gerando tabelas distintas de acordo com cada contexto: concurso, dezenas, dezenas em ordem de sorteio, premiações e local de ganhadores.

Por fim, os DataFrames pandas são convertidos em DataFrames Spark, com a adição da coluna de metadado data_extracao. As tabelas são então gravadas no ambiente Databricks, utilizando o catálogo raw e o schema default. Para a persistência dos dados, é utilizado o modo overwrite, garantindo que cada execução substitua completamente os dados existentes.

2️⃣ Ref

Na camada Ref, optei por desenvolver views SQL, priorizando praticidade e reutilização dos dados. Essas views são construídas a partir das tabelas da camada Raw, onde é realizada a tipagem explícita das colunas, o ajuste de nomenclatura e a organização dos dados em dimensões (dim) e fatos (fat).

O objetivo dessa camada é disponibilizar dados consistentes, com tipos corretos e sem estruturas complexas, facilitando o entendimento do contexto e preparando as informações para análises futuras e consumo por ferramentas de BI.
