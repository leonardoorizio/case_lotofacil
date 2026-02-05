%sql
/* tabela local ganhadores */
create or replace view ref.default.dim_local_ganhadores as 
select 
  cast(concurso as string) as concurso,
  cast(ganhadores as string) as ganhadores,
  cast(municipio as string) as municipio,
  cast(nomeFatansiaUL as string) as nomeFatansiaUL,
  cast(serie as string) as serie,
  cast(posicao as string) as posicao,
  cast(uf as string) as uf,
  to_date(data_extracao, 'dd/MM/yyyy') as data_extracao
from raw.default.df_local_ganhadores

--------------------------------------------------------------------------#

%sql
/* tabela premiações */
create or replace view ref.default.dim_premiacoes as 
select 
  cast(concurso as string) as concurso,
  cast(descricao as string) as descricao,
  cast(faixa as string) as faixa,
  cast(ganhadores as string) as ganhadores,
  cast(valorPremio as string) as valorPremio,
  to_date(data_extracao, 'dd/MM/yyyy') as data_extracao
from raw.default.df_premiacoes

--------------------------------------------------------------------------#

%sql
/* tabela dezenas */
create or replace view ref.default.dim_dezenas as 
select 
  cast(concurso as string) as concurso,
  cast(dezena as string) as dezena,
  to_date(data_extracao, 'dd/MM/yyyy') as data_extracao
from raw.default.df_dezenas

--------------------------------------------------------------------------#

%sql
/* tabela concurso */
create or replace view ref.default.fat_concurso as 
select 
  cast(loteria as string) as loteria,
  cast(concurso as string) as concurso,
  to_date(data, 'dd/MM/yyyy') as data,
  cast(local as string) as local,
  cast(concursoEspecial as string) as concursoEspecial,
  cast(acumulou as string) as acumulou,
  cast(proximoConcurso as string) as proximoConcurso,
  to_date(dataProximoConcurso, 'dd/MM/yyyy') as dataProximoConcurso,
  cast(valorArrecadado as float) as valorArrecadado,
  cast(valorAcumuladoConcurso_0_5 as float) as valorAcumuladoConcurso_0_5,
  cast(valorAcumuladoConcursoEspecial as float) as valorAcumuladoConcursoEspecial,
  cast(valorAcumuladoProximoConcurso as float) as valorAcumuladoProximoConcurso,
  to_date(data_extracao, 'dd/MM/yyyy') as data_extracao
from raw.default.df_concurso

--------------------------------------------------------------------------#
