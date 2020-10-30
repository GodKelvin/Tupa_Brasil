# Tupã Brasil

<div align="center">
  <i> Tupã, criador dos céus, terra e mares, assim como do mundo animal e vegetal, lamentavelmente observa sua criação perecer. </i>
	<br><br>
	<img src="https://github.com/GodKelvin/Tupa_Brasil/blob/main/Imagens/tupa_henrique_tome.jpg" width=45%>
	<br>
  <i>Artista: <a href="https://www.artstation.com/henriquetome">Henrique Tomé</a></i>
</div>

# Sumário
## Introdução
Modelando, agrupando e realizando uma análise dos dados das espécies ameaçadas de extinção da biodiversidade brasileira, tendo como base dados públicos do Ministério do Meio Ambiente.<br>

## QUAIS PERGUNTAS PODEM SER RESPONDIDAS COM A ANÁLISE PROPOSTA?
* Relatório listando os animais ameaçados de extinção por categoria de ameaça.
* Relatório listando os animais ameaçados de extinção separados por grupos taxonômicos.
* Relatório listando os animais ameaçados de extinção separados por família.
* Relatório listando os animais ameaçados de extinção separados por grupão (Fauna/Flora).
* Relatório da relação dos animais ameaçados de extinção levando em conta o ano do registro e a sua categoria de ameaça.
* Relatório da relação dos animais ameaçados de extinção levando em conta o ano do registro e a seu grupo taxonômico.
* Relatório da relação dos animais ameaçados de extinção levando em conta o ano do registro e sua família.
* Relatório da relação dos animais ameaçados de extinção levando em conta o ano do registro e o grupão pertencente.
* Relatório da relação dos animais ameaçados de extinção levando em conta o ano do registro e o seu bioma (se disponível tal informação). 

## DATASETS
Arquivos no formato .CSV.<br>
[Espécies Ameaçadas 2018](https://github.com/GodKelvin/Tupa_Brasil/blob/main/Arquivos/fauna_flora_ameacada_2018.csv)<br>
[Espécies Ameaçadas 2019](https://github.com/GodKelvin/Tupa_Brasil/blob/main/Arquivos/fauna_flora_ameacada_2019.csv)<br>
[Espécies Ameaçadas 2020](https://github.com/GodKelvin/Tupa_Brasil/blob/main/Arquivos/fauna_flora_ameacada_2020.csv)<br>
Dados disponibilizados pelo [Ministério do Meio Ambiente](https://dados.gov.br/dataset/especies-ameacadas).<br>

## De .CSV para SQL
Essa conversão de CSV para SQL é apenas por questões acadêmicas, visto que devido a pouca quantidade de dados (Dez mil registros (Cerca de três mil e duzentos por arquivo)), a leitura em memória seria possível normalmente.<br>

Mas antes da conversão, primeiro criamos o banco de dados.
### Modelo Conceitual
![Alt text](https://github.com/GodKelvin/Tupa_Brasil/blob/main/Imagens/modelo_conceitual.png)

### Modelo Lógico
![Alt text](https://github.com/GodKelvin/Tupa_Brasil/blob/main/Imagens/modelo_logico.png)

### Modelo Físico
SGBD Utilizado: PostgreSQL
```
/*Eliminando a estrutura do banco de dados */
drop table if exists ESPECIE cascade;
drop table if exists GRUPAO cascade;
drop table if exists GRUPO_TAX cascade;
drop table if exists CATEGORIA_AMEACA cascade;
drop table if exists FAMILIA cascade;
drop table if exists BIOMA cascade;
drop table if exists ESPECIE_BIOMA cascade;

/*Criando a estrutura das tabelas*/
create table ESPECIE(
	cod_especie SERIAL primary key not null,
	nome_especie varchar(50),
	nome_comum varchar(155),
	desc_ameaca varchar(10),
	data_registro varchar(4) not null,
	fk_cod_grupo_tax integer not null,
	fk_cod_grupao integer not null,
	fk_cod_familia integer not null,
	fk_cod_ameaca integer not null
);

create table GRUPAO(
	cod_grupao SERIAL primary key not null,
	nome_grupao varchar(25) not null
);

create table GRUPO_TAX(
	cod_grupo_tax SERIAL primary key not null,
	nome_grupo_tax varchar(25) not null
);

create table CATEGORIA_AMEACA(
	cod_ameaca SERIAL primary key not null,
	cat_ameaca varchar(25) not null
);

create table FAMILIA(
	cod_familia SERIAL primary key not null,
	nome_familia varchar(25) not null
);

create table BIOMA(
	cod_bioma SERIAL primary key not null,
	nome_bioma varchar(25) not null
);

create table ESPECIE_BIOMA(
	cod_especie_bioma SERIAL primary key not null,
	fk_cod_especie integer,
	fk_cod_bioma integer
);

alter table ESPECIE
add foreign key(fk_cod_grupao) references GRUPAO(cod_grupao),
add foreign key(fk_cod_grupo_tax) references GRUPO_TAX(cod_grupo_tax),
add foreign key(fk_cod_familia) references FAMILIA(cod_familia),
add foreign key(fk_cod_ameaca) references CATEGORIA_AMEACA(cod_ameaca);

alter table ESPECIE_BIOMA
add foreign key(fk_cod_especie) references ESPECIE(cod_especie),
add foreign key(fk_cod_bioma) references BIOMA(cod_bioma);
```
### Script de conversão dos dados
O tratamento dos arquivos foi feito utilizando a linguagem Python, cujo o objetivo é extrair as informações dos respectivos arquivos e inseri-las num banco de dados relacional (PostgreSQL - Salvo em Nuvem) para que posteriormente fosse feita a análise dos dados. <br>

<a href="https://github.com/GodKelvin/Tupa_Brasil/blob/main/Scripts/data_treat.py"><img src="https://github.com/GodKelvin/Tupa_Brasil/blob/main/Imagens/icon_py.png" width="150px" height: auto></a>
