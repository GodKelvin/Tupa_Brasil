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