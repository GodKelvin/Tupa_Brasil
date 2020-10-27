import csv
import psycopg2

path_file_csv = "/home/godkelvin/Tupa_Brasil/Arquivos/fauna_flora_ameacada_2018.csv"
path_file_credentials = ".credentials"

dic_grupo_tax = {}
dic_grupao = {}
dic_familia = {}
dic_cat_ameaca = {}

_host = ""
_db = ""
_user = ""
_password = ""


def consult(sql_query):
	cur.execute(sql_query)
	res = cur.fetchall
	print(res)
	#conn.close()
	return res


#Config BD postgreSQL
with open(path_file_credentials, ) as csv_file_credentials:
	csv_reader = csv.reader(csv_file_credentials, delimiter=';')
	#Ignorar o cabecalho
	csv_reader.__next__()
	
	row = next(csv_file_credentials)
	row = row.split(';')
	#print(row)
	_host = row[0]
	_db = row[1]
	_user = row[2]
	_password = row[3]
	
	print("...Try connection BD...")
	try:
		conn = psycopg2.connect(host=_host,database=_db, user=_user, password=_password)
		
	except:
		print("Error to acess Database")
		
#Criando um cursor
cur = conn.cursor()
print("...Reset Counters BD...")
#Resetando os counters do BD
sql_inst = 'ALTER SEQUENCE bioma_cod_bioma_seq RESTART WITH 1;'
cur.execute(sql_inst)

sql_inst = 'ALTER SEQUENCE categoria_ameaca_cod_ameaca_seq RESTART WITH 1;'
cur.execute(sql_inst)

sql_inst = 'ALTER SEQUENCE especie_cod_especie_seq RESTART WITH 1;'
cur.execute(sql_inst)

sql_inst = 'ALTER SEQUENCE especie_bioma_cod_especie_bioma_seq RESTART WITH 1;'
cur.execute(sql_inst)

sql_inst = 'ALTER SEQUENCE familia_cod_familia_seq RESTART WITH 1;'
cur.execute(sql_inst)

sql_inst = 'ALTER SEQUENCE grupao_cod_grupao_seq RESTART WITH 1;'
cur.execute(sql_inst)

sql_inst = 'ALTER SEQUENCE grupo_tax_cod_grupo_tax_seq RESTART WITH 1;'
cur.execute(sql_inst)

conn.commit()

print("...Delete all data from tables...")
#Apagando dados do banco de dados (SIM, SEM WHERE, APAGA TUDO :D )
sql_inst = 'DELETE FROM bioma;'
cur.execute(sql_inst)

sql_inst = 'DELETE FROM categoria_ameaca;'
cur.execute(sql_inst)

sql_inst = 'DELETE FROM especie;'
cur.execute(sql_inst)

sql_inst = 'DELETE FROM especie_bioma;'
cur.execute(sql_inst)

sql_inst = 'DELETE FROM familia;'
cur.execute(sql_inst)

sql_inst = 'DELETE FROM grupao;'
cur.execute(sql_inst)

sql_inst = 'DELETE FROM grupo_tax;'
cur.execute(sql_inst)

conn.commit()

with open(path_file_csv) as csv_file:
	csv_reader = csv.reader(csv_file, delimiter=';')
	#Ignorar o cabecalho
	csv_reader.__next__()
	
	#Grupao[0], grupo_tax[1], familia[2], nome_especie[3], nome_comum[4], cat_ameaca[5]
	cod_grupao = 1
	cod_grupo_tax = 1
	cod_familia = 1
	cod_cat_ameaca = 1
	
	
	for row in csv_reader:
		#print(row[0] + ',' + row[1] + ',' + row[2] + ',' + row[3] + ',' + row[4] + ',' +row[5])
		#input()
		#print(row[5])
		
		#Verificar se as chaves estrangeiras existem
		#Adicionar as chaves estrangeiras em suas respectivas tabelas
		#Adicionar infos da especie e suas chaves
		if(row[0] !=  '' and row[0] not in dic_grupao):
			dic_grupao[row[0]] = cod_grupao
			cod_grupao += 1
			
			sql_inst = "select * from grupao where nome_grupao = '" + row[0] + "';"
			#Executa instrucao
			cur.execute(sql_inst)
			
			#Guarda resposta da instrucao executada
			res = cur.fetchall()
			
			if(res == []):
				sql_inst = "insert into GRUPAO(nome_grupao) values('" + row[0] +"');"
				cur.execute(sql_inst)
				conn.commit()
				
		
		if(row[1] !=  '' and row[1] not in dic_grupo_tax):
			dic_grupo_tax[row[1]] = cod_grupo_tax
			cod_grupo_tax += 1
			
			sql_inst = "select * from familia where nome_familia = '" + row[1] + "';"
			cur.execute(sql_inst)
			res = cur.fetchall()
			#print(res)
			if(res == []):
				sql_inst = "insert into FAMILIA(nome_familia) values('"+row[1]+"');"
				cur.execute(sql_inst)
				conn.commit()
		
		'''
		if(row[2] !=  '' and row[2] not in dic_familia):
			dic_familia[row[2]] = cod_familia
			cod_familia += 1
			
		if(row[5] !=  '' and row[5] not in dic_cat_ameaca):
			dic_cat_ameaca[row[5]] = cod_cat_ameaca
			cod_cat_ameaca += 1
		'''
		
		
#input()
print(dic_grupao)
'''
print("==========")
print(dic_grupo_tax)
print("==========")
print(dic_familia)
print("==========")
print(dic_cat_ameaca)
'''
conn.close()
