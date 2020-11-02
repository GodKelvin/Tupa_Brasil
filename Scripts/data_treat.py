import csv
import psycopg2

path_file_credentials = ".credentials"

# Variaveis globais
dic_grupo_tax = {}
dic_grupao = {}
dic_familia = {}
dic_cat_ameaca = {}
conn = ""

def sql_check(table, column, parameter):
	cur = conn.cursor()
	#Dois '%%' pra inserir %s e em seguida, executar com parametro
	sql_inst = "select * from %s where %s = %%s;" %(table, column)
	cur.execute(sql_inst, [parameter])
	res = cur.fetchall()
	
	if(res == []):
		#insert into TABELA(coluna) values(parametros)
		#Dois '%%' pra inserir %s e em seguida, executar com parametro
		sql_inst = "insert into %s(%s) values(%%s);" %(table, column)
		#print(sql_inst)
		cur.execute(sql_inst, [parameter])
		conn.commit()
		
	cur.close()

def extract_credentials(path_file_credentials):
	#Config BD postgreSQL
	with open(path_file_credentials) as csv_file_credentials:
		csv_reader = csv.reader(csv_file_credentials, delimiter=';')
		#Ignorar o cabecalho
		csv_reader.__next__()
		
		row = next(csv_file_credentials)
		row = row.split(';')
		#print(row)
		'''global _host 
		global _user 
		global _password'''
		
		_host = row[0]
		_db = row[1]
		_user = row[2]
		_password = row[3]
		
		print("...Try connection BD...")
		try:
			global conn
			conn = psycopg2.connect(host=_host,database=_db, user=_user, password=_password)
			print("---Connection Success---")
			
		except:
			print("Error to acess Database")
			
def reset_database():
	
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

	sql_inst = 'DELETE FROM especie;'
	cur.execute(sql_inst)


	#Apagando dados do banco de dados (SIM, SEM WHERE, APAGA TUDO :D )
	print("...Delete all data from tables...")
	sql_inst = 'DELETE FROM bioma;'
	cur.execute(sql_inst)

	sql_inst = 'DELETE FROM categoria_ameaca;'
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
	cur.close()

def extract_data_old_files(path_file_csv, date_reg, _encoding):
	#Permitindo acesso as variaveis globais
	global dic_grupo_tax
	global dic_grupao
	global dic_familia
	global dic_cat_ameaca
	
	#Criando um cursor
	cur = conn.cursor()
	print("...Get and Insert Global Data from csv file - %s..." %date_reg)
	with open(path_file_csv, encoding=_encoding) as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=';')
		#Ignorar o cabecalho
		csv_reader.__next__()
		
		#Grupao[0], grupo_tax[1], familia[2], nome_especie[3], nome_comum[4], cat_ameaca[5]
		cod_grupao = 1
		cod_grupo_tax = 1
		cod_familia = 1
		cod_cat_ameaca = 1
		
		for row in csv_reader:
			#Verificar se as chaves estrangeiras existem
			#Adicionar as chaves estrangeiras em suas respectivas tabelas
			#Adicionar infos da especie e suas chaves
			if(row[0] !=  '' and row[0] not in dic_grupao):
				dic_grupao[row[0]] = cod_grupao
				cod_grupao += 1
				#Tabela, coluna, parametro
				sql_check("GRUPAO", "nome_grupao", row[0])
			
			if(row[1] !=  '' and row[1] not in dic_grupo_tax):
				dic_grupo_tax[row[1]] = cod_grupo_tax
				cod_grupo_tax += 1
				sql_check("GRUPO_TAX", "nome_grupo_tax", row[1])
				
			if(row[2] !=  '' and row[2] not in dic_familia):
				dic_familia[row[2]] = cod_familia
				cod_familia += 1
				sql_check("FAMILIA", "nome_familia", row[2])
			
			
			if(row[5] !=  '' and row[5] not in dic_cat_ameaca):
				dic_cat_ameaca[row[5]] = cod_cat_ameaca
				cod_cat_ameaca += 1
				#sql_check("CATEGORIA_AMEACA", "cat_ameaca", row[5])
				sql_check("CATEGORIA_AMEACA", "cat_ameaca", row[5])
					
		#Voltando para o inicio do arquivo
		csv_file.seek(0)
		csv_reader.__next__()
		print("...Get and Insert individual data specie %s from CSV file..." %date_reg)
		
		for row in csv_reader:
			if(row[0] != ""):
				fk_cod_grupao = dic_grupao[row[0]]
				fk_cod_grupo_tax = dic_grupo_tax[row[1]]
				fk_cod_familia = dic_familia[row[2]]
				nome_especie = row[3]
				nome_especie = nome_especie.replace("'", "''")
				nome_comum = row[4]
				nome_comum = nome_comum.replace("'", "''")
				fk_cod_ameaca = dic_cat_ameaca[row[5]]
				#Se nao ter nome comum, insere null
				if(nome_comum == "Vazio" or nome_comum == "vazio"):
					nome_comum = None
				
				sql_inst = "insert into ESPECIE(nome_especie, nome_comum, data_registro, \
							fk_cod_grupo_tax, fk_cod_grupao, fk_cod_familia, fk_cod_ameaca)\
							values(%s, %s, %s, %s, %s, %s, %s);"
							
				cur.execute(sql_inst, (nome_especie, nome_comum, date_reg, fk_cod_grupo_tax, fk_cod_grupao, fk_cod_familia, fk_cod_ameaca))
		
		conn.commit()
	print("---Species %s Inserted---" %date_reg)

def main():
	path_file_csv_2018 = "/home/godkelvin/Tupa_Brasil/Arquivos/fauna_flora_ameacada_2018.csv"
	path_file_csv_2019 = "/home/godkelvin/Tupa_Brasil/Arquivos/fauna_flora_ameacada_2019.csv"
	extract_credentials(path_file_credentials)	
	reset_database()
	
	#Caminho do arquivo e data dos dados
	extract_data_old_files(path_file_csv_2018, '2018', 'utf-8')

	extract_data_old_files(path_file_csv_2019, '2019', 'iso-8859-1')
	
	'''
	print(len(dic_grupao))
	print(len(dic_grupo_tax))
	print(len(dic_familia))
	print(len(dic_cat_ameaca))
	'''
	#Encerrando conexao com o banco de dados
	conn.close()
	
main()



