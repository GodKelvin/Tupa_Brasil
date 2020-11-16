import csv
import psycopg2

# Variaveis globais
dic_grupo_tax = {}
dic_grupao = {}
dic_familia = {}
dic_cat_ameaca = {}
dic_bioma = {}

conn = ""

def sql_check(table, column, parameter):
	#print(table, column, parameter)
	#input()
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
	'''
	#Permitindo acesso as variaveis globais
	
	global dic_grupo_tax
	global dic_grupao
	global dic_familia
	global dic_cat_ameaca
	'''
	#Criando um cursor
	cur = conn.cursor()
	print("...Get and Insert Global Data from csv file - %s..." %date_reg)
	with open(path_file_csv, encoding=_encoding) as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=';')
		#Ignorar o cabecalho
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


def create_dic_from_sql(table_name):
	dic_table = {}
	cur = conn.cursor()
	sql_inst = "select * from %s;" %table_name
	cur.execute(sql_inst)
	res = cur.fetchall()
	
	for tuple in res:
		#print(tuple)
		value_cod = tuple[0]
		key_dic = tuple[1]
		dic_table[key_dic] = value_cod
	
	cur.close()
	return dic_table
	
def check_global_data_files_SQL(path_file_csv, _encoding):
	#Permitindo acesso as variaveis globais
	global dic_grupo_tax
	global dic_grupao
	global dic_familia
	global dic_cat_ameaca
	global dic_bioma
	
	dic_grupo_tax = create_dic_from_sql("grupo_tax")
	dic_grupao = create_dic_from_sql("grupao")
	dic_familia = create_dic_from_sql("familia")
	dic_cat_ameaca = create_dic_from_sql("categoria_ameaca")
	
	
	
	#print("...Get and Insert Global Data from csv file - %s..." %date_reg)
	print("... Check global data...")
	with open(path_file_csv, encoding=_encoding) as csv_file:
		csv_reader = csv.reader(csv_file, delimiter=';')
		#Ignorar o cabecalho
		csv_reader.__next__()
		
		#Grupao[0], grupo_tax[1], familia[2], nome_especie[3], nome_comum[4], 
		#cat_ameaca[6], bioma[7], desc_ameaca[8]
		
		cod_grupao = 1
		cod_grupo_tax = 1
		cod_familia = 1
		cod_cat_ameaca = 1
		
		for row in csv_reader:
			#print(row[0]+', '+ row[1], row[2], row[3], row[4], row[6], row[7], row[8])
			#print("%s, %s, %s, %s, %s, %s, %s, %s" %(row[0], row[1], row[2], row[3], row[4], row[6], row[7], row[8])) 
			#input()
			#Verificar se as chaves estrangeiras existem
			#Adicionar as chaves estrangeiras em suas respectivas tabelas
			#Adicionar infos da especie e suas chaves
			
			
			
			'''
			if(row[0] !=  '' and row[0] not in dic_grupao):
				#dic_grupao[row[0]] = cod_grupao
				#cod_grupao += 1
				#Tabela, coluna, parametro
				sql_check("GRUPAO", "nome_grupao", row[0])
				dic_grupao = create_dic_from_sql("grupao")
			
		
			if(row[1] !=  '' and row[1] not in dic_grupo_tax):
				#dic_grupo_tax[row[1]] = cod_grupo_tax
				#cod_grupo_tax += 1
				sql_check("GRUPO_TAX", "nome_grupo_tax", row[1])
				dic_grupo_tax = create_dic_from_sql("grupo_tax")
			
			if(row[2] !=  '' and row[2] not in dic_familia):
				#dic_familia[row[2]] = cod_familia
				#cod_familia += 1
				sql_check("FAMILIA", "nome_familia", row[2])
				dic_familia = create_dic_from_sql("familia")
			
			
			if(row[6] !=  '' and row[6] not in dic_cat_ameaca):
				#dic_cat_ameaca[row[5]] = cod_cat_ameaca
				#cod_cat_ameaca += 1
				sql_check("CATEGORIA_AMEACA", "cat_ameaca", row[6])
				dic_cat_ameaca = create_dic_from_sql("categoria_ameaca")
			'''
			if(row[7] !=  '' and row[7] not in dic_bioma):
				vet_biomas = row[7].split(';')
				for bioma in vet_biomas:
					print(vet_biomas)
					
					if(bioma != '' and bioma != ' '):
						if(bioma[0] == ' '):
							bioma = bioma[1:]
							
						sql_check("BIOMA", "nome_bioma", bioma)
						dic_bioma = create_dic_from_sql("bioma")
				
				

def main():
	#path_file_csv_2018 = "/home/godkelvin/Tupa_Brasil/Arquivos/fauna_flora_ameacada_2018.csv"
	#path_file_csv_2019 = "/home/godkelvin/Tupa_Brasil/Arquivos/fauna_flora_ameacada_2019.csv"
	path_file_csv_2020 = "/home/godkelvin/Tupa_Brasil/Arquivos/fauna_flora_ameacada_2020.csv"
	path_file_credentials = ".credentials_teste"
	extract_credentials(path_file_credentials)	
	reset_database()
	
	#Verificando se as tabelas tem os respectivos dados globais para serem associados as especies
	#check_global_data_files_SQL(path_file_csv_2018, 'utf-8')
	#check_global_data_files_SQL(path_file_csv_2019, 'iso-8859-1')
	check_global_data_files_SQL(path_file_csv_2020, 'utf-8')
	
	#Caminho do arquivo e data dos dados
	#extract_data_old_files(path_file_csv_2018, '2018', 'utf-8')
	#extract_data_old_files(path_file_csv_2019, '2019', 'iso-8859-1')
	#extract_data_old_files(path_file_csv_2020, '2020', 'iso-8859-1')
	
	'''
	print(len(dic_grupao))
	print(len(dic_grupo_tax))
	print(len(dic_familia))
	print(len(dic_cat_ameaca))
	'''
	#Encerrando conexao com o banco de dados
	conn.close()
	
main()



