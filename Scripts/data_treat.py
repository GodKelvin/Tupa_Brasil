import csv
import psycopg2

path_file_csv = "/home/godkelvin/Tupa_Brasil/Arquivos/fauna_flora_ameacada_2018.csv"
path_file_credentials = ".credentials"

dic_grupo_tax = {}
dic_grupao = {}
dic_familia = {}
dic_cat_ameaca = {}


#Config BD postgreSQL
with open(path_file_credentials, ) as csv_file_credentials:
	csv_reader = csv.reader(csv_file_credentials, delimiter=';')
	#Ignorar o cabecalho
	csv_reader.__next__()
	
	row = next(csv_file_credentials)
	row = row.split(';')
	#Confi BD
	conn = psycopg2.connect(host=row[0],database=row[1], user=row[2], password=row[3])


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
			
		if(row[1] !=  '' and row[1] not in dic_grupo_tax):
			dic_grupo_tax[row[1]] = cod_grupo_tax
			cod_grupo_tax += 1
			
		if(row[2] !=  '' and row[2] not in dic_familia):
			dic_familia[row[2]] = cod_familia
			cod_familia += 1
			
		if(row[5] !=  '' and row[5] not in dic_cat_ameaca):
			dic_cat_ameaca[row[5]] = cod_cat_ameaca
			cod_cat_ameaca += 1
		
		
input()
print(dic_grupao)
print("==========")
print(dic_grupo_tax)
print("==========")
print(dic_familia)
print("==========")
print(dic_cat_ameaca)
	
	