import csv
path_file_csv = "/home/godkelvin/Tupa_Brasil/Arquivos/fauna_flora_ameacada_2018.csv"

dic_grupo_tax = {}
dic_grupao = {}
dic_familia = {}
dic_cat_ameacao = {}

#import psycopg2
#conn = psycopg2.connect(host="tuffi.db.elephantsql.com",database="roqnvuhz", user="roqnvuhz", password="yAsEJSuLztzjuoP7WyAbdZ3CIEVzNyTM")


with open(path_file_csv) as csv_file:
	csv_reader = csv.reader(csv_file, delimiter=';')
	#Ignorar o cabecalho
	csv_reader.__next__()
	
	#Grupao[0], grupo_tax[1], familia[2], nome_especie[3], nome_comum[4], cat_ameaca[5]
	cod_grupao = 1
	cod_grupo_tax = 1
	cod_familia = 1
	for row in csv_reader:
		#print(row[0] + ',' + row[1] + ',' + row[2] + ',' + row[3] + ',' + row[4] + ',' +row[5])
		#input()
		#print(row[5])
		if(row[0] !=  '' and row[0] not in dic_grupao):
			dic_grupao[row[0]] = cod_grupao
			cod_grupao += 1
			
		if(row[1] !=  '' and row[1] not in dic_grupo_tax):
			dic_grupo_tax[row[1]] = cod_grupo_tax
			cod_grupo_tax += 1

print(dic_grupao)
print("==========")
print(dic_grupo_tax)


	
	