import csv
import psycopg2

path_file_credentials = ".credentials"

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

	cur = conn.cursor()
	cur.execute("select * from grupo_tax")
	res = cur.fetchall()
	print(res)
	
	dic_grupo_tax = {}
	for grupo_tax in res:
		value_cod = grupo_tax[0]
		key_nome_grupo_tax = grupo_tax[1]
		dic_grupo_tax[key_nome_grupo_tax] = value_cod
	
	print("='='='='='='='='='='")
	print(dic_grupo_tax)