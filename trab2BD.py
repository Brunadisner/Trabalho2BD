import sys
import re

# import psycopg2
# #função que faz a conexão com o banco de dados
# host='localhost'
# dbname='dados'
# user='postgres'
# password='postgres'
# sslmode='require'
# conn_string = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(host, user, dbname, password, sslmode)
# conn = psycopg2.connect(conn_string) 
# cursor = conn.cursor()

# #função para inserir as tuplas na tabela
# def inserir_db(sql):
#   cursor.execute("DROP TABLE IF EXISTS inventory;")
#   cursor.execute("CREATE TABLE tabela1 (id INTEGER, A INTEGER, B INTEGER);")
#   cursor.execute("INSERT INTO dados VALUES(1,100,20);")
#   cursor.execute("INSERT INTO dados VALUES(2,20,30);")
#   conn.commit()
#   cursor.close()
#   conn.close()




def main():
    arquivo = sys.argv[1]
    file = open(arquivo, "r")

    arquivo_linhas = [] 
    for i in file:
        arquivo_linhas.append(i)
    
    for string in vetor:
        for caracter in string:
                vetor2 = re.findall(pattern,caracter)
        print(vetor2)

def limpar(arquivo_linhas):
    for linha in range(len(arquivo_linhas)):
        linha[arquivo_linhas] = re.sub('\n','',arquivo_linhas[linha])
        linha[arquivo_linhas] = re.sub('<','')
        linha[arquivo_linhas] = re.sub('>','')

    return arquivo_linhas


