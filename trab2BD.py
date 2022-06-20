import sys
import re
import psycopg2

def conexao_BD():
    #função que faz a conexão com o banco de dados
    try:
        conexao = psycopg2.connect(database= 'trabalho',
                                    host = 'localhost',
                                    user = 'postgres',
                                    password = 'root')
        print("Conectou!")
        return conexao     
    except psycopg2.DatabaseError as e:
        print("Erro ao conectar o banco:", e)
        return None

con = conexao_BD()

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
    
    arquivo_linhas = limpar(arquivo_linhas)

    for i in range(len(arquivo_linhas)):
        if arquivo_linhas[i] == '':
            inserir_banco(arquivo_linhas, i)
            break
    
    




# limpando as linhas para iniciar a leitura do arquivo
def limpar(arquivo_linhas):
    for linha in range(len(arquivo_linhas)):
        arquivo_linhas[linha] = re.sub('\n','',arquivo_linhas[linha])
        arquivo_linhas[linha] = re.sub('<','',arquivo_linhas[linha])
        arquivo_linhas[linha] = re.sub('>','',arquivo_linhas[linha])

    return arquivo_linhas


def inserir_banco(arquivo_linhas,i):
    cur = con.cursor()
    cur.execute(sql)
    inseriu = arquivo_linhas[0:i]

    for arquivo_linhas in inseriu:
        arquivo_linhas = re.sub('=', ',', arquivo_linhas)
        quebra = arquivo_linhas.split(',')
        sql = " select * from tabela1 where id = {}".format(quebra[1])
        cur.execute(sql)
        r = cur.fetchall()
        if r:
            sql = "update tabela1 set {} = {} where id {}".format(quebra[0], quebra[2], quebra[1])
            cur.execute(sql)
        else:
            sql = "insert into tabela1 (id, {}) values ".format(quebra[0], quebra[1], quebra[2])
            cur.execute(sql)

    con.commit()
    imprimir_variaveis()

def imprimir_variaveis():
    print(" Imprimindo váriaveis ---- ")
    cur  = con.cursor()
    sql = "select * from tabela order by id"
    cur.execute(sql)
    r = cur.fetchall()
    print(r)
    


main()

