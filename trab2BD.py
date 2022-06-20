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

#função para inserir as tuplas na tabela





def main():
    arquivo = sys.argv[1]
    file = open(arquivo, "r")

    arquivo_linhas = [] 
    for i in file:
        arquivo_linhas.append(i)
    
    arquivo_linhas = limpar(arquivo_linhas)

    create_db()
    linha = 0
    for i in range(len(arquivo_linhas)):
        if arquivo_linhas[i] == '':
            linha = i
            inserir_banco(arquivo_linhas, i)
            break
    
    arquivo_linhas = arquivo_linhas[linha+1::]
    
    


def create_db():
    cur = con.cursor()
    cur.execute("drop table tabela")
    cur.execute("CREATE TABLE tabela (id int not null, A INTEGER, B INTEGER, primary key(id));")
    con.commit()
    print("Tabela criada \n")
    

# limpando as linhas para iniciar a leitura do arquivo
def limpar(arquivo_linhas):
    for linha in range(len(arquivo_linhas)):
        arquivo_linhas[linha] = re.sub('\n','',arquivo_linhas[linha])
        arquivo_linhas[linha] = re.sub('<','',arquivo_linhas[linha])
        arquivo_linhas[linha] = re.sub('>','',arquivo_linhas[linha])

    return arquivo_linhas


def inserir_banco(arquivo_linhas,i):
    cur = con.cursor()
    sql = "truncate table tabela"
    cur.execute(sql)
    inseriu = arquivo_linhas[0:i]

    for linha in inseriu:
        linha = re.sub('=', ',', linha)
        quebra = linha.split(',')
        sql = " select * from tabela where id = {}".format(quebra[1])
        cur.execute(sql)
        r = cur.fetchall()
        if r:
            sql = "update tabela set {} = {} where id {}".format(quebra[0], quebra[2], quebra[1])
            cur.execute(sql)
        else:
            sql = "insert into tabela (id, {}) values ({}, {})".format(quebra[0], quebra[1], quebra[2])
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

def busca_ckpt(arquivo_linhas):
    start = 0
    end  = False

    for i, linha in enumerate(arquivo_linhas):
        if 'Start CKPT' in linha:
            start = i
        if 'End CKPT' in linha:
            end = True
    return start, end


main()

