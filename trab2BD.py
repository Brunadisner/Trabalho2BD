import sys
import re
import psycopg2
from beautifultable import BeautifulTable


commit  = []
trAberta = []
# def conexao_BD():
#     #função que faz a conexão com o banco de dados
#     try:
#         conexao = psycopg2.connect(database= 'teste',
#                                     host = 'localhost',
#                                     user = 'postgres',
#                                     password = 'root')
#         print("Conectou")
#         return conexao     
#     except psycopg2.DatabaseError as e:
#         print("Erro ao conectar o banco:", e)
#         return None

# con = conexao_BD()


def main():
    arquivo = sys.argv[1]
    file = open(arquivo, "r")

    arquivo_linhas = [] 
    endCKPT = []
    for i in file:
        arquivo_linhas.append(i)
    
    arquivo_linhas = limpar(arquivo_linhas)

    # create_db()
    linha = 0
    for i in range(len(arquivo_linhas)):
        if arquivo_linhas[i] == '':
            linha = i
            inserir_banco(arquivo_linhas, i)
            break
    
    arquivo_linhas = arquivo_linhas[linha+1::]
    #chama a função para buscar o endCKPT e StartCKPT

    startC, endC = busca_ckpt(arquivo_linhas)

    if endC == False:
        print("Checkpoint não foi finalizado")
        linhaCKPT = arquivo_linhas
        redo(arquivo_linhas, linhaCKPT, endCKPT,startC)
    else:
        linhaCKPT = arquivo_linhas[startC::]
        redo(arquivo_linhas, linhaCKPT, endC, startC)

    
    for i in trAberta:
        if i in commit:
            print('Transação', i, 'fez REDO')
        else:
            print('Transação', i,  'não fez REDO')

    imprimir_variaveis()
    


# limpando as linhas para iniciar a leitura do arquivo
def limpar(arquivo_linhas):
    for linha in range(len(arquivo_linhas)):
        arquivo_linhas[linha] = re.sub('\n','',arquivo_linhas[linha])
        arquivo_linhas[linha] = re.sub('<','',arquivo_linhas[linha])
        arquivo_linhas[linha] = re.sub('>','',arquivo_linhas[linha])

    return arquivo_linhas

#função para inserir as tuplas na tabela
def create_db():
    cur = con.cursor()
    cur.execute("drop table tabela")
    cur.execute("CREATE TABLE tabela (id int not null, A int, B int, primary key(id))")
    con.commit()
    print("Tabela criada \n")

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
        t = cur.fetchall()
        if t:
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
    t = cur.fetchall()
    print(t)


def busca_ckpt(arquivo_linhas):
    start = 0
    end  = False

    for i, linha in enumerate(arquivo_linhas):
        if 'Start CKPT' in linha:
            start = i
        if 'End CKPT' in linha:
            end = True
    return start, end



def redo(arquivo_linhas, linhaCKPT, endCKPT, StartC = 0):
    
# vai percorrer ao contrário as linhas que tem CKPT verificando o que tem commit 
    for i in range(len(linhaCKPT)-1,0,-1):
        # se tem commit adiciona na lista de commit
        if 'commit' in linhaCKPT[i]:
            commit.append(linhaCKPT[i].split()[1])
        # se encontra start então adiciona na lista de transições abertas
        if 'start' in linhaCKPT[i]:
            trAberta.append(linhaCKPT[i].split()[1])
    
    # se o endCKPT é true quer dizer que tem transação em aberto 
    if endCKPT == True:
        res = re.findall(r'\(.*?\)', arquivo_linhas[StartC])
        res = "".join([n for n in res[0] if n != '(' and n !=')'])
        print(res)
        [trAberta.append(n) for n in res.split(',')]
    
    commit.reverse()

def verificar_valores(arquivo_linhas, i):
    for linha in arquivo_linhas:
        if i in linha and 'start' not in linha and 'commit' not in linha and 'CKPT' not in linha:
             quebra = linha.split(',')
             sql = " select * from tabela where id = {}".format(quebra[1])
             cur.execute(sql)
             t = cur.fetchall()
             if t:
                sql = "update tabela set {} = {} where id {}".format(quebra[0], quebra[2], quebra[1])
                cur.execute(sql)
             else:
                sql = "insert into tabela (id, {}) values ({}, {})".format(quebra[0], quebra[1], quebra[2])
                cur.execute(sql)
                

main()

