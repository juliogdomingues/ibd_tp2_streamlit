import pandas as pd
import streamlit as st
import sqlite3
import io
import git
import os

st.title('Trabalho Prático 2')
st.header('Introdução a Banco de Dados')
st.subheader('Departamento de Ciência da Computação')
st.subheader('Universidade Federal de Minas Gerais')

# Função para verificar e atualizar o banco de dados a partir do repositório Git
def verificar_e_atualizar_db():
    repo_path = './Data_IBD'
    repo_url = 'https://github.com/souza-marcos/Data_IBD.git'
    db_file = os.path.join(repo_path, 'database.db')

    # Clonar ou puxar as atualizações do repositório se ele já existir
    if not os.path.exists(repo_path):
        git.Repo.clone_from(repo_url, repo_path)
    else:
        repo = git.Repo(repo_path)
        origin = repo.remotes.origin
        origin.pull()

    # Carregar os dados se o arquivo do banco de dados não existir
    if not os.path.exists(db_file):
        conn = sqlite3.connect(db_file)
        cursor = conn.cursor()
        with io.open(os.path.join(repo_path, 'dump.sql'), 'r', encoding='utf-8') as f:
            dump = f.read()
            cursor.executescript(dump)
        conn.close()

# Verificar e atualizar o banco de dados automaticamente
verificar_e_atualizar_db()

# Carrega e exibe o diagrama ER
st.image('modeloer.png', caption='Diagrama ER', use_column_width=True)

# Carrega e exibe o diagrama relacional
st.image('relacional.png', caption='Diagrama Relacional', use_column_width=True)

# Estabelece a conexão com o banco de dados
conn = sqlite3.connect('database.db')

# Funções para as consultas pré-definidas
def consulta_refinarias_petrobras():
    query = '''
    SELECT Instalacao, CNPJ
    FROM Refinaria
    WHERE RazaoSocial = 'PETROLEO BRASILEIRO S/A'
    '''
    return pd.read_sql_query(query, conn)

def empresas_ociosas():
    query = '''
    SELECT CNPJ, Data
    FROM Processamento
    WHERE Volume = 0 and strftime('%Y', Data) >= '2023';
    '''
    return pd.read_sql_query(query, conn)

def consulta_variacao_capacidade():
    query = '''
    SELECT
        A1.CNPJ,
        A2.DataConcessaoAnterior,
        A1.DataConcessao AS DataConcessaoAtual,
        A2.UltimaCapacidade,
        A1.Capacidade AS CapacidadeAtual,
        ((A1.Capacidade - A2.UltimaCapacidade) * 100 / A2.UltimaCapacidade) AS CrescimentoPercentual
    FROM Autorizacao A1
    INNER JOIN (
        SELECT
            AutoID AS A2_AutoID,
            CNPJ AS A2_CNPJ,
            DataConcessao AS DataConcessaoAnterior,
            Capacidade AS UltimaCapacidade
        FROM Autorizacao
    ) A2 ON A1.CNPJ = A2.A2_CNPJ AND A1.DataConcessao > A2.DataConcessaoAnterior
    ORDER BY A1.CNPJ, A2.DataConcessaoAnterior DESC;
    '''
    return pd.read_sql_query(query, conn)

def consulta_empresas_por_estado():
    query = '''
    SELECT E.Estado, COUNT(EmpID) AS "Numero de Empresas"
    FROM Estado E NATURAL JOIN Empresa
    GROUP BY E.Estado
    ORDER BY "Numero de Empresas" DESC;
    '''
    return pd.read_sql_query(query, conn)

def refinarias_maior_media():
    query = '''
    SELECT Instalacao, Data, Volume
    FROM Refinaria NATURAL JOIN (SELECT CNPJ, Data, Volume
								             FROM Processamento AS P1
								             WHERE strftime('%Y', P1.Data) >= '2023' AND P1.Volume > (SELECT AVG(P2.Volume)
                                                         								              FROM Processamento AS P2
                                                         								              WHERE strftime('%Y', P2.Data) >= '2023'
                                                         								              GROUP BY P2.CNPJ
                                                        								              HAVING P1.CNPJ =  P2.CNPJ));
    '''
    return pd.read_sql_query(query, conn)

def consulta_producao_glp():
    query = '''
    SELECT
        p.Nome AS Produto,
        p.Unidade AS Unidade,
        e.Estado,
        SUM(pr.Quantidade) AS ProducaoTotal
    FROM Producao pr
    JOIN Produto p ON pr.ProdID = p.ProdID
    JOIN Empresa em ON pr.EmpID = em.EmpID
    JOIN Estado e ON em.EstadoID = e.EstadoID
    WHERE p.Nome = 'GLP ' AND strftime('%Y', pr.Data) BETWEEN '2019' AND '2023'
    GROUP BY p.Nome, e.Estado, p.Unidade
    ORDER BY ProducaoTotal DESC;
    '''
    return pd.read_sql_query(query, conn)

def consulta_producao_gasolina_macrorregiao():
    query = '''
    SELECT es.Regiao, SUM(pr.Quantidade) AS ProducaoTotalGasolinaA
    FROM Producao pr
    JOIN Produto p ON pr.ProdID = p.ProdID
    JOIN Empresa em ON pr.EmpID = em.EmpID
    JOIN Estado es ON em.EstadoID = es.EstadoID
    WHERE p.Nome = 'Gasolina A '
    GROUP BY es.Regiao;
    '''
    return pd.read_sql_query(query, conn)

def consulta_producao_por_empresa():
    query = '''
    SELECT
        em.Nome AS Empresa,
        p.Nome AS Produto,
        p.Unidade AS UnidadeDeMedida,
        SUM(pr.Quantidade) AS QuantidadeTotal
    FROM Producao pr
    JOIN Produto p ON pr.ProdID = p.ProdID
    JOIN Empresa em ON pr.EmpID = em.EmpID
    GROUP BY em.Nome, p.Nome, p.Unidade;
    '''
    return pd.read_sql_query(query, conn)

def maior_produtor():
    query = '''
    SELECT Nome AS NomeProduto, NomeEmpresa AS MaiorProdutor2023, Regiao
    FROM (SELECT IDProd, EmpID, NomeEmpresa, MAX(QuantidadeProduzida2023), Regiao
			FROM (SELECT ProdID AS IDProd, EmpID, Nome AS NomeEmpresa, SUM(Quantidade) AS QuantidadeProduzida2023, Regiao
						FROM Producao NATURAL JOIN Empresa NATURAL JOIN Estado
						WHERE strftime('%Y', Data) >= '2023'
						GROUP BY ProdID, EmpID)
			GROUP BY IDProd
			ORDER BY IDProd) JOIN Produto ON IDProd = ProdID;
    '''
    return pd.read_sql_query(query, conn)

def consulta_refinarias_gasolina():
    query = '''
    SELECT
        r.Instalacao AS Refinaria,
        SUM(pr.Quantidade) AS QuantidadeTotalGasolinaA
    FROM Producao pr
    JOIN Produto p ON pr.ProdID = p.ProdID
    JOIN Refinaria r ON pr.EmpID = r.EmpID
    WHERE p.Nome = 'Gasolina A '
    GROUP BY r.Instalacao
    ORDER BY QuantidadeTotalGasolinaA DESC;
    '''
    return pd.read_sql_query(query, conn)

# Interface Streamlit
st.title("Consultas SQL")

# Botões para as consultas pré-definidas
if st.button('Consulta 1: Refinarias da Petrobras'):
    df = consulta_refinarias_petrobras()
    st.write(df)

if st.button('Consulta 2: Empresas ociosas'):
    df = empresas_ociosas()
    st.write(df)

if st.button('Consulta 3: Variação da Capacidade Autorizada'):
    df = consulta_variacao_capacidade()
    st.write(df)

if st.button('Consulta 4: Número de Empresas por Estado'):
    df = consulta_empresas_por_estado()
    st.write(df)
    st.image('consulta4.png', caption='Número de Empresas por Estado')

if st.button('Consulta 5: Meses em que refinarias processaram volume maior que a própria média anual (2023)'):
    df = refinarias_maior_media()
    st.write(df)

if st.button('Consulta 6: Produção de GLP por Estado (2019-2023)'):
    df = consulta_producao_glp()
    st.write(df)
    st.image('consulta6.png', caption='Produção de GLP por Estado (2019-2023)')

if st.button('Consulta 7: Produção de Gasolina A por Macrorregião'):
    df = consulta_producao_gasolina_macrorregiao()
    st.write(df)
    st.image('consulta7.png', caption='Produção de Gasolina A por Macrorregião')

if st.button('Consulta 8: Produção Total por Empresa'):
    df = consulta_producao_por_empresa()
    st.write(df)

if st.button('Consulta 9: Empresa que mais produziu cada tipo de produto em 2023'):
    df = maior_produtor()
    st.write(df)

if st.button('Consulta 10: Refinarias com Maior Produção de Gasolina A'):
    df = consulta_refinarias_gasolina()
    st.write(df)
    st.image('consulta10.png', caption='Refinarias com Maior Produção de Gasolina A')

# Campo de texto e botão para a consulta personalizada
user_query = st.text_area("Digite sua consulta SQL aqui:")
if st.button('Executar Consulta Personalizada'):
    try:
        df = pd.read_sql_query(user_query, conn)
        st.write(df)
    except Exception as e:
        st.error(f"Erro ao executar a consulta: {e}")

# Fechar a conexão com o banco de dados
conn.close()


st.markdown('---')  # Adiciona uma linha horizontal para separar o conteúdo
st.markdown("""
#### Grupo:
- Gustavo Chaves Ferreira
- Júlio Guerra Domingues
- Manuel Junio Ferraz Cardoso
- Marcos Daniel Souza Netto

#### Professor:
- Rodrygo Luis Teodoro Santos
""", unsafe_allow_html=True)
