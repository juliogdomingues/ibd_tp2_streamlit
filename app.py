import pandas as pd
import streamlit as st
import sqlite3
import io
import git
import os

# Função para clonar o repositório Git e carregar os dados
def clonar_e_carregar():
    # Caminho do diretório onde o repositório será clonado
    repo_path = './Data_IBD'
    
    # URL do repositório Git
    repo_url = 'https://github.com/souza-marcos/Data_IBD.git'

    # Clonar o repositório se ele ainda não existir
    if not os.path.exists(repo_path):
        git.Repo.clone_from(repo_url, repo_path)

    # Carregar os dados
    conn = sqlite3.connect('database.db')
    cursor = conn.cursor()

    # Carregamento do dump
    with io.open(os.path.join(repo_path, 'dump.sql'), 'r', encoding='utf-8') as f:
        dump = f.read()
        cursor.executescript(dump)

    conn.close()

# Botão no Streamlit para clonar o repositório e carregar os dados
if st.button('Clonar Repositório e Carregar Dados'):
    clonar_e_carregar()
    st.success('Repositório Clonado e Dados Carregados com Sucesso!')


# Carrega e exibe o diagrama ER
st.image('modeloer.png', caption='Diagrama ER', use_column_width=True)

# Carrega e exibe o diagrama relacional
st.image('relacional.jpeg', caption='Diagrama Relacional', use_column_width=True)

# Função para carregar e mostrar a imagem do Diagrama ER
def mostrar_diagrama_er():
    image_er = st.file_uploader("Carregar Diagrama ER", type=["png", "jpg", "jpeg"])
    if image_er is not None:
        st.image(image_er, caption='Diagrama ER')

# Função para carregar e mostrar a imagem do Diagrama Relacional
def mostrar_diagrama_relacional():
    image_relacional = st.file_uploader("Carregar Diagrama Relacional", type=["png", "jpg", "jpeg"])
    if image_relacional is not None:
        st.image(image_relacional, caption='Diagrama Relacional')

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

def consulta_produtores_independentes():
    query = '''
    SELECT EmpID, Nome
    FROM Empresa
    WHERE ProdutorIndependente = 1
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

def consulta_producao_nafta_ano():
    query = '''
    SELECT
        EmpID, Nome,
        Ano,
        ProducaoTotal
    FROM (
        SELECT
            em.EmpID, em.Nome,
            strftime('%Y', pr.Data) AS Ano,
            SUM(pr.Quantidade) AS ProducaoTotal
        FROM Producao pr
        JOIN Empresa em ON pr.EmpID = em.EmpID
        WHERE ProdID = 1
        GROUP BY em.EmpID, Ano
    ) AS ProducaoPorAno
    JOIN (
        SELECT
            Ano AS MaxAno,
            MAX(ProducaoTotal) AS MaxProducaoTotal
        FROM (
            SELECT
                strftime('%Y', pr.Data) AS Ano,
                SUM(pr.Quantidade) AS ProducaoTotal
            FROM Producao pr
            WHERE ProdID = 1
            GROUP BY Ano
        ) AS ProducaoPorAnoMax
        GROUP BY Ano
    ) AS MaxProducaoPorAno ON ProducaoPorAno.Ano = MaxProducaoPorAno.MaxAno
        AND ProducaoPorAno.ProducaoTotal = MaxProducaoPorAno.MaxProducaoTotal
    ORDER BY Ano;
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
    GROUP BY p.Nome, e.Estado, p.Unidade;
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

def consulta_estados_refinarias():
    query = '''
    SELECT e.Estado, COUNT(r.EmpID) AS NumeroDeRefinarias
    FROM Refinaria r
    JOIN Empresa em ON r.EmpID = em.EmpID
    JOIN Estado e ON em.EstadoID = e.EstadoID
    GROUP BY e.Estado
    ORDER BY NumeroDeRefinarias DESC;
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
if st.button('Refinarias da Petrobras'):
    df = consulta_refinarias_petrobras()
    st.write(df)

if st.button('Produtores Independentes'):
    df = consulta_produtores_independentes()
    st.write(df)

# ... Continuação do código anterior ...

if st.button('Variação da Capacidade Autorizada'):
    df = consulta_variacao_capacidade()
    st.write(df)

if st.button('Produção de Nafta por Ano'):
    df = consulta_producao_nafta_ano()
    st.write(df)

if st.button('Número de Empresas por Estado'):
    df = consulta_empresas_por_estado()
    st.write(df)

if st.button('Produção de GLP por Estado (2019-2023)'):
    df = consulta_producao_glp()
    st.write(df)

if st.button('Produção de Gasolina A por Macrorregião'):
    df = consulta_producao_gasolina_macrorregiao()
    st.write(df)

if st.button('Produção Total por Empresa'):
    df = consulta_producao_por_empresa()
    st.write(df)

if st.button('Estados com Maior Número de Refinarias'):
    df = consulta_estados_refinarias()
    st.write(df)

if st.button('Refinarias com Maior Produção de Gasolina A'):
    df = consulta_refinarias_gasolina()
    st.write(df)

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

