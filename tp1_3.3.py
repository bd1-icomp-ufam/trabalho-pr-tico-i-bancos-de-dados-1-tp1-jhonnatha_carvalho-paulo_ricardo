import psycopg2
from psycopg2 import sql
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
import streamlit as st

# Carregar as configurações do banco de dados
def load_config():
    config = {
        'host': 'localhost',
        'database': 'tp1',
        'user': 'postgres',
        'password': 'senha0405'
    }
    return config

# Conectar ao banco de dados PostgreSQL
def connect(config):
    try:
        conn = psycopg2.connect(**config)
        return conn
    except (psycopg2.DatabaseError, Exception) as error:
        st.error(f"Erro ao conectar ao banco de dados: {error}")
        return None

# Função para formatar datas no formato dd/mm/yyyy
def format_date(date):
    return date.strftime("%d/%m/%Y") if date else "N/A"

# Função para formatar números decimais com duas casas
def format_decimal(number):
    return f"{number:.2f}"

# Listar os 5 comentários mais úteis com maior e menor avaliação
def listar_comentarios_produto(product_id):
    config = load_config()
    conn = connect(config)
    if not conn:
        return
    cur = conn.cursor()

    query = """
    WITH TopHelpfulReviews AS (
        SELECT r.ReviewID, r.CustomerID, r.Rating, r.HelpfulVotes, r.ReviewDate
        FROM Review r
        WHERE r.ProductID = %s
        ORDER BY r.HelpfulVotes DESC
        LIMIT 10
    )
    (
        SELECT * FROM TopHelpfulReviews
        ORDER BY Rating DESC, HelpfulVotes DESC
    );
    """
    cur.execute(query, (product_id,))
    comentarios = cur.fetchall()
    cur.close()
    conn.close()

    comentarios_ordenados = sorted([comentario for comentario in comentarios if comentario[2]], key=lambda x: (-x[2], -x[3]))

    melhores_comentarios = comentarios_ordenados[:5]
    piores_comentarios = comentarios_ordenados[-5:]
    
    melhores_formatados = [
        (review_id, customer_id, format_decimal(rating), helpful_votes, format_date(review_date))
        for (review_id, customer_id, rating, helpful_votes, review_date) in melhores_comentarios
    ]
    
    piores_formatados = [
        (review_id, customer_id, format_decimal(rating), helpful_votes, format_date(review_date))
        for (review_id, customer_id, rating, helpful_votes, review_date) in piores_comentarios
    ]
    piores_formatados = sorted(piores_formatados, key=lambda x: x[3], reverse=True)

    st.write("### Comentários Melhor Avaliados")
    st.write(pd.DataFrame(melhores_formatados, columns=["ReviewID", "CustomerID", "Rating", "HelpfulVotes", "ReviewDate"]))

    st.write("### Comentários Pior Avaliados")
    st.write(pd.DataFrame(piores_formatados, columns=["ReviewID", "CustomerID", "Rating", "HelpfulVotes", "ReviewDate"]))

# Listar os produtos similares com maiores vendas
def listar_similares_maior_venda(product_id):
    config = load_config()
    conn = connect(config)
    if not conn:
        return
    cur = conn.cursor()

    query = """
    SELECT p.ProductID, p.Title, p.SalesRank
    FROM Produto p
    JOIN ProdutoSimilar ps ON p.ProductID = ps.SimilarProductID
    WHERE ps.ProductID = %s
    AND p.SalesRank < (
        SELECT SalesRank FROM Produto WHERE ProductID = %s
    )
    ORDER BY p.SalesRank DESC;
    """
    cur.execute(query, (product_id, product_id))
    similares = cur.fetchall()

    query = """
    SELECT p.ProductID, p.Title, p.SalesRank
    FROM Produto p
    WHERE p.ProductID = %s;
    """
    cur.execute(query, (product_id,))
    produto = cur.fetchall()
    cur.close()
    conn.close()
    
    st.write("### Produto Base")
    st.write(pd.DataFrame(produto, columns=["ProductID", "Title", "SalesRank"]))
    
    st.write("### Produtos Similares com Maior Venda")
    st.write(pd.DataFrame(similares, columns=["ProductID", "Title", "SalesRank"]))

# Mostrar a evolução das médias de avaliação
def evolucao_media_avaliacao(product_id):
    config = load_config()
    conn = connect(config)
    if not conn:
        return
    cur = conn.cursor()
    
    query = """
    SELECT r.ReviewDate, AVG(r.Rating) AS avg_rating
    FROM Review r
    WHERE r.ProductID = %s
    GROUP BY r.ReviewDate
    ORDER BY r.ReviewDate;
    """
    cur.execute(query, (product_id,))
    evolucao = cur.fetchall()
    cur.close()
    conn.close()
    
    data = {
        'ReviewDate': [review_date for review_date, _ in evolucao],
        'AvgRating': [float(avg_rating) for _, avg_rating in evolucao]
    }
    
    df = pd.DataFrame(data)
    df['ReviewDate'] = pd.to_datetime(df['ReviewDate'])

    st.write("### Evolução da Média de Avaliações")
    st.line_chart(df.set_index('ReviewDate'))

    # Exibindo a tabela abaixo do gráfico
    st.write("### Tabela de Evolução da Média de Avaliações Diárias")
    st.write(df)
    
# Listar os 10 produtos líderes de venda por grupo
def listar_lideres_venda_por_grupo():
    config = load_config()
    conn = connect(config)
    if not conn:
        return
    cur = conn.cursor()
    
    query = """
    WITH RankedProducts AS (
        SELECT 
            ProductGroup, 
            ProductID, 
            Title, 
            SalesRank,
            ROW_NUMBER() OVER (PARTITION BY ProductGroup ORDER BY SalesRank ASC) AS rank
        FROM Produto
        WHERE SalesRank > 0
    )
    SELECT 
        ProductGroup, 
        ProductID, 
        Title, 
        SalesRank
    FROM RankedProducts
    WHERE rank <= 10
    ORDER BY ProductGroup, SalesRank;
    """
    cur.execute(query)
    lideres = cur.fetchall()
    cur.close()
    conn.close()
    
    st.write("### Produtos Líderes de Venda por Grupo")
    st.write(pd.DataFrame(lideres, columns=["ProductGroup", "ProductID", "Title", "SalesRank"]))

# Listar os 10 produtos com a maior média de avaliações úteis
def listar_produtos_melhores_avaliacoes():
    config = load_config()
    conn = connect(config)
    if not conn:
        return
    cur = conn.cursor()
    
    query = """
    SELECT P.ProductID, P.Title, AVG(R.HelpfulVotes) AS AvgHelpfulVotes
    FROM Produto P
    JOIN Review R ON P.ProductID = R.ProductID
    GROUP BY P.ProductID, P.Title
    ORDER BY AvgHelpfulVotes DESC
    LIMIT 10;
    """
    cur.execute(query)
    melhores_avaliacoes = cur.fetchall()
    cur.close()
    conn.close()
    
    st.write("### Produtos com Maior Média de Avaliações Úteis")
    st.write(pd.DataFrame(melhores_avaliacoes, columns=["ProductID", "Title", "AvgHelpfulVotes"]))

# Listar as 5 categorias com a maior média de avaliações úteis
def listar_melhores_categorias():
    config = load_config()
    conn = connect(config)
    if not conn:
        return
    cur = conn.cursor()
    
    query = """
    WITH RankedCategories AS (
    SELECT 
        C.CategoryID,                                       
        C.CategoryName, 
        AVG(R.HelpfulVotes) AS AvgHelpfulVotes,
        ROW_NUMBER() OVER (PARTITION BY C.CategoryName ORDER BY AVG(R.HelpfulVotes) DESC) AS rn
    FROM 
        Categoria C
    JOIN 
        CategoriaProduto CP ON C.CategoryID = CP.CategoryID
    JOIN 
        Review R ON CP.ProductID = R.ProductID
    GROUP BY 
        C.CategoryID, C.CategoryName
    )
    SELECT 
        CategoryID, 
        CategoryName, 
        AvgHelpfulVotes
    FROM 
        RankedCategories
    WHERE 
        rn = 1
    ORDER BY 
        AvgHelpfulVotes DESC
    LIMIT 5;
    """
    cur.execute(query)
    melhores_categorias = cur.fetchall()
    cur.close()
    conn.close()
    
    st.write("### Categorias com Maior Média de Avaliações Úteis")
    st.write(pd.DataFrame(melhores_categorias, columns=["CategoryID", "CategoryName", "AvgHelpfulVotes"]))

# Listar os 10 clientes que mais fizeram comentários por grupo
def listar_clientes_por_grupo():
    config = load_config()
    conn = connect(config)
    if not conn:
        return
    cur = conn.cursor()
    
    query = """
    WITH RankedReviews AS (
        SELECT 
            P.ProductGroup, 
            R.CustomerID, 
            COUNT(R.ReviewID) AS TotalReviews,
            ROW_NUMBER() OVER (PARTITION BY P.ProductGroup ORDER BY COUNT(R.ReviewID) DESC) AS rank
        FROM Review R
        JOIN Produto P ON R.ProductID = P.ProductID
        GROUP BY P.ProductGroup, R.CustomerID
    )
    SELECT 
        ProductGroup, 
        CustomerID, 
        TotalReviews
    FROM RankedReviews
    WHERE rank <= 10
    ORDER BY ProductGroup, TotalReviews DESC;
    """
    cur.execute(query)
    clientes = cur.fetchall()
    cur.close()
    conn.close()
    
    st.write("### Clientes que Mais Fizeram Comentários por Grupo")
    st.write(pd.DataFrame(clientes, columns=["ProductGroup", "CustomerID", "TotalReviews"]))

# Função principal
def main():
    st.title("Dashboard de Análise de Produtos")

    # Seleção da funcionalidade
    opcoes = [
        "Comentários mais úteis de um produto",
        "Produtos similares com maiores vendas",
        "Evolução da média de avaliações",
        "Produtos líderes de venda por grupo",
        "Produtos com maior média de avaliações úteis",
        "Categorias com maior média de avaliações úteis",
        "Clientes que mais fizeram comentários por grupo"
    ]
    escolha = st.selectbox("Escolha uma funcionalidade:", opcoes)

    # ID do produto input
    if escolha in [
        "Comentários mais úteis de um produto",
        "Produtos similares com maiores vendas",
        "Evolução da média de avaliações"
    ]:
        product_id = st.text_input("Digite o ID do produto:", "")

    # Chama a função correspondente com base na escolha
    if escolha == "Comentários mais úteis de um produto":
        if product_id:
            listar_comentarios_produto(product_id)
    elif escolha == "Produtos similares com maiores vendas":
        if product_id:
            listar_similares_maior_venda(product_id)
    elif escolha == "Evolução da média de avaliações":
        if product_id:
            evolucao_media_avaliacao(product_id)
    elif escolha == "Produtos líderes de venda por grupo":
        listar_lideres_venda_por_grupo()
    elif escolha == "Produtos com maior média de avaliações úteis":
        listar_produtos_melhores_avaliacoes()
    elif escolha == "Categorias com maior média de avaliações úteis":
        listar_melhores_categorias()
    elif escolha == "Clientes que mais fizeram comentários por grupo":
        listar_clientes_por_grupo()

if __name__ == "__main__":
    main()
