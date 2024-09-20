import psycopg2
from psycopg2 import sql
from datetime import datetime
import os

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
        with psycopg2.connect(**config) as conn:
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print("Erro", str(error))

# Função para formatar datas no formato dd/mm/yyyy
def format_date(date):
    return date.strftime("%d/%m/%Y") if date else "N/A"

# Função para formatar números decimais com duas casas
def format_decimal(number):
    return f"{number:.2f}"



# Listar os 5 comentários mais úteis com maior e menor avaliação

# Função para listar os 5 melhores e os 5 piores comentários mais úteis
def listar_comentarios_produto(product_id):
    print("\nCarregando comentários mais úteis...\n")
    config = load_config()
    conn = connect(config)
    cur = conn.cursor()

    # Subquery para pegar os 10 comentários mais úteis (HelpfulVotes)
    query = """
    WITH TopHelpfulReviews AS (
        SELECT r.ReviewID, r.CustomerID, r.Rating, r.HelpfulVotes, r.ReviewDate
        FROM Review r
        WHERE r.ProductID = %s
        ORDER BY r.HelpfulVotes DESC
        LIMIT 10
    )
    (
        -- Selecionando os 5 com maior rating
        SELECT * FROM TopHelpfulReviews
        ORDER BY Rating DESC, HelpfulVotes DESC
    );
    """

    # Executando a consulta com o ProductID fornecido
    cur.execute(query, (product_id,))
    comentarios = cur.fetchall()
    cur.close()
    conn.close()

    # Ordenar os comentários pelos critérios especificados
    comentarios_ordenados = sorted([comentario for comentario in comentarios if comentario[2]], key=lambda x: (-x[2], -x[3]))

    # Pegar os 5 primeiros e os 5 últimos comentários
    melhores_comentarios = comentarios_ordenados[:5]
    piores_comentarios = comentarios_ordenados[-5:]
    
    # Formatar as datas e exibir resultados
    melhores_formatados = [
        (review_id, customer_id, format_decimal(rating), helpful_votes, format_date(review_date))
        for (review_id, customer_id, rating, helpful_votes, review_date) in melhores_comentarios
    ]
    
    piores_formatados = [
        (review_id, customer_id, format_decimal(rating), helpful_votes, format_date(review_date))
        for (review_id, customer_id, rating, helpful_votes, review_date) in piores_comentarios
    ]
    piores_formatados = sorted(piores_formatados, key=lambda x: x[3], reverse=True)

    # Exibir os melhores comentários
    print("\nMelhor avaliados:")
    print("ReviewID | CustomerID | Rating | HelpfulVotes | ReviewDate")
    print("-" * 60)
    for comentario in melhores_formatados:
        print(f"{comentario[0]} | {comentario[1]} | {comentario[2]} | {comentario[3]} | {comentario[4]}")

    # Exibir os piores comentários
    print("\nPior avaliados:")
    print("ReviewID | CustomerID | Rating | HelpfulVotes | ReviewDate")
    print("-" * 60)
    for comentario in piores_formatados:
        print(f"{comentario[0]} | {comentario[1]} | {comentario[2]} | {comentario[3]} | {comentario[4]}")
    print("\n")
    input("Pressione enter para continuar...\n")


# Listar os produtos similares com maiores vendas
def listar_similares_maior_venda(product_id):
    print("\nCarregando produtos similares com maiores vendas...")
    config = load_config()
    conn = connect(config)
    cur = conn.cursor()
    
    # Consulta para listar produtos similares com maior SalesRank
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

    # Consulta para buscar o produto base
    query = """
    SELECT p.ProductID, p.Title, p.SalesRank
    FROM Produto p
    WHERE p.ProductID = %s;
    """
    cur.execute(query, (product_id,))
    produto = cur.fetchall()

    cur.close()
    conn.close()
    
    # Formatar os resultados
    similares_formatados = [
        (product_id, title, sales_rank)
        for (product_id, title, sales_rank) in similares
    ]
    
    produto_formatado = [
        (product_id, title, sales_rank)
        for (product_id, title, sales_rank) in produto
    ]

    # Exibir o produto base
    print("\nProduto Base:")
    print("ProductID | Title | SalesRank")
    print("-" * 60)
    for produto in produto_formatado:
        print(f"{produto[0]} | {produto[1]} | {produto[2]}")

    # Exibir os produtos similares com maior venda
    print("\nProdutos Similares com Maior Venda:")
    print("ProductID | Title | SalesRank")
    print("-" * 60)
    for similar in similares_formatados:
        print(f"{similar[0]} | {similar[1]} | {similar[2]}")

    print("\n")
    input("Pressione enter para continuar...\n")

# Mostrar a evolução das médias de avaliação
def evolucao_media_avaliacao(product_id):
    print("\nCarregando evolução da média de avaliação...\n")
    config = load_config()
    conn = connect(config)
    cur = conn.cursor()
    
    # Consulta para calcular a média de avaliação por data
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
    
    # Formatar datas e números decimais
    evolucao_formatada = [
        (format_date(review_date), format_decimal(avg_rating))
        for (review_date, avg_rating) in evolucao
    ]
    
    # Exibir os resultados
    print("\nEvolução da Média de Avaliação:")
    print("ReviewDate | AvgRating")
    print("-" * 40)
    for data, media in evolucao_formatada:
        print(f"{data} | {media}")
    print("\n")
    input("Pressione enter para continuar...\n")


# Listar os 10 produtos líderes de venda por grupo
def listar_lideres_venda_por_grupo():
    print("\nCarregando lista de líderes de venda por grupo...\n")
    config = load_config()
    conn = connect(config)
    cur = conn.cursor()
    
    # Consulta SQL para listar os 10 produtos líderes de venda por grupo
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
    
    # Formatar os resultados
    lideres_formatados = [
        (
            product_group, 
            product_id, 
            title, 
            (sales_rank) if sales_rank is not None else "N/A"
        )
        for (product_group, product_id, title, sales_rank) in lideres
    ]
    
    # Exibir os resultados no terminal
    print("\nLíderes de Venda por Grupo:")
    print("ProductGroup | ProductID | Title | SalesRank")
    print("-" * 50)
    for produto in lideres_formatados:
        print(f"{produto[0]} | {produto[1]} | {produto[2]} | {produto[3]}")
    print("\n")
    input("Pressione enter para continuar...\n")

# Listar as 5 categorias com a maior média de avaliações úteis
def listar_melhores_categorias():
    print("\nCarregando lista de melhores categorias...\n")
    config = load_config()
    conn = connect(config)
    cur = conn.cursor()
    
    # Consulta SQL para listar as melhores categorias
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
    
    # Formatar os resultados
    melhores_categorias_formatadas = [
        (category_name, format_decimal(avg_helpful_votes))
        for (_, category_name, avg_helpful_votes) in melhores_categorias
    ]
    
    # Exibir os resultados no terminal
    print("\nMelhores Categorias:")
    print("CategoryName | AvgHelpfulVotes")
    print("-" * 40)
    for categoria in melhores_categorias_formatadas:
        print(f"{categoria[0]} | {categoria[1]}")
    print("\n")
    input("Pressione enter para continuar...\n")


# Listar os 10 clientes que mais fizeram comentários por grupo
def listar_clientes_por_grupo():
    print("\nCarregando lista de clientes por grupo...\n")
    config = load_config()
    conn = connect(config)
    cur = conn.cursor()
    
    # Consulta SQL para listar clientes por grupo de produtos
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
    SELECT ProductGroup, CustomerID, TotalReviews
    FROM RankedReviews
    WHERE rank <= 10
    ORDER BY ProductGroup, TotalReviews DESC;
    """
    cur.execute(query)
    clientes = cur.fetchall()
    cur.close()
    conn.close()
    
    # Exibir os resultados no terminal
    print("\nClientes por Grupo de Produtos:")
    print("ProductGroup | CustomerID | ReviewCount")
    print("-" * 40)
    for cliente in clientes:
        print(f"{cliente[0]} | {cliente[1]} | {cliente[2]}")

    print("\n")
    input("Pressione enter para continuar...\n")

def listar_produtos_melhores_avaliacoes():
    print("\nCarregando lista de produtos com melhores avaliações...\n")
    config = load_config()
    conn = connect(config)
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
    
    # Formatar números decimais e exibir resultados
    print("\nMelhores Avaliações:")
    print("ProductID | Title | AvgHelpfulVotes")
    print("-" * 50)
    
    for (product_id, title, avg_helpful_votes) in melhores_avaliacoes:
        print(f"{product_id} | {title} | {format_decimal(avg_helpful_votes)}")
    print("\n")
    input("Pressione enter para continuar...\n")


def main():
    
    
    while True:
        os.system('clear')
        print("Selecione a função:")
        choices = [
            'Listar comentários mais úteis',
            'Listar Produtos Similares com Maior Venda',
            'Evolução da Avaliação',
            'Listar os 10 produtos líderes de venda em cada grupo de produtos',
            'Listar os 10 produtos com a maior média de avaliações úteis positivas por produto',
            'Listar as 5 categorias de produto com a maior média de avaliações úteis positivas por produto',
            'Listar os 10 clientes que mais fizeram comentários por grupo de produto'
        ]
        
        for i, choice in enumerate(choices, 1):
            print(f"{i}. {choice}")
        try:
            choice = int(input("Escolha uma opção (ou -1 para encerrar): "))
            if choice == -1:
                break
            elif choice == 1:
                product_id = input("Digite o ID do produto: ")
                listar_comentarios_produto(product_id)
            elif choice == 2:
                product_id = input("Digite o ID do produto: ")
                listar_similares_maior_venda(product_id)
            elif choice == 3:
                product_id = input("Digite o ID do produto: ")
                evolucao_media_avaliacao(product_id)
            elif choice == 4:
                listar_lideres_venda_por_grupo()
            elif choice == 5:
                listar_produtos_melhores_avaliacoes()
            elif choice == 6:
                listar_melhores_categorias()
            elif choice == 7:
                listar_clientes_por_grupo()
            else:
                print("Opção inválida. Tente novamente.")
        except ValueError:
            print("Entrada inválida. Por favor, insira um número.")

    print("Encerrando o programa...")

if __name__ == "__main__":
    main()

