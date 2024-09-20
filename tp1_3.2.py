import argparse
import os
import psycopg2
import psycopg2.extras
import re
import time
from concurrent.futures import ThreadPoolExecutor


BATCH_SIZE = 10000

def load_config():
    """ Retornar as configurações do banco de dados diretamente no código """
    config = {
        'host': 'localhost',
        'database': 'tp1',
        'user': 'postgres',
        'password': 'senha0405'
    }
    return config

def create_database(config):
    """ Criar o banco de dados com o nome especificado na configuração """
    try:
        # Conectar ao PostgreSQL sem especificar um banco de dados
        connection = psycopg2.connect(
            host=config['host'],
            user=config['user'],
            password=config['password']
        )
        connection.autocommit = True  # Para permitir a criação do banco de dados

        with connection.cursor() as cursor:
            cursor.execute(f"CREATE DATABASE {config['database']};")
            print(f"Bando de dados '{config['database']}' criado com sucesso.")

    except psycopg2.errors.DuplicateDatabase:
        print(f"O banco de dados '{config['database']}' já existe.")
    except (psycopg2.DatabaseError, Exception) as error:
        print(f"Erro ao criar o banco de dados: {error}")
    finally:
        if connection:
            connection.close()



def connect(config):
    """ Connect to the PostgreSQL database server """
    try:
        with psycopg2.connect(**config) as conn:
            #print('Connected to the PostgreSQL server.')
            return conn
    except (psycopg2.DatabaseError, Exception) as error:
        print(error)



def create_tables(conn):
    """ Create tables in the PostgreSQL database """
    commands = (
        """
        CREATE TABLE IF NOT EXISTS Produto (
            ProductID VARCHAR(50) PRIMARY KEY,
            Title VARCHAR(500),
            SalesRank INTEGER,
            ProductGroup VARCHAR(50)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS ProdutoSimilar (
            ProductID VARCHAR(50),
            SimilarProductID VARCHAR(50),
            FOREIGN KEY (ProductID) REFERENCES Produto(ProductID),
            FOREIGN KEY (SimilarProductID) REFERENCES Produto(ProductID),
            PRIMARY KEY (ProductID, SimilarProductID)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Categoria (
            CategoryID INTEGER PRIMARY KEY,
            CategoryName VARCHAR(255)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS CategoriaProduto (
            ProductID VARCHAR(50),
            CategoryID INTEGER,
            FOREIGN KEY (ProductID) REFERENCES Produto(ProductID),
            FOREIGN KEY (CategoryID) REFERENCES Categoria(CategoryID),
            PRIMARY KEY (ProductID, CategoryID)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS Review (
            ReviewID SERIAL PRIMARY KEY,
            ProductID VARCHAR(50),
            ReviewDate DATE,
            CustomerID VARCHAR(50),
            Rating INTEGER,
            Votes INTEGER,
            HelpfulVotes INTEGER,
            FOREIGN KEY (ProductID) REFERENCES Produto(ProductID)
        )
        """
    )

    try:
        with conn.cursor() as cur:
            for command in commands:
                cur.execute(command)
                print(f"Executed command: {command}")
            conn.commit()
            print('Tables created successfully.')
    except (psycopg2.DatabaseError, Exception) as error:
        print(f'Error creating tables: {error}')


def insert_product_from_file(file_path, config):
    """Inserir ou atualizar produtos no banco de dados a partir de um arquivo de texto usando inserção por lote"""
    conn = connect(config)
    try:
        with conn.cursor() as cur:
            with open(file_path, 'r') as file:
                content = file.read()

            produtos = re.split(r'\n\s*Id:', content)
            total_products = len(produtos)

            batch_data = []

            for product in produtos:
                product_id_match = re.search(r'ASIN:\s*(\S+)', product)
                title_match = re.search(r'title:\s*(.+)', product)
                sales_rank_match = re.search(r'salesrank:\s*(-?\d+)', product)
                product_group_match = re.search(r'group:\s*(\w+)', product)
                
                if product_id_match and title_match and sales_rank_match and product_group_match:
                    product_id = product_id_match.group(1)
                    title = title_match.group(1).strip()
                    sales_rank = int(sales_rank_match.group(1))
                    product_group = product_group_match.group(1).strip()

                    batch_data.append((product_id, title, sales_rank, product_group))
                
                # Inserir por lote
                if len(batch_data) >= BATCH_SIZE:
                    psycopg2.extras.execute_values(
                        cur,
                        '''
                            INSERT INTO Produto (ProductID, Title, SalesRank, ProductGroup)
                            VALUES %s
                            ON CONFLICT (ProductID) 
                            DO UPDATE SET 
                                Title = EXCLUDED.Title,
                                SalesRank = EXCLUDED.SalesRank,
                                ProductGroup = EXCLUDED.ProductGroup;
                        ''',
                        batch_data
                    )
                    conn.commit()
                    batch_data = []

            # Inserir o que sobrou no final
            if batch_data:
                psycopg2.extras.execute_values(
                    cur,
                    '''
                        INSERT INTO Produto (ProductID, Title, SalesRank, ProductGroup)
                        VALUES %s
                        ON CONFLICT (ProductID) 
                        DO UPDATE SET 
                            Title = EXCLUDED.Title,
                            SalesRank = EXCLUDED.SalesRank,
                            ProductGroup = EXCLUDED.ProductGroup;
                    ''',
                    batch_data
                )
                conn.commit()

    except (psycopg2.DatabaseError, Exception) as error:
        print(f'Error during the operation produto: {error}')
        conn.rollback()
    finally:
        conn.close()




def insert_categories_from_file(file_path, config):
    """Inserir categorias e a associação entre produto e categorias no banco de dados a partir de um arquivo de texto usando inserção por lote"""
    conn = connect(config)
    try:
        with conn.cursor() as cur:
            with open(file_path, 'r') as file:
                content = file.read()

            produtos = re.split(r'\n\s*Id:', content)
            total_products = len(produtos)

            categoria_data = []
            categoria_produto_data = []

            for product in produtos:
                product_id_match = re.search(r'ASIN:\s*(\S+)', product)
                if not product_id_match:
                    continue
                
                product_id = product_id_match.group(1)
                
                categories_match = re.search(r'categories:\s*(\d+)\s*(.+)', product, re.DOTALL)
                
                if categories_match:
                    categories_data = categories_match.group(2).strip().split('\n')
                    for category_path in categories_data:
                        category_parts = re.findall(r'\|([^[]+)\[(\d+)\]', category_path)

                        for category_name, category_id in category_parts:
                            category_name = category_name.strip()

                            categoria_data.append((category_id, category_name))
                            categoria_produto_data.append((product_id, category_id))

                            # Inserir por lote
                            if len(categoria_data) >= BATCH_SIZE:
                                psycopg2.extras.execute_values(
                                    cur,
                                    '''
                                        INSERT INTO Categoria (CategoryID, CategoryName)
                                        VALUES %s
                                        ON CONFLICT (CategoryID) DO NOTHING;
                                    ''',
                                    categoria_data
                                )
                                psycopg2.extras.execute_values(
                                    cur,
                                    '''
                                        INSERT INTO CategoriaProduto (ProductID, CategoryID)
                                        VALUES %s
                                        ON CONFLICT (ProductID, CategoryID) DO NOTHING;
                                    ''',
                                    categoria_produto_data
                                )
                                conn.commit()
                                categoria_data = []
                                categoria_produto_data = []

            # Inserir o que sobrou no final
            if categoria_data:
                psycopg2.extras.execute_values(
                    cur,
                    '''
                        INSERT INTO Categoria (CategoryID, CategoryName)
                        VALUES %s
                        ON CONFLICT (CategoryID) DO NOTHING;
                    ''',
                    categoria_data
                )
                psycopg2.extras.execute_values(
                    cur,
                    '''
                        INSERT INTO CategoriaProduto (ProductID, CategoryID)
                        VALUES %s
                        ON CONFLICT (ProductID, CategoryID) DO NOTHING;
                    ''',
                    categoria_produto_data
                )
                conn.commit()

    except (psycopg2.DatabaseError, Exception) as error:
        print(f'Error during the operation categoria: {error}')
        conn.rollback()
    finally:
        conn.close()




def insert_reviews_from_file(file_path, config):
    """Inserir os reviews no banco de dados a partir de um arquivo de texto usando inserção por lote"""
    conn = connect(config)
    try:
        with conn.cursor() as cur:
            with open(file_path, 'r') as file:
                content = file.read()

            produtos = re.split(r'\n\s*Id:', content)
            total_products = len(produtos)

            review_data = []

            for product in produtos:
                product_id_match = re.search(r'ASIN:\s*(\S+)', product)
                if not product_id_match:
                    continue
                
                product_id = product_id_match.group(1)
                
                reviews_match = re.search(r'reviews:\s*total:\s*\d+\s*downloaded:\s*\d+\s*avg rating:\s*[\d.]+([\s\S]*?)(?=Id:|\Z)', product)
                
                if reviews_match:
                    reviews_data = reviews_match.group(1).strip().split('\n')
                    for review_line in reviews_data:
                        review_match = re.search(r'(\d{4}-\d{1,2}-\d{1,2})\s+cutomer:\s*(\S+)\s+rating:\s*(\d+)\s+votes:\s*(\d+)\s+helpful:\s*(\d+)', review_line)
                        
                        if review_match:
                            review_date = review_match.group(1)
                            customer_id = review_match.group(2)
                            rating = int(review_match.group(3))
                            votes = int(review_match.group(4))
                            helpful = int(review_match.group(5))

                            review_data.append((product_id, review_date, customer_id, rating, votes, helpful))

                            # Inserir por lote
                            if len(review_data) >= BATCH_SIZE:
                                psycopg2.extras.execute_values(
                                    cur,
                                    '''
                                        INSERT INTO Review (ProductID, ReviewDate, CustomerID, Rating, Votes, HelpfulVotes)
                                        VALUES %s
                                        ON CONFLICT (ReviewID) DO NOTHING;
                                    ''',
                                    review_data
                                )
                                conn.commit()
                                review_data = []

            # Inserir o que sobrou no final
            if review_data:
                psycopg2.extras.execute_values(
                    cur,
                    '''
                        INSERT INTO Review (ProductID, ReviewDate, CustomerID, Rating, Votes, HelpfulVotes)
                        VALUES %s
                        ON CONFLICT (ReviewID) DO NOTHING;
                    ''',
                    review_data
                )
                conn.commit()

    except (psycopg2.DatabaseError, Exception) as error:
        print(f'Error during the operation review: {error}')
        conn.rollback()
    finally:
        conn.close()



def insert_similar_products_from_file(file_path, config):
    """Inserir produtos similares no banco de dados a partir de um arquivo de texto usando inserção por lote"""
    conn = connect(config)

    try:
        with conn.cursor() as cur:
            # Abrir e ler o arquivo
            with open(file_path, 'r') as file:
                content = file.read()
            
            # Encontrar todos os blocos de produto
            produtos = re.split(r'\n\s*Id:', content)
            
            produto_data = []
            similar_data = []
            
            for product in produtos:
                # Extrair o ProductID
                product_id_match = re.search(r'ASIN:\s*(\S+)', product)
                if not product_id_match:
                    continue  # Pular se o ID do produto não for encontrado
                
                product_id = product_id_match.group(1)
                
                # Encontrar os produtos similares associados
                similar_products_match = re.search(r'similar:\s*(\d+)\s+(.+)', product)
                
                if similar_products_match:
                    similar_count = int(similar_products_match.group(1))
                    
                    # Se não houver produtos similares, ignorar essa parte
                    if similar_count == 0:
                        continue

                    similar_products = similar_products_match.group(2).strip().split()
                    
                    for similar_product_id in similar_products:
                        # Verificar se o produto similar existe na tabela Produto
                        cur.execute('SELECT 1 FROM Produto WHERE ProductID = %s', (similar_product_id,))
                        product_exists = cur.fetchone()

                        if not product_exists:
                            # Produto similar não existe, então inserir como "produto desconhecido"
                            try:
                                cur.execute('''
                                    INSERT INTO Produto (ProductID, Title, SalesRank, ProductGroup)
                                    VALUES (%s, %s, %s, %s)
                                    ON CONFLICT (ProductID) DO NOTHING;
                                ''', (similar_product_id, 'Produto Desconhecido', None, 'Desconhecido'))
                            except (psycopg2.DatabaseError, Exception) as error:
                                print(f'Erro ao inserir produto desconhecido {similar_product_id}. Error: {error}')
                        
                        # Verificar se o produto ainda existe na tabela Produto antes de adicionar à tabela ProdutoSimilar
                        cur.execute('SELECT 1 FROM Produto WHERE ProductID = %s', (product_id,))
                        product_exists = cur.fetchone()

                        if product_exists:
                            similar_data.append((product_id, similar_product_id))

                        # Inserir em lotes se o tamanho do lote for alcançado
                        if len(produto_data) >= BATCH_SIZE:
                            psycopg2.extras.execute_values(
                                cur,
                                '''
                                    INSERT INTO Produto (ProductID, Title, SalesRank, ProductGroup)
                                    VALUES %s
                                    ON CONFLICT (ProductID) DO NOTHING;
                                ''',
                                produto_data
                            )
                            produto_data = []

                        if len(similar_data) >= BATCH_SIZE:
                            psycopg2.extras.execute_values(
                                cur,
                                '''
                                    INSERT INTO ProdutoSimilar (ProductID, SimilarProductID)
                                    VALUES %s
                                    ON CONFLICT (ProductID, SimilarProductID) DO NOTHING;
                                ''',
                                similar_data
                            )
                            similar_data = []
            
            # Inserir o que sobrou no final
            if produto_data:
                psycopg2.extras.execute_values(
                    cur,
                    '''
                        INSERT INTO Produto (ProductID, Title, SalesRank, ProductGroup)
                        VALUES %s
                        ON CONFLICT (ProductID) DO NOTHING;
                    ''',
                    produto_data
                )
            
            if similar_data:
                psycopg2.extras.execute_values(
                    cur,
                    '''
                        INSERT INTO ProdutoSimilar (ProductID, SimilarProductID)
                        VALUES %s
                        ON CONFLICT (ProductID, SimilarProductID) DO NOTHING;
                    ''',
                    similar_data
                )
            
            conn.commit()
    
    except (psycopg2.DatabaseError, Exception) as error:
        print(f'Error during the operation similar: {error}')
        conn.rollback()
    finally:
        conn.close()



def dividir_arquivo(input_file):
    output_file1 = 'parte1.txt'
    output_file2 = 'parte2.txt'
    with open(input_file, 'r') as file:
        lines = file.readlines()

    # Filtrar as linhas que começam com "Id:" para contar os itens
    ids = [i for i, line in enumerate(lines) if line.startswith("Id:")]

    # Número total de itens
    total_itens = len(ids)

    # Determinar o ponto de divisão (metade dos itens)
    metade = total_itens // 2

    # Dividir os itens no meio
    primeira_metade = ids[:metade]
    segunda_metade = ids[metade:]

    # Criar os arquivos de saída
    with open(output_file1, 'w') as file1:
        for i in range(primeira_metade[0], segunda_metade[0]):  # Linhas até o início da segunda metade
            file1.write(lines[i])

    with open(output_file2, 'w') as file2:
        for i in range(segunda_metade[0], len(lines)):  # Linhas até o final
            file2.write(lines[i])

def produto_thread(file_path, config):
    """ Processar as quatro funções em paralelo usando threads """
    dividir_arquivo(file_path)
    file_path1 = 'parte1.txt'
    file_path2 = 'parte2.txt'  
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        futures.append(executor.submit(insert_product_from_file, file_path1, config))
        futures.append(executor.submit(insert_product_from_file, file_path2, config))

        # Aguardar todos os processos terminarem
        for future in futures:
            future.result()

def process_insertion(file_path, config):
    """ Processar as quatro funções em paralelo usando threads """

    with ThreadPoolExecutor(max_workers=os.cpu_count()-2) as executor:
        futures = []
        futures.append(executor.submit(insert_categories_from_file, file_path, config))
        futures.append(executor.submit(insert_reviews_from_file, file_path, config))
        futures.append(executor.submit(insert_similar_products_from_file, file_path, config))

        # Aguardar todos os processos terminarem
        for future in futures:
            future.result()

def deletar_partes():
    arquivos = ['parte1.txt', 'parte2.txt']
    
    for arquivo in arquivos:
        if os.path.exists(arquivo):
            os.remove(arquivo)
            print(f"")
        else:
            print(f"")
def main():
    """Função principal para coordenar a execução das inserções no banco de dados"""
    # Configuração do argparse para aceitar o caminho do arquivo pelo terminal
    parser = argparse.ArgumentParser(description="Processar inserções no banco de dados a partir de um arquivo.")
    parser.add_argument('file_path', type=str, help="Caminho para o arquivo de dados (ex: 'amazon.txt')")
    
    # Parsea os argumentos
    args = parser.parse_args()
    file_path = args.file_path
    
    # Carregar a configuração do banco de dados
    config = load_config()
    create_database(config)
    
    # Criação das tabelas
    conn = connect(config)
    create_tables(conn)
    
    # Inserir produtos, categorias, produtos similares e reviews em sequência
    start_time = time.time()
    
    print("Iniciando a inserção de produtos...")
    produto_thread(file_path, config)
    deletar_partes()
    print("Iniciando a inserção de categorias, produtos similares e reviews...")
    process_insertion(file_path, config)
    #print("Iniciando a inserção de categorias...")
    #insert_categories_from_file(file_path, config)
    
    #print("Iniciando a inserção de produtos similares...")
    #insert_similar_products_from_file(file_path, config)
    
    #print("Iniciando a inserção de reviews...")
    #insert_reviews_from_file(file_path, config)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Processo completo. Tempo total: {elapsed_time:.2f} segundos.")


if __name__ == "__main__":
    main()
