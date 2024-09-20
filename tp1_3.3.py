import psycopg2
from psycopg2 import sql
import tkinter as tk
from tkinter import messagebox, scrolledtext
from tkinter import ttk
from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

# Caarregar as configurações do banco de dados
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
        messagebox.showerror("Erro", str(error))

# Função para formatar datas no formato dd/mm/yyyy
def format_date(date):
    return date.strftime("%d/%m/%Y") if date else "N/A"

# Função para formatar números decimais com duas casas
def format_decimal(number):
    return f"{number:.2f}"

# Exibir resultados em uma caixa de texto rolável
def display_results(text_widget, results, headers=None):
    text_widget.delete(1.0, tk.END)
    if headers:
        text_widget.insert(tk.END, " | ".join(headers) + "\n")
        text_widget.insert(tk.END, "-" * 100 + "\n")
    for result in results:
        text_widget.insert(tk.END, f"{result}\n")

# Listar os 5 comentários mais úteis com maior e menor avaliação

# Função para listar os 5 melhores e os 5 piores comentários mais úteis
def listar_comentarios_produto(product_id, text_widget):
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

    # Cabeçalhos para exibir os resultados
    text_widget.delete(1.0, tk.END)
    
    text_widget.insert(tk.END, "Melhor avaliados:\n")
    text_widget.insert(tk.END, "ReviewID | CustomerID | Rating | HelpfulVotes | ReviewDate\n")
    text_widget.insert(tk.END, "-" * 100 + "\n")
    for comentario in melhores_formatados:
        text_widget.insert(tk.END, f"{comentario}\n")
    
    text_widget.insert(tk.END, "\nPior avaliados:\n")
    text_widget.insert(tk.END, "ReviewID | CustomerID | Rating | HelpfulVotes | ReviewDate\n")
    text_widget.insert(tk.END, "-" * 100 + "\n")
    for comentario in piores_formatados:
        text_widget.insert(tk.END, f"{comentario}\n")


# Listar os produtos similares com maiores vendas
def listar_similares_maior_venda(product_id, text_widget):
    config = load_config()
    conn = connect(config)
    cur = conn.cursor()
    
    query = """
    SELECT p.ProductID, p.Title, p.SalesRank
    FROM Produto p
    JOIN ProdutoSimilar ps ON p.ProductID = ps.SimilarProductID
    WHERE ps.ProductID = %s
    AND p.SalesRank > (
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
    
    # Formatar os números decimais e exibir resultados
    similares_formatados = [
        (product_id, title, (sales_rank))
        for (product_id, title, sales_rank) in similares
    ]
    
    produto_formatado = [
        (product_id, title, (sales_rank))
        for (product_id, title, sales_rank) in produto
        ]

    text_widget.delete(1.0, tk.END)
    
    text_widget.insert(tk.END, "Produto Base:\n")
    text_widget.insert(tk.END, "ProductID | Title | SalesRank\n")
    text_widget.insert(tk.END, "-" * 100 + "\n")
    for produto in produto_formatado:
        text_widget.insert(tk.END, f"{produto}\n")

    text_widget.insert(tk.END, "\nProdutos Similares com Maior Venda:\n")
    text_widget.insert(tk.END, "ProductID | Title | SalesRank\n")
    text_widget.insert(tk.END, "-" * 100 + "\n")
    for similar in similares_formatados:
        text_widget.insert(tk.END, f"{similar}\n")

# Mostrar a evolução das médias de avaliação
def evolucao_media_avaliacao(product_id, text_widget):
    config = load_config()
    conn = connect(config)
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
    
    # Formatar datas e números decimais
    evolucao_formatada = [
        (format_date(review_date), format_decimal(avg_rating))
        for (review_date, avg_rating) in evolucao
    ]
    
    headers = ["ReviewDate", "AvgRating"]
    display_results(text_widget, evolucao_formatada, headers)
    
    data = {
        'ReviewDate': [format_date(review_date) for review_date, _ in evolucao],
        'AvgRating': [float(format_decimal(avg_rating)) for _, avg_rating in evolucao]  # Converter para float
    }
    
    # Criar um DataFrame do Pandas
    df = pd.DataFrame(data)
    
    # Converter datas para o formato datetime
    df['ReviewDate'] = pd.to_datetime(df['ReviewDate'], format='%d/%m/%Y')
    
    # Criar um intervalo contínuo de datas
    all_dates = pd.date_range(start=df['ReviewDate'].min(), end=df['ReviewDate'].max(), freq='D')
    df.set_index('ReviewDate', inplace=True)
    df = df.reindex(all_dates, fill_value=None)
    df.index.name = 'ReviewDate'
    
    # Interpolação de médias de avaliação
    df['AvgRating'] = df['AvgRating'].interpolate(method='linear')
    sample_size = max(1, len(df.index) // 10)  # Ajuste o divisor para alterar o número de pontos
    sampled_df = df.iloc[::sample_size]
    
    # Criar gráfico de linha com marcadores
    fig, ax = plt.subplots()
    ax.plot(sampled_df.index, sampled_df['AvgRating'], marker='o', linestyle='-', color='blue')
    ax.set_xlabel('Data da Avaliação')
    ax.set_ylabel('Média de Avaliação')
    ax.set_title('Evolução da Média de Avaliações ao Longo do Tempo')
    ax.grid(True)

    # Formatador de datas para o eixo x
    date_format = DateFormatter('%d/%m/%Y')
    ax.xaxis.set_major_formatter(date_format)

    # Ajustar os ticks do eixo x para corresponder exatamente às datas presentes
    ax.set_xticks(df.index[::max(1, len(df.index) // 6)])  # Mostrar menos ticks no eixo X se houver muitas datas
    plt.xticks(rotation=45)

    
    
    # Integrar o gráfico ao Tkinter
    root = tk.Tk()
    root.title("Gráfico de Evolução da Avaliação")
    root.geometry("900x600")
    
    canvas = FigureCanvasTkAgg(fig, master=root)
    canvas.draw()
    canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    root.mainloop()

# Listar os 10 produtos líderes de venda por grupo
def listar_lideres_venda_por_grupo(text_widget):
    config = load_config()
    conn = connect(config)
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
    
    # Formatar números decimais e exibir resultados
    lideres_formatados = [
        (
            product_group, 
            product_id, 
            title, 
            (sales_rank) if sales_rank is not None else "N/A"
        )
        for (product_group, product_id, title, sales_rank) in lideres
    ]
    
    headers = ["ProductGroup", "ProductID", "Title", "SalesRank"]
    display_results(text_widget, lideres_formatados, headers)



# Listar os 10 produtos com a maior média de avaliações úteis
def listar_produtos_melhores_avaliacoes(text_widget):
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
    
    # Formatar números decimais
    melhores_avaliacoes_formatadas = [
        (product_id, title, format_decimal(avg_helpful_votes))
        for (product_id, title, avg_helpful_votes) in melhores_avaliacoes
    ]
    
    headers = ["ProductID", "Title", "AvgHelpfulVotes"]
    display_results(text_widget, melhores_avaliacoes_formatadas, headers)


# Listar as 5 categorias com a maior média de avaliações úteis
def listar_melhores_categorias(text_widget):
    config = load_config()
    conn = connect(config)
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
    
    # Formatar números decimais e ignorar CategoryID
    melhores_categorias_formatadas = [
        (category_name, format_decimal(avg_helpful_votes))
        for (_, category_name, avg_helpful_votes) in melhores_categorias
    ]
    
    headers = ["CategoryName", "AvgHelpfulVotes"]
    display_results(text_widget, melhores_categorias_formatadas, headers)


# Listar os 10 clientes que mais fizeram comentários por grupo
def listar_clientes_por_grupo(text_widget):
    config = load_config()
    conn = connect(config)
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
    SELECT ProductGroup, CustomerID, TotalReviews
    FROM RankedReviews
    WHERE rank <= 10
    ORDER BY ProductGroup, TotalReviews     ;
    """
    cur.execute(query)
    clientes = cur.fetchall()
    cur.close()
    conn.close()
    
    headers = ["ProductGroup", "CustomerID", "ReviewCount"]
    display_results(text_widget, clientes, headers)

def procurar_por_titulo(titulo, text_widget):
    config = load_config()
    conn = connect(config)
    cur = conn.cursor()
    
    query = """
    SELECT *
    FROM Produto p
    WHERE p.Title LIKE %s;
    """
    cur.execute(query, (titulo,))
    produtos = cur.fetchall()
    cur.close()
    conn.close()
    
    # Exibir resultados
    produtos_formatados = [
        (product_id, title, (sales_rank), group)
        for (product_id, title, sales_rank, group) in produtos
    ]
    
    headers = ["ProductID", "Title", "SalesRank", "ProductGroup"]
    display_results(text_widget, produtos_formatados, headers)

def create_gui():
    def update_label(event):
        # Atualiza o texto do rótulo com base na escolha do combo box
        if combo_choice.get() == 'procurar por titulo':
            label_product_id.config(text="Digite o título do produto:")
        else:
            label_product_id.config(text="Digite o ID do produto:")

    def handle_choice():
        choice = combo_choice.get()
        product_id = entry_product_id.get()
        if choice == 'Listar comentários mais úteis':
            listar_comentarios_produto(product_id, text_area)
        elif choice == 'Listar Produtos Similares com Maior Venda':
            listar_similares_maior_venda(product_id, text_area)
        elif choice == 'Evolução da Avaliação':
            evolucao_media_avaliacao(product_id, text_area)
        elif choice == 'Listar os 10 produtos líderes de venda em cada grupo de produtos':
            listar_lideres_venda_por_grupo(text_area)
        elif choice == 'Listar os 10 produtos com a maior média de avaliações úteis positivas por produto':
            listar_produtos_melhores_avaliacoes(text_area)
        elif choice == 'Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto':
            listar_melhores_categorias(text_area)
        elif choice == 'Listar os 10 clientes que mais fizeram comentários por grupo de produto':
            listar_clientes_por_grupo(text_area)
        else:
            messagebox.showerror("Erro", "Opção inválida.")

    root = tk.Tk()
    root.title("Consulta ao Banco de Dados")
    root.geometry("900x600")  # Largura x Altura

    root.grid_rowconfigure(3, weight=1)  # Linha da caixa de texto rolável
    root.grid_columnconfigure(0, weight=1)  # Coluna da primeira coluna
    root.grid_columnconfigure(1, weight=1)  # Coluna da segunda coluna

    # Caixa seletora para escolher a função
    tk.Label(root, text="Selecione a função:").grid(row=0, column=0, padx=10, pady=10, sticky='w')
    combo_choice = ttk.Combobox(root, values=[
        'Listar comentários mais úteis',
        'Listar Produtos Similares com Maior Venda',
        'Evolução da Avaliação',
        'Listar os 10 produtos líderes de venda em cada grupo de produtos',
        'Listar os 10 produtos com a maior média de avaliações úteis positivas por produto',
        'Listar a 5 categorias de produto com a maior média de avaliações úteis positivas por produto',
        'Listar os 10 clientes que mais fizeram comentários por grupo de produto'
    ])
    combo_choice.grid(row=0, column=1, padx=10, pady=10, sticky='ew')
    combo_choice.current(0)

    # Rótulo para o campo de entrada
    label_product_id = tk.Label(root, text="Digite o ID do produto:")
    label_product_id.grid(row=1, column=0, padx=10, pady=10, sticky='w')

    # Campo para entrada do ID do produto
    entry_product_id = tk.Entry(root)
    entry_product_id.grid(row=1, column=1, padx=10, pady=10, sticky='ew')

    # Botão para executar a função selecionada
    btn_execute = tk.Button(root, text="Executar", command=handle_choice)
    btn_execute.grid(row=2, column=0, columnspan=2, padx=10, pady=10, sticky='ew')

    # Caixa de texto rolável para exibir os resultados
    text_area = scrolledtext.ScrolledText(root, wrap=tk.WORD)
    text_area.grid(row=3, column=0, columnspan=2, padx=10, pady=10, sticky='nsew')

    # Atualizar o rótulo quando a escolha mudar
    combo_choice.bind("<<ComboboxSelected>>", update_label)

    # Configurar as proporções das colunas e linhas para que eles se ajustem ao redimensionar a janela
    root.grid_rowconfigure(3, weight=1)
    root.grid_columnconfigure(0, weight=1)
    root.grid_columnconfigure(1, weight=1)
    root.protocol("WM_DELETE_WINDOW", root.quit)
    root.mainloop()

if __name__ == "__main__":
    create_gui()
