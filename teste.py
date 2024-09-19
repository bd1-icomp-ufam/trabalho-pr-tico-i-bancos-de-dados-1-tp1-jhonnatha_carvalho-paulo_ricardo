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

# Exemplo de uso
input_file = 'amazon.txt'  # Seu arquivo de entrada
dividir_arquivo(input_file)
