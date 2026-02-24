import pandas as pd
import unicodedata
import re
from thefuzz import fuzz
from thefuzz import process

# ==============================================================================
# CONFIGURAÇÕES (CRUZAMENTO DUPLO)
# ==============================================================================
ARQUIVO_RECEITA = "receita.csv"
ARQUIVO_TICKET_TESTE = "GO_Goianésia.csv" 
ARQUIVO_SAIDA = "auditoria_fuzzy_goianesia_inteligente.csv"

# ==============================================================================
# FUNÇÕES DE ENGENHARIA E LIMPEZA
# ==============================================================================
def normalizar_texto(texto):
    if pd.isna(texto) or str(texto).strip() == "": return ""
    texto = str(texto).upper().strip()
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    texto = re.sub(r'[^A-Z0-9\s]', '', texto)
    return re.sub(r'\s+', ' ', texto).strip()

def extrair_numero_endereco(endereco):
    """Extrai apenas os dígitos numéricos do endereço para fazer o match duplo."""
    if pd.isna(endereco): return "SN"
    endereco_str = str(endereco).upper()
    
    # 1. Tenta achar o número logo após a vírgula (Padrão: Rua X, 123)
    match_virgula = re.search(r',\s*(\d+)', endereco_str)
    if match_virgula:
        return match_virgula.group(1)
    
    # 2. Se não tem vírgula, caça o primeiro bloco de números da string
    match_numero = re.search(r'\d+', endereco_str)
    if match_numero:
        return match_numero.group(0)
    
    return "SN"

# ==============================================================================
# PIPELINE PRINCIPAL
# ==============================================================================
def main():
    print(f"🚀 Iniciando Cruzamento Inteligente (Duplo) para: {ARQUIVO_TICKET_TESTE}")
    
    # 1. Carrega o arquivo do Ticket
    try:
        df_ticket = pd.read_csv(ARQUIVO_TICKET_TESTE, dtype=str, sep=',')
    except Exception as e:
        print(f"❌ Erro ao ler o Ticket: {e}")
        return
        
    cidade_alvo = normalizar_texto(df_ticket['CIDADE'].iloc[0])
    uf_alvo = normalizar_texto(df_ticket['UF'].iloc[0])
    
    # Extrai o número do Ticket (Isso é a nossa carta na manga)
    df_ticket['NOME_TICKET_NORM'] = df_ticket['ESTABELECIMENTO'].apply(normalizar_texto)
    df_ticket['NUMERO_TICKET'] = df_ticket['ENDERECO'].apply(extrair_numero_endereco)

    print(f"📍 Bloqueio Geográfico: {cidade_alvo} - {uf_alvo}")
    
    # 2. Carrega a Receita Federal filtrando a cidade
    print("⏳ Carregando Receita Federal e preparando dicionário de endereços...")
    chunks_receita = []
    try:
        colunas_uso = ['cnpj_completo', 'razao_social', 'nome_fantasia', 'municipio', 'uf', 'num_logradouro']
        leitor = pd.read_csv(ARQUIVO_RECEITA, usecols=colunas_uso, dtype=str, chunksize=200000, on_bad_lines='skip')
        
        for chunk in leitor:
            chunk['municipio_norm'] = chunk['municipio'].apply(normalizar_texto)
            chunk['uf_norm'] = chunk['uf'].apply(normalizar_texto)
            chunk_filtrado = chunk[(chunk['municipio_norm'] == cidade_alvo) & (chunk['uf_norm'] == uf_alvo)]
            chunks_receita.append(chunk_filtrado)
            
        df_receita_cidade = pd.concat(chunks_receita)
        print(f"✔️ {len(df_receita_cidade)} empresas encontradas na Receita.")
    except Exception as e:
        print(f"❌ Erro na Receita: {e}")
        return

    # 3. Prepara o Dicionário de Busca (Nome -> CNPJ e Número)
    df_receita_cidade['NUMERO_RECEITA'] = df_receita_cidade['num_logradouro'].apply(extrair_numero_endereco)
    df_receita_cidade['NOME_FANTASIA_NORM'] = df_receita_cidade['nome_fantasia'].apply(normalizar_texto)
    df_receita_cidade['RAZAO_SOCIAL_NORM'] = df_receita_cidade['razao_social'].apply(normalizar_texto)
    
    dict_opcoes = {}
    for _, row in df_receita_cidade.iterrows():
        cnpj = row['cnpj_completo']
        num = row['NUMERO_RECEITA']
        razao = row.get('razao_social', '')
        fantasia = row.get('nome_fantasia', '')
        
        # Mapeia tanto a Fantasia quanto a Razão Social para o mesmo CNPJ e Número
        if pd.notna(row['NOME_FANTASIA_NORM']) and row['NOME_FANTASIA_NORM'] != "":
            dict_opcoes[row['NOME_FANTASIA_NORM']] = {'cnpj': cnpj, 'numero': num, 'razao': razao, 'fantasia': fantasia}
        if pd.notna(row['RAZAO_SOCIAL_NORM']) and row['RAZAO_SOCIAL_NORM'] != "":
            dict_opcoes[row['RAZAO_SOCIAL_NORM']] = {'cnpj': cnpj, 'numero': num, 'razao': razao, 'fantasia': fantasia}

    lista_nomes_receita = list(dict_opcoes.keys())

    # 4. O MOTOR DE CRUZAMENTO DUPLO
    print("🧠 Iniciando Algoritmo de Inteligência Léxica (Top 5 + Número)...")
    
    resultados = []
    
    for idx, row in df_ticket.iterrows():
        nome_busca = row['NOME_TICKET_NORM']
        num_busca = row['NUMERO_TICKET']
        
        match_final = {
            'RECEITA_MELHOR_NOME': '', 
            'SCORE': 0, 
            'NUMERO_RECEITA': '',
            'CNPJ_RECEITA': '', 
            'RECEITA_RAZAO_SOCIAL': '',
            'RECEITA_FANTASIA': '',
            'MOTIVO_APROVACAO': '🔴 REPROVADO'
        }
        
        if nome_busca and lista_nomes_receita:
            # Em vez de pegar só 1, pede os 5 nomes mais parecidos
            top_5 = process.extract(nome_busca, lista_nomes_receita, scorer=fuzz.token_sort_ratio, limit=5)
            
            for nome_candidato, nota in top_5:
                dados_cand = dict_opcoes[nome_candidato]
                num_cand = dados_cand['numero']
                
                # REGRA 1: Nome muito igual (Aprova direto independente do número)
                if nota >= 85:
                    match_final.update({
                        'RECEITA_MELHOR_NOME': nome_candidato, 'SCORE': nota, 'NUMERO_RECEITA': num_cand,
                        'CNPJ_RECEITA': dados_cand['cnpj'], 'RECEITA_RAZAO_SOCIAL': dados_cand['razao'],
                        'RECEITA_FANTASIA': dados_cand['fantasia'], 'MOTIVO_APROVACAO': '🟢 ALTA SIMILARIDADE (>=85%)'
                    })
                    break # Para a busca, achou o campeão
                    
                # REGRA 2: Cruzamento Duplo (Nome Razoável + Mesmo Número)
                elif nota >= 60 and num_busca != "SN" and num_busca == num_cand:
                    match_final.update({
                        'RECEITA_MELHOR_NOME': nome_candidato, 'SCORE': nota, 'NUMERO_RECEITA': num_cand,
                        'CNPJ_RECEITA': dados_cand['cnpj'], 'RECEITA_RAZAO_SOCIAL': dados_cand['razao'],
                        'RECEITA_FANTASIA': dados_cand['fantasia'], 'MOTIVO_APROVACAO': '🔵 CRUZAMENTO DUPLO (NOME >=60% + MESMO NÚMERO)'
                    })
                    break # Para a busca, achou pelo número
                
                # Se for o primeiro da lista e não passou nas regras, salva só para registro (sem dar CNPJ)
                if match_final['SCORE'] == 0:
                    match_final.update({
                        'RECEITA_MELHOR_NOME': nome_candidato, 'SCORE': nota, 'NUMERO_RECEITA': num_cand,
                        'MOTIVO_APROVACAO': f'🔴 REPROVADO ({nota}%) - Números diferentes ({num_busca} x {num_cand})'
                    })
        
        resultados.append(match_final)

    # 5. CONSOLIDAÇÃO DOS DADOS
    df_resultados = pd.DataFrame(resultados)
    
    # Junta as colunas novas com o df_ticket original
    df_ticket = pd.concat([df_ticket, df_resultados], axis=1)
    
    # Cria a Flag Final
    df_ticket['CRUZAMENTO_APROVADO'] = df_ticket['MOTIVO_APROVACAO'].apply(lambda x: 'SIM' if '🟢' in x or '🔵' in x else 'NÃO')

    # Remove colunas auxiliares de processamento para não poluir o Excel
    df_ticket.drop(columns=['NOME_TICKET_NORM'], inplace=True, errors='ignore')
    
    # Salva
    df_ticket.sort_values(by='CRUZAMENTO_APROVADO', ascending=False, inplace=True)
    df_ticket.to_csv(ARQUIVO_SAIDA, index=False, encoding='utf-8-sig', sep=';')
    
    qtd_aprovados = len(df_ticket[df_ticket['CRUZAMENTO_APROVADO'] == 'SIM'])
    total = len(df_ticket)
    
    print("\n========================================================")
    print("📊 RELATÓRIO: INTELIGÊNCIA LÉXICA + CRUZAMENTO DUPLO")
    print("========================================================")
    print(f"Total avaliado no Ticket: {total}")
    print(f"Aprovados com Sucesso:    {qtd_aprovados}")
    print(f"Taxa de Aprovação:        {(qtd_aprovados/total)*100:.2f}%")
    print(f"\n✅ Arquivo Diagnóstico gerado: {ARQUIVO_SAIDA}")

if __name__ == "__main__":
    main()