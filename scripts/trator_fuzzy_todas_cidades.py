import pandas as pd
import unicodedata
import re
import os
import glob
from thefuzz import fuzz
from thefuzz import process

# ==============================================================================
# CONFIGURAÇÕES DO LOTE (BATCH PROCESSING)
# ==============================================================================
ARQUIVO_RECEITA = "receita.csv"
ARQUIVO_SAIDA_MASTER = "tickets_cruzados_brasil_completo.csv"

# ==============================================================================
# FUNÇÕES CORE
# ==============================================================================
def normalizar_texto(texto):
    if pd.isna(texto) or str(texto).strip() == "": return ""
    texto = str(texto).upper().strip()
    texto = ''.join(c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn')
    texto = re.sub(r'[^A-Z0-9\s]', '', texto)
    return re.sub(r'\s+', ' ', texto).strip()

def extrair_numero_endereco(endereco):
    if pd.isna(endereco): return "SN"
    endereco_str = str(endereco).upper()
    match_virgula = re.search(r',\s*(\d+)', endereco_str)
    if match_virgula: return match_virgula.group(1)
    match_numero = re.search(r'\d+', endereco_str)
    return match_numero.group(0) if match_numero else "SN"

# ==============================================================================
# O MOTOR DO TRATOR
# ==============================================================================
def main():
    print("🚜 LIGANDO O TRATOR DE PROCESSAMENTO EM LOTE...")
    
    # Mapeia todos os CSVs da pasta, mas ignora as bases estruturais e resultados antigos
    todos_csvs = glob.glob("*.csv")
    arquivos_ignorar = [ARQUIVO_RECEITA, ARQUIVO_SAIDA_MASTER, "cadastur_associados_merge_ceps.csv", "master_data_abrasel_completo.csv"]
    arquivos_ticket = [f for f in todos_csvs if f not in arquivos_ignorar and not f.startswith("auditoria") and not f.startswith("resultado") and not f.startswith("siga")]
    
    if not arquivos_ticket:
        print("❌ Nenhum arquivo de cidade encontrado para processar.")
        return
        
    print(f"📁 Encontrados {len(arquivos_ticket)} arquivos de cidades. Preparando para varredura...")
    
    # Cria o arquivo Master vazio ou o apaga se já existir de um teste anterior
    if os.path.exists(ARQUIVO_SAIDA_MASTER):
        os.remove(ARQUIVO_SAIDA_MASTER)
        
    primeiro_arquivo = True
    total_linhas_processadas = 0
    total_aprovados = 0

    # LOOP PRINCIPAL: Roda arquivo por arquivo
    for num_arquivo, arquivo_atual in enumerate(arquivos_ticket, 1):
        print(f"\n========================================================")
        print(f"▶️ PROCESSANDO [{num_arquivo}/{len(arquivos_ticket)}]: {arquivo_atual}")
        print(f"========================================================")
        
        try:
            df_ticket = pd.read_csv(arquivo_atual, dtype=str, sep=',', on_bad_lines='skip')
        except Exception as e:
            print(f"⚠️ Erro ao ler {arquivo_atual}. Pulando... Erro: {e}")
            continue
            
        if df_ticket.empty or 'CIDADE' not in df_ticket.columns:
            print(f"⚠️ Arquivo vazio ou sem coluna CIDADE. Pulando...")
            continue
            
        cidade_alvo = normalizar_texto(df_ticket['CIDADE'].iloc[0])
        uf_alvo = normalizar_texto(df_ticket['UF'].iloc[0])
        
        df_ticket['NOME_TICKET_NORM'] = df_ticket['ESTABELECIMENTO'].apply(normalizar_texto)
        df_ticket['NUMERO_TICKET'] = df_ticket['ENDERECO'].apply(extrair_numero_endereco)
        
        print(f"📍 Bloqueio: {cidade_alvo}-{uf_alvo} ({len(df_ticket)} restaurantes)")
        
        # 1. Carrega só o pedaço da Receita dessa cidade
        chunks_receita = []
        try:
            leitor = pd.read_csv(ARQUIVO_RECEITA, usecols=['cnpj_completo', 'razao_social', 'nome_fantasia', 'municipio', 'uf', 'num_logradouro'], dtype=str, chunksize=200000, on_bad_lines='skip')
            for chunk in leitor:
                chunk['municipio_norm'] = chunk['municipio'].apply(normalizar_texto)
                chunk['uf_norm'] = chunk['uf'].apply(normalizar_texto)
                chunks_receita.append(chunk[(chunk['municipio_norm'] == cidade_alvo) & (chunk['uf_norm'] == uf_alvo)])
            df_receita_cidade = pd.concat(chunks_receita)
        except Exception as e:
            print(f"⚠️ Erro ao buscar Receita. Pulando... Erro: {e}")
            continue
            
        if df_receita_cidade.empty:
            print(f"⚠️ Nenhuma empresa na Receita para {cidade_alvo}. Reprovando todos.")
            lista_nomes_receita = []
        else:
            df_receita_cidade['NUMERO_RECEITA'] = df_receita_cidade['num_logradouro'].apply(extrair_numero_endereco)
            df_receita_cidade['NOME_FANTASIA_NORM'] = df_receita_cidade['nome_fantasia'].apply(normalizar_texto)
            df_receita_cidade['RAZAO_SOCIAL_NORM'] = df_receita_cidade['razao_social'].apply(normalizar_texto)
            
            dict_opcoes = {}
            for _, row in df_receita_cidade.iterrows():
                if pd.notna(row['NOME_FANTASIA_NORM']) and row['NOME_FANTASIA_NORM'] != "":
                    dict_opcoes[row['NOME_FANTASIA_NORM']] = {'cnpj': row['cnpj_completo'], 'numero': row['NUMERO_RECEITA'], 'razao': row.get('razao_social', ''), 'fantasia': row.get('nome_fantasia', '')}
                if pd.notna(row['RAZAO_SOCIAL_NORM']) and row['RAZAO_SOCIAL_NORM'] != "":
                    dict_opcoes[row['RAZAO_SOCIAL_NORM']] = {'cnpj': row['cnpj_completo'], 'numero': row['NUMERO_RECEITA'], 'razao': row.get('razao_social', ''), 'fantasia': row.get('nome_fantasia', '')}
            lista_nomes_receita = list(dict_opcoes.keys())

        # 2. IA Léxica para os restaurantes dessa cidade
        resultados = []
        for idx, row in df_ticket.iterrows():
            nome_busca = row['NOME_TICKET_NORM']
            num_busca = row['NUMERO_TICKET']
            
            match_final = {
                'RECEITA_MELHOR_NOME': '', 'SCORE': 0, 'NUMERO_RECEITA': '',
                'CNPJ_RECEITA': '', 'RECEITA_RAZAO_SOCIAL': '', 'RECEITA_FANTASIA': '',
                'MOTIVO_APROVACAO': '🔴 REPROVADO - Cidade sem dados na Receita' if not lista_nomes_receita else '🔴 REPROVADO'
            }
            
            if nome_busca and lista_nomes_receita:
                top_5 = process.extract(nome_busca, lista_nomes_receita, scorer=fuzz.token_sort_ratio, limit=5)
                for nome_candidato, nota in top_5:
                    dados_cand = dict_opcoes[nome_candidato]
                    num_cand = dados_cand['numero']
                    
                    if nota >= 85:
                        match_final.update({'RECEITA_MELHOR_NOME': nome_candidato, 'SCORE': nota, 'NUMERO_RECEITA': num_cand, 'CNPJ_RECEITA': dados_cand['cnpj'], 'RECEITA_RAZAO_SOCIAL': dados_cand['razao'], 'RECEITA_FANTASIA': dados_cand['fantasia'], 'MOTIVO_APROVACAO': '🟢 ALTA SIMILARIDADE (>=85%)'}); break
                    elif nota >= 60 and num_busca != "SN" and num_busca == num_cand:
                        match_final.update({'RECEITA_MELHOR_NOME': nome_candidato, 'SCORE': nota, 'NUMERO_RECEITA': num_cand, 'CNPJ_RECEITA': dados_cand['cnpj'], 'RECEITA_RAZAO_SOCIAL': dados_cand['razao'], 'RECEITA_FANTASIA': dados_cand['fantasia'], 'MOTIVO_APROVACAO': '🔵 CRUZAMENTO DUPLO (NOME >=60% + MESMO NÚMERO)'}); break
                    
                    if match_final['SCORE'] == 0:
                        match_final.update({'RECEITA_MELHOR_NOME': nome_candidato, 'SCORE': nota, 'NUMERO_RECEITA': num_cand, 'MOTIVO_APROVACAO': f'🔴 REPROVADO ({nota}%) - Diferentes ({num_busca}x{num_cand})'})
            
            resultados.append(match_final)

        # 3. Consolidação no arquivo da cidade
        df_resultados = pd.DataFrame(resultados)
        df_ticket = pd.concat([df_ticket, df_resultados], axis=1)
        df_ticket['CRUZAMENTO_APROVADO'] = df_ticket['MOTIVO_APROVACAO'].apply(lambda x: 'SIM' if '🟢' in x or '🔵' in x else 'NÃO')
        df_ticket.drop(columns=['NOME_TICKET_NORM'], inplace=True, errors='ignore')
        
        # 4. Empilha (Append) no Arquivo Master Nacional
        # Escreve o cabeçalho apenas na primeira vez
        df_ticket.to_csv(ARQUIVO_SAIDA_MASTER, mode='a', index=False, header=primeiro_arquivo, encoding='utf-8-sig', sep=';')
        primeiro_arquivo = False
        
        # Atualiza métricas para o relatório final
        total_linhas_processadas += len(df_ticket)
        aprovados_cidade = len(df_ticket[df_ticket['CRUZAMENTO_APROVADO'] == 'SIM'])
        total_aprovados += aprovados_cidade
        print(f"✔️ Concluído. Aprovados aqui: {aprovados_cidade}/{len(df_ticket)}")

    # ==============================================================================
    # RELATÓRIO FINAL NACIONAL
    # ==============================================================================
    print("\n" + "="*60)
    print("🏆 PROCESSAMENTO EM LOTE FINALIZADO COM SUCESSO!")
    print("="*60)
    print(f"Total de Cidades processadas:   {len(arquivos_ticket)}")
    print(f"Total de Restaurantes avaliados: {total_linhas_processadas:,}")
    print(f"Total de Matches aprovados:     {total_aprovados:,}")
    if total_linhas_processadas > 0:
        print(f"Taxa de Aproveitamento Global:  {(total_aprovados/total_linhas_processadas)*100:.2f}%")
    print(f"\n✅ Base Master Consolidada salva em: {ARQUIVO_SAIDA_MASTER}")

if __name__ == "__main__":
    main()