import os
import time
import base64
import zipfile
import io
import pandas as pd
import xml.etree.ElementTree as ET
from pynfe.processamento.comunicacao import ComunicacaoSefaz

# ==========================================
# 1. CONFIGURAÇÕES
# ==========================================
UF = 'SC'
CERTIFICADO = 'certificado_a1.pfx'
SENHA = 'sua_senha'
CNPJ = '00000000000000'
ARQUIVO_NSU = 'ultimo_nsu.txt'

# ==========================================
# 2. CONTROLE RIGOROSO DE NSU
# ==========================================
def ler_ultimo_nsu():
    if os.path.exists(ARQUIVO_NSU):
        with open(ARQUIVO_NSU, 'r') as f:
            return f.read().strip()
    return '0' # Se for a primeira vez na vida que roda, começa do 0

def salvar_ultimo_nsu(nsu):
    with open(ARQUIVO_NSU, 'w') as f:
        f.write(str(nsu))

# ==========================================
# 3. FILTRO DE PRECISÃO (DIESEL E ARLA)
# ==========================================
def eh_abastecimento(item_xml, ns):
    prod = item_xml.find('ns:prod', ns)
    if prod is None: return False
    
    ncm = prod.find('ns:NCM', ns).text if prod.find('ns:NCM', ns) is not None else ""
    xprod = prod.find('ns:xProd', ns).text.upper() if prod.find('ns:xProd', ns) is not None else ""
    
    # Valida NCM característico OU presença forte da palavra
    is_diesel = ncm.startswith('2710') or 'DIESEL' in xprod
    is_arla = ncm == '31021010' or 'ARLA' in xprod
    
    return is_diesel or is_arla

# ==========================================
# 4. EXECUÇÃO PRINCIPAL
# ==========================================
def rodar_extracao():
    con = ComunicacaoSefaz(uf=UF, certificado=CERTIFICADO, senha=SENHA, homologacao=False)
    
    ult_nsu = ler_ultimo_nsu()
    max_nsu = str(int(ult_nsu) + 1) # Força a entrada no loop
    notas_abastecimento = []
    
    print(f"Iniciando consulta a partir do NSU: {ult_nsu}")

    while int(ult_nsu) < int(max_nsu):
        try:
            resposta = con.consulta_distribuicao(cnpj=CNPJ, ultimo_nsu=ult_nsu)
            root = ET.fromstring(resposta.content)
            ns = {'ns': 'http://www.portalfiscal.inf.br/nfe'}
            
            ret_dist_nfe = root.find('.//ns:retDistDFeInt', ns)
            if ret_dist_nfe is not None:
                # Atualiza os ponteiros
                ult_nsu_sefaz = ret_dist_nfe.find('ns:ultNSU', ns).text
                max_nsu = ret_dist_nfe.find('ns:maxNSU', ns).text
                
                if ult_nsu == ult_nsu_sefaz:
                    break # Não há notas novas
                
                ult_nsu = ult_nsu_sefaz
                print(f"Processando lote até NSU {ult_nsu} (Máximo disponível: {max_nsu})")

                docs = ret_dist_nfe.findall('.//ns:docZip', ns)
                
                for doc in docs:
                    schema = doc.get('schema')
                    # Pega apenas XMLs completos de NFe (ignora resumos e eventos de cancelamento)
                    if schema and schema.startswith('procNFe'):
                        # Descompacta o XML retornado em base64 e zip
                        xml_zipado = base64.b64decode(doc.text)
                        with zipfile.ZipFile(io.BytesIO(xml_zipado)) as zf:
                            for file_name in zf.namelist():
                                xml_descompactado = zf.read(file_name)
                                nota_xml = ET.fromstring(xml_descompactado)
                                
                                # Varre os itens da nota
                                itens = nota_xml.findall('.//ns:det', ns)
                                para_abastecimento = False
                                valor_abastecimento = 0.0
                                
                                for item in itens:
                                    if eh_abastecimento(item, ns):
                                        para_abastecimento = True
                                        vprod = item.find('.//ns:prod/ns:vProd', ns).text
                                        valor_abastecimento += float(vprod)
                                
                                if para_abastecimento:
                                    cnpj_emit = nota_xml.find('.//ns:emit/ns:CNPJ', ns).text
                                    xnome_emit = nota_xml.find('.//ns:emit/ns:xNome', ns).text
                                    chave = nota_xml.find('.//ns:protNFe/ns:infProt/ns:chNFe', ns).text
                                    
                                    notas_abastecimento.append({
                                        'CNPJ_Posto': cnpj_emit,
                                        'Nome_Posto': xnome_emit,
                                        'Valor_Combustivel': round(valor_abastecimento, 2),
                                        'Chave_NFe': chave,
                                        'NSU_Origem': doc.get('NSU')
                                    })
                
                # Salva o progresso a cada lote finalizado com sucesso
                salvar_ultimo_nsu(ult_nsu)
            
            # Pausa para evitar bloqueio da SEFAZ
            time.sleep(2)
            
        except Exception as e:
            print(f"Erro na comunicação ou parse: {e}")
            break

    # Gera o Excel se encontrou dados novos
    if notas_abastecimento:
        df = pd.DataFrame(notas_abastecimento)
        nome_arquivo = f"abastecimentos_ate_nsu_{ult_nsu}.xlsx"
        df.to_excel(nome_arquivo, index=False)
        print(f"\nSucesso! {len(notas_abastecimento)} notas de abastecimento encontradas. Salvo em {nome_arquivo}")
    else:
        print("\nNenhum abastecimento novo encontrado desde a última consulta.")

if __name__ == "__main__":
    rodar_extracao()