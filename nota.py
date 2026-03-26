import os
import time
import base64
import zipfile
import io
import urllib3
import logging
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
from pynfe.processamento.comunicacao import ComunicacaoSefaz
from pynfe.entidades.evento import Evento
from dotenv import load_dotenv

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

# ==========================================
# LOGGING
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('sefaz_consulta.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ==========================================
# 1. CONFIGURAÇÕES
# ==========================================
UF          = os.getenv('UF_EMPRESA', 'SC')
CERTIFICADO = os.getenv('CAMINHO_CERTIFICADO')
SENHA       = os.getenv('SENHA_CERTIFICADO')
CNPJ        = os.getenv('CNPJ_EMPRESA')
ARQUIVO_NSU = 'ultimo_nsu.txt'

# Intervalo MÍNIMO obrigatório entre chamadas à SEFAZ (segundos).
# A SEFAZ exige pelo menos 1 hora entre execuções completas.
# Este valor controla o sleep entre LOTES dentro da mesma execução.
INTERVALO_ENTRE_LOTES = 5      # segundos entre cada requisição de lote
MAX_LOTES_POR_EXECUCAO = 50   # segurança: evita loop infinito

# ==========================================
# 2. CONTROLE DE NSU
# ==========================================
def ler_ultimo_nsu() -> int:
    if os.path.exists(ARQUIVO_NSU):
        with open(ARQUIVO_NSU, 'r') as f:
            valor = f.read().strip()
            if valor.isdigit():
                return int(valor)
    return 0

def salvar_ultimo_nsu(nsu: int):
    with open(ARQUIVO_NSU, 'w') as f:
        f.write(str(nsu))
    log.info(f"NSU salvo: {nsu}")

# ==========================================
# 3. FILTRO DE ABASTECIMENTO (DIESEL / ARLA)
# ==========================================
NCM_DIESEL_PREFIXO = '2710'
NCM_ARLA           = '31021010'

def eh_abastecimento(item_xml, ns) -> tuple[bool, float]:
    """Retorna (é_abastecimento, valor_produto)."""
    prod = item_xml.find('ns:prod', ns)
    if prod is None:
        return False, 0.0

    ncm_el   = prod.find('ns:NCM', ns)
    xprod_el = prod.find('ns:xProd', ns)
    vprod_el = prod.find('ns:vProd', ns)

    ncm   = ncm_el.text.strip()   if ncm_el   is not None else ''
    xprod = xprod_el.text.upper() if xprod_el is not None else ''
    vprod = float(vprod_el.text)  if vprod_el is not None else 0.0

    is_diesel = ncm.startswith(NCM_DIESEL_PREFIXO) or 'DIESEL' in xprod
    is_arla   = ncm == NCM_ARLA or 'ARLA' in xprod

    return (is_diesel or is_arla), vprod

# ==========================================
# 4. MANIFESTAÇÃO (SOMENTE QUANDO NECESSÁRIO)
# ==========================================
def manifestar_ciencia(con: ComunicacaoSefaz, chave: str):
    """
    Envia Ciência da Operação (110210) apenas uma vez por chave.
    Registra em arquivo local para não reenviar.
    """
    arquivo_manifests = 'manifestacoes_enviadas.txt'
    chaves_enviadas: set[str] = set()

    if os.path.exists(arquivo_manifests):
        with open(arquivo_manifests, 'r') as f:
            chaves_enviadas = {linha.strip() for linha in f if linha.strip()}

    if chave in chaves_enviadas:
        log.debug(f"Manifestação já enviada para {chave}, pulando.")
        return

    try:
        evento = Evento(
            cnpj=CNPJ,
            chave=chave,
            tp_evento='210210',  # Ciência da Operação
            seq_evento='1',
            c_orgao='91',        # Ambiente Nacional
            data_emissao=datetime.now()
        )
        con.recepcao_evento(evento)

        with open(arquivo_manifests, 'a') as f:
            f.write(chave + '\n')

        log.info(f"Manifestação enviada: {chave}")
        time.sleep(2)  # pausa após cada manifestação

    except Exception as e:
        log.error(f"Erro ao manifestar {chave}: {e}")

# ==========================================
# 5. PROCESSAMENTO DE UM LOTE
# ==========================================
def processar_lote(root, ns, con, notas_abastecimento: list):
    """Processa os documentos de um lote retornado pela SEFAZ."""
    ret = root.find('.//ns:retDistDFeInt', ns)
    if ret is None:
        return None, None, False

    cstat = ret.find('ns:cStat', ns)
    cstat_val = cstat.text if cstat is not None else '???'

    # --- Bloqueio da SEFAZ ---
    if cstat_val == '656':
        log.warning("SEFAZ bloqueou por Consumo Indevido (656). Aguarde 1 hora.")
        return None, None, True   # sinaliza bloqueio

    # --- Sem documentos novos ---
    if cstat_val == '137':
        log.info("Nenhum documento novo (137).")
        return None, None, False

    ult_nsu_el = ret.find('ns:ultNSU', ns)
    max_nsu_el = ret.find('ns:maxNSU', ns)

    if ult_nsu_el is None or max_nsu_el is None:
        log.warning("Resposta sem ultNSU/maxNSU.")
        return None, None, False

    ult_nsu_ret = int(ult_nsu_el.text)
    max_nsu_ret = int(max_nsu_el.text)

    for doc in ret.findall('.//ns:docZip', ns):
        schema = doc.get('schema', '')

        # ---- XML completo (procNFe) ----------------------------------------
        if schema.startswith('procNFe'):
            try:
                xml_bytes = base64.b64decode(doc.text)
                with zipfile.ZipFile(io.BytesIO(xml_bytes)) as zf:
                    for fname in zf.namelist():
                        nota_xml = ET.fromstring(zf.read(fname))
                        itens = nota_xml.findall('.//ns:det', ns)

                        valor_total = 0.0
                        is_abast    = False

                        for item in itens:
                            ok, vprod = eh_abastecimento(item, ns)
                            if ok:
                                is_abast = True
                                valor_total += vprod

                        if is_abast:
                            cnpj_emit  = _texto(nota_xml, './/ns:emit/ns:CNPJ', ns)
                            xnome_emit = _texto(nota_xml, './/ns:emit/ns:xNome', ns)
                            chave      = _texto(nota_xml, './/ns:protNFe/ns:infProt/ns:chNFe', ns)
                            nsu_doc    = doc.get('NSU', '')

                            notas_abastecimento.append({
                                'CNPJ_Posto':        cnpj_emit,
                                'Nome_Posto':        xnome_emit,
                                'Valor_Combustivel': round(valor_total, 2),
                                'Chave_NFe':         chave,
                                'NSU_Origem':        nsu_doc,
                            })
                            log.info(f"Abastecimento encontrado! Chave: {chave} | Valor: R$ {valor_total:.2f}")

            except Exception as e:
                log.error(f"Erro ao processar procNFe (NSU {doc.get('NSU')}): {e}")

        # ---- Resumo (resNFe) — manifesta SOMENTE se for abastecimento --------
        # Nota: em resumos não temos itens detalhados, então manifestamos
        # todas as notas pendentes para obter o XML completo na próxima rodada.
        # Se preferir manifestar só as de abastecimento, filtre pelo CNPJ emitente
        # ou aguarde o XML completo chegar na próxima execução.
        elif schema.startswith('resNFe'):
            try:
                xml_bytes = base64.b64decode(doc.text)
                with zipfile.ZipFile(io.BytesIO(xml_bytes)) as zf:
                    for fname in zf.namelist():
                        resumo = ET.fromstring(zf.read(fname))
                        chave_el = resumo.find('.//ns:chNFe', ns)
                        if chave_el is not None:
                            # Manifesta para que a SEFAZ libere o XML completo
                            manifestar_ciencia(con, chave_el.text)
            except Exception as e:
                log.error(f"Erro ao processar resNFe (NSU {doc.get('NSU')}): {e}")

    return ult_nsu_ret, max_nsu_ret, False

def _texto(xml_root, xpath: str, ns: dict) -> str:
    el = xml_root.find(xpath, ns)
    return el.text.strip() if el is not None and el.text else ''

# ==========================================
# 6. EXECUÇÃO PRINCIPAL
# ==========================================
def rodar_extracao() -> str | None:
    if not all([CERTIFICADO, SENHA, CNPJ]):
        log.error("Variáveis de ambiente incompletas (CAMINHO_CERTIFICADO, SENHA_CERTIFICADO, CNPJ_EMPRESA).")
        return None

    con = ComunicacaoSefaz(UF, CERTIFICADO, SENHA, False)
    ns  = {'ns': 'http://www.portalfiscal.inf.br/nfe'}

    nsu_atual = ler_ultimo_nsu()
    notas_abastecimento: list[dict] = []
    lotes_processados = 0

    log.info(f"=== Iniciando varredura SEFAZ | NSU inicial: {nsu_atual} ===")

    while lotes_processados < MAX_LOTES_POR_EXECUCAO:
        lotes_processados += 1

        try:
            log.info(f"Consultando NSU {nsu_atual} (lote {lotes_processados}/{MAX_LOTES_POR_EXECUCAO})...")
            resposta = con.consulta_distribuicao(cnpj=CNPJ, nsu=nsu_atual)
            root     = ET.fromstring(resposta.content)

            ult_nsu_ret, max_nsu_ret, bloqueado = processar_lote(root, ns, con, notas_abastecimento)

            if bloqueado:
                # Salva progresso e interrompe — agende a próxima execução para daqui 1h
                salvar_ultimo_nsu(nsu_atual)
                break

            if ult_nsu_ret is None:
                # Sem documentos novos ou erro — salva e encerra
                salvar_ultimo_nsu(nsu_atual)
                break

            salvar_ultimo_nsu(ult_nsu_ret)
            log.info(f"Lote OK | ultNSU={ult_nsu_ret} | maxNSU={max_nsu_ret}")

            # Chegou ao fim da fila — não precisa continuar
            if ult_nsu_ret >= max_nsu_ret:
                log.info("Todos os documentos disponíveis foram processados.")
                nsu_atual = ult_nsu_ret
                break

            nsu_atual = ult_nsu_ret

            # Pausa obrigatória entre requisições dentro da mesma execução
            time.sleep(INTERVALO_ENTRE_LOTES)

        except Exception as e:
            log.error(f"Erro de comunicação com a SEFAZ: {e}")
            salvar_ultimo_nsu(nsu_atual)
            break

    # ==========================================
    # 7. EXPORTA CSV
    # ==========================================
    if notas_abastecimento:
        nome_arquivo = f"abastecimentos_ate_nsu_{nsu_atual}.csv"
        df = pd.DataFrame(notas_abastecimento)
        df.to_csv(nome_arquivo, index=False, sep=';', encoding='utf-8-sig')
        log.info(f"CSV gerado: {nome_arquivo} ({len(df)} registros)")
        return nome_arquivo

    log.info("Nenhum abastecimento encontrado nesta execução.")
    return None


if __name__ == '__main__':
    resultado = rodar_extracao()
    if resultado:
        print(f"\n✅ Arquivo gerado: {resultado}")
    else:
        print("\nℹ️  Nenhum arquivo gerado (sem abastecimentos ou erro).")