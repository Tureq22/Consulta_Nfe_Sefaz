# Extrator de NF-e: Abastecimento (Diesel e ARLA)

Sistema em Python para consultar a SEFAZ via Certificado A1 e extrair relatórios precisos de notas fiscais de abastecimento (Diesel e ARLA 32) emitidas contra o CNPJ da empresa. 

O script utiliza paginação por NSU para garantir que nenhuma nota seja duplicada ou perdida entre as execuções, filtrando os resultados baseados no NCM dos produtos.

## Tecnologias
* Python 3
* PyNFe
* Pandas
* python-dotenv

## Como configurar e rodar

1. Clone o repositório:
   ```bash
   git clone [https://github.com/seu-usuario/seu-repositorio.git](https://github.com/seu-usuario/seu-repositorio.git)