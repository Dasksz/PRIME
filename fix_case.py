
import re

def fix_case_in_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    replacements = {
        # General data properties
        r'item\.VLVENDA': 'item.vlvenda',
        r'item\.TOTPESOLIQ': 'item.totpesoliq',
        r'item\.VLBONIFIC': 'item.vlbonific',
        r'item\.CODCLI': 'item.codcli',
        r'item\.NOME': 'item.nome',
        r'item\.SUPERV': 'item.superv',
        r'item\.DESCRICAO': 'item.descricao',
        r'item\.PRODUTO': 'item.produto',
        r'item\.FORNECEDOR': 'item.fornecedor',
        r'item\.CODFOR': 'item.codfor',
        r'item\.OBSERVACAOFOR': 'item.observacaofor',
        r'item\.QTVENDA': 'item.qtvenda',
        r'item\.QTVENDA_EMBALAGEM_MASTER': 'item.qtvenda_embalagem_master',
        r'item\.DTPED': 'item.dtped',

        # Row properties in renderTable
        r'row\.PEDIDO': 'row.pedido',
        r'row\.CODCLI': 'row.codcli',
        r'row\.NOME': 'row.nome',
        r'row\.FORNECEDORES_STR': 'row.fornecedores_str',
        r'row\.DTPED': 'row.dtped',
        r'row\.DTSAIDA': 'row.dtsaida',
        r'row\.TOTPESOLIQ': 'row.totpesoliq',
        r'row\.VLVENDA': 'row.vlvenda',
        r'row\.POSICAO': 'row.posicao',

        # Client properties
        r"client\['Código'\]": "client.codigo_cliente",
        r"client\['código'\]": "client.codigo_cliente",
        r"client\.dataCadastro": "client.datacadastro",
        r"client\.razaoSocial": "client.razaosocial",
        r"client\.ultimaCompra": "client.ultimacompra",

        # Order info properties
        r'orderInfo\.VLVENDA': 'orderInfo.vlvenda',

        # Sale object 's' properties
        r's\.PRODUTO': 's.produto',
        r's\.QTVENDA_EMBALAGEM_MASTER': 's.qtvenda_embalagem_master',
        r's\.DTPED': 's.dtped',
        r's\.VLVENDA': 's.vlvenda',
        r's\.FILIAL': 's.filial',
        r's\.CODFOR': 's.codfor',


        # sale object properties
        r'sale\.DTPED': 'sale.dtped',
        r'sale\.OBSERVACAOFOR': 'sale.observacaofor',

        # aggregatedOrders properties
        r'a\.DTPED': 'a.dtped',
        r'b\.DTPED': 'b.dtped',
    }

    # A few specific multi-line or tricky cases that regex handles better
    # In openModal: const unitPrice = (item.QTVENDA > 0) ? (item.VLVENDA / item.QTVENDA) : 0;
    content = re.sub(r'const unitPrice = \(item\.qtvenda > 0\) \? \(item\.VLVENDA / item\.qtvenda\) : 0;', r'const unitPrice = (item.qtvenda > 0) ? (item.vlvenda / item.qtvenda) : 0;', content)

    # In updateStockView: allCombinedSales.filter(s => s.PRODUTO === productCode);
    content = re.sub(r's\.PRODUTO === productCode', r's.produto === productCode', content)

    # In updateCoverageView: s.FILIAL === filial
    content = re.sub(r's\.FILIAL === filial', r's.filial === filial', content)

    # In updateCoverageView: s.CODFOR
    content = re.sub(r's\.CODFOR', r's.codfor', content)

    # General replacements using the dictionary
    for pattern, replacement in replacements.items():
        # Use word boundaries (\b) to avoid replacing parts of longer names
        # e.g., avoid changing 'item.SOMEVLVENDA'
        content = re.sub(r'\b' + pattern + r'\b', replacement, content)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)

if __name__ == "__main__":
    fix_case_in_file("ONDASH.html")
    print("Case sensitivity fixes applied to ONDASH.html")
