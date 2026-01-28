# Lógica de Cálculo da Página de Inovações

Este documento detalha como os cálculos da página de **Inovações** funcionam no sistema, com base na análise do código fonte (`app.js`).

## 1. Base de Clientes (O Denominador)

A porcentagem de cobertura é calculada sobre uma base de **"Clientes Ativos"**. Para um cliente entrar nessa contagem (o 100%), ele deve obedecer a todas as regras abaixo:

*   **Filtros de Tela:** O cliente deve pertencer à Cidade, Filial, Supervisor e Vendedor selecionados no topo da página.
*   **Regra de RCA (Exclusão):** Clientes dos RCAs **300** e **306** são sempre excluídos.
*   **Regra de Atividade (Inclusão):** O cliente é considerado "Ativo" se atender a **pelo menos uma** das condições a seguir:
    1.  Ser da rede **"AMERICANAS"**.
    2.  **OU** Ter o RCA principal diferente de **"53"** (Balcão).
        *   *Importante:* Isso significa que clientes de vendedores de rota (ex: RCA 123) contam na base para o cálculo de % **mesmo que não tenham comprado nada** no mês atual.
    3.  **OU** Ter efetuado alguma compra no mês atual (mesmo que seja de RCA 53).

## 2. Positivação (O Numerador)

Este número representa quantos dos clientes da "Base" acima compraram os produtos de inovação.

*   **Critério:** O cliente é contado como "Positivado" se comprou **pelo menos 1 produto** que pertença à categoria/lista de inovações selecionada.
*   **Vendas vs. Bonificação:** O sistema soma quem comprou (Venda) e quem recebeu Bonificação. Se um cliente fez os dois, ele conta apenas uma vez (cliente único).
*   **Filtro de Vendedor:** Se você filtrar por um Vendedor específico, a positivação só conta se a venda do produto de inovação foi feita *por aquele vendedor*.

## 3. Fórmulas Matemáticas

As porcentagens exibidas nos cards e na tabela seguem estas fórmulas:

### A. Cobertura Atual (%)
> "De todos os meus clientes ativos, quantos compraram inovação este mês?"

$$
\text{Cobertura Atual} = \left( \frac{\text{Clientes Positivados (Mês Atual)}}{\text{Total Clientes Ativos (Base)}} \right) \times 100
$$

### B. Cobertura Anterior (%)
> "Desses mesmos clientes ativos que tenho hoje, quantos compraram inovação no mês passado?"

$$
\text{Cobertura Anterior} = \left( \frac{\text{Clientes Positivados (Histórico)}}{\text{Total Clientes Ativos (Base)}} \right) \times 100
$$

*Nota:* O denominador é a base de clientes **de HOJE**. Isso serve para mostrar a evolução da *carteira atual*, ignorando clientes que saíram ou mudaram de rota no passado.

### C. Variação (%)
> "Quanto minha cobertura cresceu ou caiu em relação ao mês passado?"

$$
\text{Variação} = \left( \frac{\text{Cobertura Atual} - \text{Cobertura Anterior}}{\text{Cobertura Anterior}} \right) \times 100
$$

*   Se a cobertura anterior for 0 e a atual for positiva, o sistema exibe como **"Novo"**.

## Resumo Prático

Se você tem uma carteira com **100 clientes** (RCA != 53):
1.  Se **20** compraram o produto "X" este mês -> Cobertura Atual = **20%**.
2.  Se desses mesmos 100, apenas **10** tinham comprado "X" no mês passado -> Cobertura Anterior = **10%**.
3.  Sua Variação será de **+100%** (dobrou a cobertura).
