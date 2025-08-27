# 🤖 Bot Copilot – Geração automatizada de minutas via DANFE

Este projeto automatiza o preenchimento de minutas a partir de arquivos PDF de DANFE, gerando documentos formatados em Excel e exportando para PDF com dados organizados, valores formatados e múltiplas páginas.

---

## 🚀 Funcionalidades

- Extração de dados de DANFE (emitente, remetente, produtos)
- Preenchimento de tokens em template Excel (`.xlsx`)
- Formatação de valores no padrão brasileiro (`R$ 1.099,05`)
- Cálculo de totais no backend
- Geração de PDF final com múltiplas páginas
- Integração com scraping de RATs por ocorrência e produto (em desenvolvimento)

---

## 🧰 Tecnologias utilizadas

- Python 3.11
- [Spire.XLS](https://www.e-iceblue.com/Introduce/spire-xls-for-python.html)
- pdfplumber
- Docker
- Git

---

## 📁 Estrutura do projeto

app/ 
├── bot.py # Orquestra o fluxo principal 
├── excel_filler_spire.py # Preenche o template Excel e exporta para PDF 
├── danfe_parser.py # Extrai dados do PDF (texto + tabelas) 
├── rat_search.py # Consulta RATs por ocorrência e produto
├── services/ # Módulos auxiliares 
└── templates/ # Template Excel (.xlsx) com tokens

## 🧪 Como rodar localmente

docker compose up --build

## 📌 Versão atual

**v1.0.1** – Refinamento: preenchimento completo da minuta com tokens e formatação brasileira

## 📍 Próximos passos

- Revisar lógica de associação de RATs por produto
    
- Criar testes automatizados para validação de extração
    
- Melhorar tratamento de exceções e logs
    
- Adicionar interface de upload e visualização
 

## 👨‍💻 Autor

Rodrigo Lucas Pinheiro 
Cosmópolis, SP – Brasil