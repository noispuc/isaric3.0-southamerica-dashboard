# 📦 ISARIC HUB SA - South America Arboviroses Dashboard

This repository contains the source code related to South America Dashboard, a statistic visual tool based on VERTEX (ISARIC) containing data arboviroses surveillance system from South America. 
The following database are integrated:
- SINAN - Brazil.

## 🧠 O que você encontra aqui:
- Estrutura de diretórios
- Configuração de CI/CD
- Documentação com MkDocs
- Testes automatizados
- Padrões de `.env`, `.gitignore`, `pyproject.toml`, etc.
- Checklist de limpeza para novos projetos


## 📦 Estrutura

- `src/`: Código fonte principal
- `tests/`: Testes automatizados com `unittest`
- `docs/`: Documentação gerada com MkDocs
- `config/`: Arquivos `.env` para ambientes

## 📚 Documentação

A documentação é gerada com [MkDocs](https://www.mkdocs.org/) e inclui:

- Referência de código com `mkdocstrings`
- Diagramas em Mermaid
- Guia de início rápido

## ⚙️ Requisitos

- Python 3.10+
- [pip](https://pip.pypa.io/en/stable/)
- Ambiente virtual recomendado

```bash
python -m venv venv
source venv/bin/activate  # ou venv\Scripts\activate no Windows
pip install -r requirements.txt

## ✅ Checklist de Limpeza Pós-Clonagem

Após criar seu repositório a partir deste template:

- [ ] Remover testes em `tests/` se não forem usados
- [ ] Ajustar estrutura em `src/` conforme sua lógica de negócio
- [ ] Atualizar ou apagar arquivos em `docs/` se necessário
- [ ] Configurar `.env` a partir do `config/`
- [ ] Revisar `README.md` com a descrição específica do projeto
- [ ] Validar dependências e versões no `requirements.txt`

## 🔐 Credenciais do banco de dados (PostgreSQL) e uso de .env

Os scripts que acessam o banco  não devem ter usuário/senha escritos diretamente no código.
As credenciais são lidas de variáveis de ambiente, normalmente definidas via arquivo .env local, que NÃO é versionado.

As variáveis esperadas são:

PGUSER – usuário do PostgreSQL

PGPASSWORD – senha desse usuário

PGHOST – host do banco 

PGPORT – porta do banco (padrão: 5432)

PGDATABASE – nome do banco (ex.: datasus)

💻 Desenvolvimento local 

Crie um arquivo .env na sua máquina 

Preencha com as variáveis do seu ambiente, por exemplo:

PGUSER=seu_usuario_postgres
PGPASSWORD=sua_senha_postgres
PGHOST=localhost
PGPORT=5432
PGDATABASE=datasus


Certifique-se de que o arquivo .env não será commitado.
No .gitignore do projeto devem existir entradas semelhantes a:

# Arquivos reais de credenciais (não versionar)
.env
*.env


Ao rodar os scripts / dashboard, o processo irá ler essas variáveis de ambiente e montar a conexão com o banco automaticamente.

🔎 Importante: cada desenvolvedor é responsável por criar o seu próprio .env local com as credenciais que tiver.
Esse arquivo é apenas local e não deve ser enviado para o GitHub.