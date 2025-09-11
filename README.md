# 📦 ISARIC HUB SA Template - Repositório de Referência Técnica

Este repositório serve como exemplo **didático e técnico** da estrutura padrão adotada pelos projetos da organização NOIS.
Atualize o readme para reflita as informações corretas do projeto.

## 🧠 O que você encontra aqui:
- Estrutura de diretórios
- Configuração de CI/CD
- Documentação com MkDocs
- Testes automatizados
- Padrões de `.env`, `.gitignore`, `pyproject.toml`, etc.
- Checklist de limpeza para novos projetos

Ao criar um novo projeto, use este repositório como **template** e ajuste conforme necessário.

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
