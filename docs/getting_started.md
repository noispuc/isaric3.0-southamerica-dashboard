# 🧭 Getting Started — Guia para Desenvolvimento Python

Este guia orienta você a preparar seu ambiente, instalar dependências, executar o projeto e contribuir seguindo as boas práticas adotadas pela equipe NOIS.

---

## 🐍 1. Instale o Python

- Versão recomendada: **Python 3.10 ou superior**
- Verifique com: `python --version`

Baixe em [python.org](https://www.python.org/downloads)

🔍 Recomendamos seguir o guia de estilo [PEP 8](https://peps.python.org/pep-0008/) para manter o código legível, limpo e consistente.

---

## 💻 2. Configure o VSCode

- Instale o [Visual Studio Code](https://code.visualstudio.com/)
- Instale as extensões:
  - Python (Microsoft)
  - Pylance
  - Markdown Preview Enhanced
- Garanta que o VSCode reconheça seu Python (Ctrl + Shift + P → "Python: Select Interpreter")

---

## ⚙️ 3. Clone o repositório
```bash
git clone https://github.com/NOIS/testgithub.git
cd testgithub

---

## 🧪 4. Crie e ative seu ambiente virtual
**Windows:**
```bash
python -m venv venv
venv\Scripts\activate

**Mac/Linux:**
```bash
python3 -m venv venv
source venv/bin/activate

---

## 📦 5. Instale as dependências
```bash
pip install -r requirements.txt

---

## 6. Execute o projeto
```bash
python src/main.py

---

## 🧪 7. Rode os testes unitários
```bash
python -m unittest discover tests/

📚 Os testes automatizados são escritos utilizando o módulo [unittest](https://docs.python.org/3/library/unittest.html), que já vem embutido no Python.

---

## 📚 8. Visualize a documentação local com MkDocs
```bash
mkdocs serve

Acesse: http://localhost:8000

