## 💡 Fluxo de Execução — Diagrama Mermaid

```mermaid
flowchart TD
    A[main.py] --> B[Importa funções do calculadora.py]
    B --> C[Executa somar(a, b)]
    B --> D[Executa subtrair(a, b)]
    B --> E[Executa multiplicar(a, b)]
    B --> F[Executa dividir(a, b)]
    C --> G[Exibe resultado no terminal]
    D --> G
    E --> G
    F --> G