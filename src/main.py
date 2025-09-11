"""
main.py

Este arquivo representa o ponto de entrada do projeto.

Boas práticas observadas:
- Importações organizadas
- Execução centralizada via `main()`
- Chamadas diretas às funções da calculadora
- Docstrings padronizadas para documentação automática com mkdocstrings
"""

from calculadora import somar, subtrair, multiplicar, dividir


def main() -> None:
    """
    Executa operações matemáticas usando funções importadas.

    Esta função demonstra o uso básico das operações disponíveis no módulo
    `calculadora.py`, imprimindo os resultados no terminal.

    Returns:
        None
    """
    a = 10
    b = 5

    print(f"Soma entre {a} e {b}: {somar(a, b)}")
    print(f"Subtração entre {a} e {b}: {subtrair(a, b)}")
    print(f"Multiplicação entre {a} e {b}: {multiplicar(a, b)}")
    print(f"Divisão entre {a} e {b}: {dividir(a, b)}")


if __name__ == "__main__":
    main()
