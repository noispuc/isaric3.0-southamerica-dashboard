"""
calculadora.py

Este módulo fornece funções matemáticas simples. 
Siga estas boas práticas:
- Sempre use nomes claros e verbos no infinitivo
- Inclua docstrings com exemplos do resultado
- Valide entradas quando necessário
"""

def somar(a: float, b: float) -> float:
    """
    Retorna a soma de dois números.

    Args:
        a (float): Primeiro número.
        b (float): Segundo número.

    Returns:
        float: Resultado de a + b.
    """
    return a + b

def subtrair(a: float, b: float) -> float:
    """
    Retorna a diferença entre dois números.

    Args:
        a (float): Minuendo.
        b (float): Subtraendo.

    Returns:
        float: Resultado de a - b.
    """
    return a - b

def multiplicar(a: float, b: float) -> float:
    """
    Retorna o produto de dois números.

    Args:
        a (float): Primeiro fator.
        b (float): Segundo fator.

    Returns:
        float: Resultado de a * b.
    """
    return a * b

def dividir(a: float, b: float) -> float:
    """
    Retorna a divisão de dois números.

    Args:
        a (float): Dividendo.
        b (float): Divisor.

    Returns:
        float: Resultado de a / b.

    Raises:
        ValueError: Se b for igual a zero.
    """
    if b == 0:
        raise ValueError("Divisão por zero não é permitida.")
    return a / b
