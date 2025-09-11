# tests/test_calculadora.py
import unittest
from calculadora import somar, subtrair, multiplicar, dividir

class TestCalculadora(unittest.TestCase):

    def test_somar(self):
        self.assertEqual(somar(2, 3), 5)

    def test_subtrair(self):
        self.assertEqual(subtrair(10, 4), 6)

    def test_multiplicar(self):
        self.assertEqual(multiplicar(3, 7), 21)

    def test_dividir(self):
        self.assertEqual(dividir(15, 3), 5)

    def test_divisao_por_zero(self):
        with self.assertRaises(ValueError):
            dividir(10, 0)

if __name__ == "__main__":
    unittest.main()
