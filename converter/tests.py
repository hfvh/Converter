import unittest
from converter import converter


class TestCalc(unittest.TestCase):

    def test_get_colomns(self):
        expect =
        actual = calc.correct_expression('-sin(30)+.25-(-pow(2, 2))')
        self.assertEqual(expect, actual)