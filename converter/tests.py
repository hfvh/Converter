import unittest
import converter.converter


class TestCalc(unittest.TestCase):

    def test_get_column(self):
        expect = ['id1', 'id2', 'id3']
        actual = converter.get_column([{'id1': 'a', 'id2': 'a', 'id3': 'a'}, {'id1': 'b', 'id2': 'b', 'id3': 'b'}])
        self.assertEqual(expect, actual)

    def test_create_schema(self):
        name = 'file.csv'
        columns = ['id1', 'id2', 'id3']
        expect = pattern = {"type": "record", "namespase": "Tutorial", "name": f"{name[:-4]}",
                            "fields": [{"name": 'id1', "type": "string"}, {"name": 'id2', "type": "string"},
                                       {"name": 'id3', "type": "string"}]}
        actual = converter.create_schema(columns, name)
        self.assertEqual(expect, actual)