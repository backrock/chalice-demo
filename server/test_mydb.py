# -*- coding: UTF-8 -*-
import unittest
import boto3

import myabs
from myabs import BaseDB
# from server import myabs
     
myabs.myprint("testst")

def foo()->float:
    return "59"

dynamodb = myabs.boto3.resource('dynamodb')

class IntegerArithmeticTestCase(unittest.TestCase):
        def testAdd(self): # test method names begin with 'test'
            self.assertEqual((1 + 2), 3) 
            self.assertEqual(0 + 1, 1)
            print('assertEqual')
        def testMultiply(self):
            self.assertEqual((0 * 10), 0) 
            self.assertEqual((5 * 8), 40)
        def test_myabs(self):
            print('testmygg')
            assert(myabs.myabsf(-1) == 0,'test wrong')
            ###
            assert(myabs.myabsf(23.5) == -23,'tett')
            
        def test_01(self):
            assert foo() == 6 ,'判断当前值是否是6, 当前值是:%s'%foo()
            
            
class TestBaseDB(unittest.TestCase):
        def test_db(self):
            self.db = BaseDB(dynamodb)
            self.db.create_table("test_table")
            print(type(self.db))
        def test_select(self):
            dataset = self.db.select_table()
            print(f"the data sets: {type(dataset)} - {dataset}")
        def insert_item(self):
            item0 = {
                    "year": "2023-05-06",
                    "title": "My Dog Spot",
                    "AlbumTitle": "Hey Now",
                    "Price": Decimal('1.98'),
                    "Genre": "Country",
                    "CriticRating": Decimal('8.4')
                }
            self.db.insert_item(item0)
    



if __name__ == '__main__':
        unittest.main(['-s','test_myabs.py'])
