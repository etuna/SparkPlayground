import re
from pyspark import SparkContext
import os
import shutil

sparkContext = SparkContext()
sparkContext.setLogLevel('error')

file = sparkContext.textFile("DSBook.txt")

loweredText = file.map(lambda line: line.lower())
loweredText.saveAsTextFile('words_lower.txt')

os.rename('words_lower_temp.txt/part-00000', 'words_lower.txt')
shutil.rmtree('words_lower_temp.txt')
