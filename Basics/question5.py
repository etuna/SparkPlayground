import re
from pyspark import SparkContext

sparkContext = SparkContext()
sparkContext.setLogLevel('error')

file = sparkContext.textFile("DSBook.txt")

loweredText = file.map(lambda line: line.replace(" ", '-'))
loweredText.saveAsTextFile('words-.txt')
