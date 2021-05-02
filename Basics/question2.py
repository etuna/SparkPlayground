import re
from pyspark import SparkContext

def split(word):
    return [char for char in word]

sparkContext = SparkContext()
sparkContext.setLogLevel('error')

file = sparkContext.textFile("DSBook.txt")
letters = file.flatMap(lambda line: list(line)).filter(lambda letter: letter.isalpha()).map(lambda letter: letter)

counts = letters.map(lambda letter: (letter, 1)).reduceByKey(lambda a, b:a + b)
output = counts.collect()
print(output)
print('#####################################################################')
print('#############################  OUTPUT  ##############################')
for letter, count in output:
    print(letter + ' : ' + str(count), end= ", ")
print()
print('#####################################################################')


