import re
from pyspark import SparkContext

sparkContext = SparkContext()
sparkContext.setLogLevel('error')

file = sparkContext.textFile("DSBook.txt")
words  = file.flatMap(lambda line: re.findall(r"[^\w\s]+|\w+", line))
counts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b:a + b)
output = counts.collect()
print(output)
print('#####################################################################')
print('#############################  OUTPUT  ##############################')
totalNumWord = 0
for word, count in output:
    print(word + ' : ' + str(count), end= ", ")
    totalNumWord = totalNumWord + count
print()
print("Total Number of words: " + str(totalNumWord))
print('#####################################################################')


