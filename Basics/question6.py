import re
from pyspark import SparkContext
from datetime import datetime
import matplotlib
import matplotlib.pyplot as plot
import numpy as np
import zipfile


sparkContext = SparkContext()
sparkContext.setLogLevel('error')



# numbers.txt
startTime = datetime.now()
file = sparkContext.textFile("Numbers.zip/numbers.txt")
numbers = file.flatMap(lambda line: line.split(" "))
print('#####################################################################')
print('#############################  OUTPUT  ##############################')
print('[NUMBERS.TXT] [MEAN] :' +str(numbers.map(lambda num: float(num)).mean()))
print('[NUMBERS.TXT] [STDEV] :'+str(numbers.map(lambda num: float(num)).stdev()))
print('[NUMBERS.TXT] [VARIANCE] :'+str(numbers.map(lambda num: float(num)).variance()))
endTime = datetime.now()
duration = endTime - startTime

# numbers2.txt
startTime2 = datetime.now()
file2 = sparkContext.textFile("numbers2.txt")
numbers2 = file2.flatMap(lambda line: line.split(" "))
print('#####################################################################')
print('#############################  OUTPUT  ##############################')
print('[NUMBERS2.TXT] [MEAN] :'+str(numbers2.map(lambda num: float(num)).mean()))
print('[NUMBERS2.TXT] [STDEV] :'+str(numbers2.map(lambda num: float(num)).stdev()))
print('[NUMBERS2.TXT] [VARIANCE] :'+str(numbers2.map(lambda num: float(num)).variance()))
endTime2 = datetime.now()
duration2 = endTime2 - startTime2


# numbers4.txt
startTime4 = datetime.now()
file4 = sparkContext.textFile("numbers4.txt")
numbers4 = file4.flatMap(lambda line: line.split(" "))
print('#####################################################################')
print('#############################  OUTPUT  ##############################')
print('[NUMBERS4.TXT] [MEAN] :'+str(numbers4.map(lambda num: float(num)).mean()))
print('[NUMBERS4.TXT] [STDEV] :'+str(numbers4.map(lambda num: float(num)).stdev()))
print('[NUMBERS4.TXT] [VARIANCE] :'+str(numbers4.map(lambda num: float(num)).variance()))
endTime4 = datetime.now()
duration4 = endTime4 - startTime4


# numbers8.txt
startTime8 = datetime.now()
file8 = sparkContext.textFile("numbers8.txt")
numbers8 = file8.flatMap(lambda line: line.split(" "))
print('#####################################################################')
print('#############################  OUTPUT  ##############################')
print('[NUMBERS8.TXT] [MEAN] :'+str(numbers8.map(lambda num: float(num)).mean()))
print('[NUMBERS8.TXT] [STDEV] :'+str(numbers8.map(lambda num: float(num)).stdev()))
print('[NUMBERS8.TXT] [VARIANCE] :'+str(numbers8.map(lambda num: float(num)).variance()))
endTime8 = datetime.now()
duration8 = endTime8 - startTime8

# numbers16.txt
startTime16 = datetime.now()
file16 = sparkContext.textFile("numbers16.txt")
numbers16 = file16.flatMap(lambda line: line.split(" "))
print('#####################################################################')
print('#############################  OUTPUT  ##############################')
print('[NUMBERS16.TXT] [MEAN] :'+str(numbers16.map(lambda num: float(num)).mean()))
print('[NUMBERS16.TXT] [STDEV] :'+str(numbers16.map(lambda num: float(num)).stdev()))
print('[NUMBERS16.TXT] [VARIANCE] :'+str(numbers16.map(lambda num: float(num)).variance()))
endTime16 = datetime.now()
duration16 = endTime16 - startTime16

# numbers32.txt
startTime32 = datetime.now()
file32 = sparkContext.textFile("numbers32.txt")
numbers32 = file32.flatMap(lambda line: line.split(" "))
print('#####################################################################')
print('#############################  OUTPUT  ##############################')
print('[NUMBERS32.TXT] [MEAN] :'+str(numbers32.map(lambda num: float(num)).mean()))
print('[NUMBERS32.TXT] [STDEV] :'+str(numbers32.map(lambda num: float(num)).stdev()))
print('[NUMBERS32.TXT] [VARIANCE] :'+str(numbers32.map(lambda num: float(num)).variance()))
endTime32 = datetime.now()
duration32 =  endTime32- startTime32


sizes = ['1', '2','4', '8', '16', '32']
durations = [duration.seconds, duration2.seconds, duration4.seconds, duration8.seconds, duration16.seconds, duration32.seconds]
fig, ax = plot.subplots()
x = np.arange(len(sizes))
width = 0.10

ax.bar(x-width, durations)
ax.set_label('label')
ax.set_xlabel('sizes')
ax.set_ylabel('Duration')
ax.set_title('Durations of execution')
ax.set_xticks(x)
ax.set_xticklabels(sizes)
ax.legend(labels=['duration', 'sizes'])





fig.tight_layout()
plot.show()

plot.savefig('test2.png')




