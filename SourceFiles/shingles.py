import csv
from collections import defaultdict
import json


filename="RS"
data_file = open(filename+"_data.txt", "r")
global_shingles = {}
file = open(filename+"_shingles.txt", 'w')

n = 4
cnt = 0
for song_cnt,i in enumerate(data_file):
	lyrics = i.split(",")[4].strip()
	lis=[]
	shing = [lyrics[j:j + n] for j in range(len(lyrics))]
	for i in shing:
		if i not in global_shingles:
			global_shingles[i] = cnt
			cnt+=1
		lis.append(str(global_shingles[i]))
	file.write(str(song_cnt) + "," + ",".join(lis) + "\n")

file.close()
json.dump(global_shingles, open("global_shingles.txt", "w"))

# for key in song_shingle:
# 	x = key
# 	val = song_shingle[x]
# 	val1 = map(lambda x: str(x), val)
# 	w = ",".join(val1)
# 	# print(val)
# 	file.write(str(x)+","+w+"\n")
#
