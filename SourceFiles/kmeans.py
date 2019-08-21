import sys
from pyspark import SparkConf, SparkContext
from song import song
from cluster import cluster
from collections import defaultdict
from itertools import combinations

filename="RS"
SparkContext.setSystemProperty('spark.executor.memory', '6g')

sc=SparkContext("local","Lappname")
partition_size=10

def find_cluster(x):
    d={}
    x=list(x)
    if len(x)>0:
        for i in x:
            if i.id not in cid:
                js=-1
                c=None
                for (k,j) in enumerate(cen):
                    y=jaccard(j.centroid.signature,i.signature)
                    if y>js:
                        js=y
                        c=j.centroid.id
                if c in d:
                    d[c].append(i)
                else:
                    d[c]=[i]

    rt=[]
    for i in d:
        rt.append((i,d[i]))
    return rt

def jaccard(a,b):
	inter = set(a).intersection(set(b))
	inter_size = len(inter)
	union = set(a).union(set(b))
	union_size = len(union)
	return float(inter_size)/union_size

min_hash_file=sc.textFile(filename+"_hashes.txt",partition_size).map(lambda x: x.split("~"))
songs = min_hash_file.map(lambda x: song(x[0],x[1]))

def jaccard_cluster_updater(clusters):
    for cluster in clusters:
        for song in cluster.members:
            score=0
            for other in cluster.members:
                if song.id!=other.id:
                    score+=jaccard(song.signature,other.signature)
            print song.id,score
            song.similarity_score=score
        cluster.update_centroid()
        yield cluster
def lsh_cluster_updater(clusters):
    for cluster in clusters:
        signature_length=len(cluster.centroid.signature)
        number_of_bands=50
        for song in cluster.members:
            chunk_size = signature_length / number_of_bands
            song_signature = song.signature
            chunked_Strings = [song_signature[i:i + chunk_size] for i in range(0, signature_length, chunk_size)]
            for i,each_chunk in enumerate(chunked_Strings):
                if each_chunk!='':
                    yield (cluster.id,i),(song,each_chunk)

def find_candidates(iterator):
    for each in iterator:
        ((cluster,band_id),song_sign_pairs)=each
        the_dict={}
        for pair in list(song_sign_pairs):
            song,chunk=pair
            chunk="-".join(chunk)
            if chunk in the_dict:
                the_dict[chunk].append(song)
            else:
                the_dict[chunk]=[song]
        for key in the_dict:
            list_songs=the_dict[key]
            for i,actual_song in enumerate(list_songs):
                yield (cluster,actual_song.id),list_songs[:i]+list_songs[i+1:]
				
def getting_similar_pairs(iterator):
    for each in iterator:
        (cluster_id,song_id),similar_songs=each
        song_score=0
        cluster=cen[0]
        for i in cen:
            if i.id==cluster_id:
                cluster=i
        song=cluster.centroid
        flag=0
        for member in cluster.members:
            if member.id==song_id:
                song=member
                flag=1
                break
        if flag>0 and len(similar_songs)>0:
            score = 0
            other_len=len(similar_songs)
            for other in similar_songs:
                score += jaccard(song.signature, other.signature)
            song_score=float(score)/other_len
        print song.id, song_score
            # song.similarity_score = song_score
        yield cluster_id,(song_id,song_score)

def get_old_cen(cen):
    old_cen={}
    for cluster in cen:
        cen_members = set([member.id for member in cluster.members])
        old_cen[cluster.id]=cen_members.copy()
    return old_cen

def convergence(old_cid,new_cid,old_cen,cen):
    global flag
    if old_cid==new_cid:
        counter=0
        for cluster in cen:
            cen_members = set([member.id for member in cluster.members])
            if old_cen[cluster.id]==cen_members:
                counter+=1
        if counter==len(cen):
            flag=1

G_K=5
cent=songs.takeSample(True,G_K,9)
cen=[]
cid=set()
for i in cent:
    cen.append(cluster(i))
    cid.add(i.id)

# print cid
flag=0
number_of_iteration=6
old_cid={}
old_cen={}
iter_counter=0
while flag<1:
#Cluster assignment
    cl_as=songs.mapPartitions(lambda x:find_cluster(x)).reduceByKey(lambda x,y:x+y).collectAsMap()
    for i in cen:
        i.members=[i.centroid]
        if i.centroid.id in cl_as:
            i.members.extend(cl_as[i.centroid.id])
    print("Centroid assigned")
#Cluster updation
    cid=set()
	parallel_updation = sc.parallelize(cen, partition_size).mapPartitions(lsh_cluster_updater).groupByKey().partitionBy(partition_size).mapPartitions(find_candidates) \
        .aggregateByKey([], lambda x, y: x + y, lambda x, y: x + y).partitionBy(partition_size).mapPartitions(getting_similar_pairs).groupByKey().mapValues(lambda x: list(x))

    collector=parallel_updation.collect()
    for cluster_id,sender in collector:
        for cluster in cen:
            if cluster_id==cluster.id:
                cluster.update_centroid(sender)
                break
    for cluster in cen:
        cid.add(cluster.centroid.id)
    print("cid",cid)
    convergence(old_cid,cid,old_cen,cen)
    old_cid=cid.copy()
    old_cen=get_old_cen(cen)
    iter_counter+=1
output_file=open(filename+"_kmeans.txt","w")

for id in old_cen:
    list_cluster_elements=list(old_cen[id])
    output_file.write(str(id)+":"+",".join(list_cluster_elements)+"\n")
output_file.close()


