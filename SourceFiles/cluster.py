GLOBAL_ID=0
class cluster:
    centroid=None
    members=[]
    id=0
    def __init__(self,song):
        global GLOBAL_ID
        self.id=GLOBAL_ID
        GLOBAL_ID+=1
        self.centroid=song
        self.members=[song]

    def add_song(self,song):
        self.members.append(song)

    def update_centroid(self,list_of_song_similarity):
        cur_max=0
        candidate=self.centroid
        for song_id,score in list_of_song_similarity:
            for song in self.members:
                if song_id==song.id and cur_max<score:
                    candidate=song
                    cur_max=score
                    break
        self.centroid=candidate

    def printer(self):
        print "--"*25
        print "id",self.centroid.id
        for i in self.members:
            i.printer()
