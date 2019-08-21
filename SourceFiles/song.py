class song:
    id=0
    year=""
    genre=""
    shingle_list=[]
    similar_songs=[]
    similarity_score=0
    signature=""
    n=0

    def __init__(self,id,signature):
        self.id=id
        self.signature=signature.split(",")

    def assign_similar_songs(self,similars):
        self.similar_songs=similars

    def printer(self):
        print self.id
       
    def __eq__(self,other):
        return self.id==other.id

