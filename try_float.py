import timeit

data = [{"score": 4744},
        {"score": 1743},
        {"score": 2233},
        {"score": 9932},
        ]


def new_face_1(data):
    match_score = 95
    for x in data:
        score = float(x.get('score',0))/100
        if score > match_score:
            return score

def new_face_2(data):
    match_score = 95
    for x in data:
        score = (x.get('score',0))/100
        if score > match_score:
            return score


def new_face_3(data):
    match_score = 95
    match_score *= 100
    for x in data:
        score = x.get('score', 0)
        if score > match_score:
            return score

print(timeit.timeit(lambda:new_face_1(data), number=100000))
print(timeit.timeit(lambda:new_face_2(data), number=100000))
print(timeit.timeit(lambda:new_face_3(data), number=100000))
