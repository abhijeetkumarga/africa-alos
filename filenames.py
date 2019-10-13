NS = ['N40','N35','N30','N25','N20','N15','N10','N05','N00','S05','S10','S15','S20','S25','S30']
EW = ['W020','W015','W010','W005','E000','E005','E010','E015','E020','E025','E030','E035','E040','E045','E050']

final_mesh = []

for i, n_i in enumerate(NS):
    for j, n_j in enumerate(EW):
        final_mesh.append(n_i+n_j)
