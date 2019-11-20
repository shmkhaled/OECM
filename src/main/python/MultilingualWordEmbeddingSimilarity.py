# Created by Shimaa 19.Nov.2019
import io
import numpy as np
from scipy.spatial import distance
import csv



def load_vec(emb_path, nmax=50000):
    vectors = []
    word2id = {}
    with io.open(emb_path, 'r', encoding='utf-8', newline='\n', errors='ignore') as f:
        next(f)
        for i, line in enumerate(f):
            word, vect = line.rstrip().split(' ', 1)
            vect = np.fromstring(vect, sep=' ')
            assert word not in word2id, 'word found twice'
            vectors.append(vect)
            word2id[word] = len(word2id)
            if len(word2id) == nmax:
                break
    id2word = {v: k for k, v in word2id.items()}
    embeddings = np.vstack(vectors)
    return embeddings, id2word, word2id


# src_path = '/home/shimaa/MUSE/dumped/debug/TrainingResults-en-es/vectors-en.txt'
# tgt_path = '/home/shimaa/MUSE/dumped/debug/TrainingResults-en-es/vectors-es.txt'
# src_path = '/home/shimaa/MUSE/data/vectors/wiki.multi.en.vec'
# tgt_path = '/home/shimaa/MUSE/data/vectors/wiki.multi.es.vec'
src_path = '/home/shimaa/MUSE/data/vectors/wiki.multi.de.vec'
tgt_path = '/home/shimaa/MUSE/data/vectors/wiki.multi.en.vec'

nmax = 50000  # maximum number of word embeddings to load

src_embeddings, src_id2word, src_word2id = load_vec(src_path, nmax)
tgt_embeddings, tgt_id2word, tgt_word2id = load_vec(tgt_path, nmax)


def getWordSim(word1, word2, src_emb, src_id2word, tgt_emb, tgt_id2word):
    word2id1 = {v: k for k, v in src_id2word.items()}
    word_emb1 = src_emb[word2id1[word1]]
    # print("Word embedding for \"%s\":" % word1, word_emb1)
    word2id2 = {v: k for k, v in tgt_id2word.items()}
    word_emb2 = tgt_emb[word2id2[word2]]
    # print("Word embedding for \"%s\":" % word1, word_emb2)
    xx = 1 - distance.cosine(word_emb1, word_emb2)
    return xx


def getSimilarity(phrase, emb, id2word):
    word2id = {v: k for k, v in id2word.items()}
    word_emb = np.zeros((300, ), dtype='float32')
    n_words = 0
    for w in phrase.split():
        if w in word2id:
            n_words += 1
            word_emb = np.add(word_emb, emb[word2id[w]])
    if (n_words > 0):
        word_emb = np.divide(word_emb, n_words)
    return word_emb


# s1_afv = getSimilarity('leiter workshops', src_embeddings, src_id2word)
# s2_afv = getSimilarity('workshop chair', tgt_embeddings, tgt_id2word)
# sim = 1 - distance.cosine(s1_afv, s2_afv)
# print('Similarity = ', sim)

with open('/home/shimaa/OECM/Output/Conference_de_classes') as f:
    sourceClasses = f.read().splitlines()

with open('/home/shimaa/OECM/Output/Edas_en_classes') as f:
    targetClasses = f.read().splitlines()

sim = []
for s in sourceClasses:
    for t in targetClasses:
        vector_s = getSimilarity(s, src_embeddings, src_id2word)
        vector_t = getSimilarity(t, tgt_embeddings, tgt_id2word)
        similarity = 1 - distance.cosine(vector_s, vector_t)
        sim.append((s, t, similarity))
# print(sim)

with open('/home/shimaa/OECM/Output/MUSE-Results/Conference_deXEdas_en(Classes).txt', 'w') as f:
    for item in sim:
        f.write("{}\n".format(item))


# with open('/home/shimaa/OECM/Output/MUSE_Results.csv', 'wb') as resultFile:
#     wr = csv.writer(resultFile, dialect='excel')
#     wr.writerows(sim)

# with open('/home/shimaa/OECM/Output/MUSE_Results.csv', 'wb') as resultFile:
#     wr = csv.writer(resultFile, dialect='excel')
#     for item in sim:
#         wr.writerow([item, ])
