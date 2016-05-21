import argparse
import re
from nltk.stem.lancaster import LancasterStemmer
from ast import literal_eval
from collections import defaultdict


alphabet = re.compile(u'[a-zA-Z]+')
st = LancasterStemmer()


def hashcode(s):
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return (((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000) % 50


def binary_search(index_data, word, l, r):
    if r < l:
        return None
    m = (r+l)//2
    if index_data[m][0] < word:
        return binary_search(index_data, word, m+1, r)
    elif index_data[m][0] > word:
        return binary_search(index_data, word, l, m-1)
    else:
        return index_data[m]


parser = argparse.ArgumentParser(description="Ranked Boolean search")
parser.add_argument("--query", help="Query terms separated by a space", required=True)
parser.add_argument("--weights", help="Weight divided by comma. First - authors, second - title, third - body",
                    required=True)

args = parser.parse_args()
weights = args.weights.split(',')
words = args.query.lower().split(' ')
stem_query = [st.stem(w) for w in words]
num_words = len(stem_query)

weight_dict = {"a": float(weights[0]), "t": float(weights[1]), "w": float(weights[2])}
indexes = []
for i in range(num_words):
    with open("./index_result/%s_part.txt" % hashcode(stem_query[i]), "r") as f:
        index_data = []
        for line in f:
            data = literal_eval(line)
            index_data.append(data)
    indexes.append(binary_search(index_data, stem_query[i], 0, len(index_data)))

res = defaultdict(int)
for node in indexes:
    for doc in node[1]:
        res[doc[0]] += doc[2]*weight_dict[doc[1]]

res = list(sorted(res.items(), key=lambda x: -x[1]))

with open("result.txt", "w") as f:
    print(*res, sep="\r\n", file=f)



