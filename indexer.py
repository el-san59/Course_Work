#!/usr/bin/env python
# encoding: utf-8
from __future__ import print_function
from nltk.stem.lancaster import LancasterStemmer
import re
import json
import math
from collections import Counter
alphabet = re.compile(u'[a-zA-Z]+')


def hashcode(s):
    h = 0
    for c in s:
        h = (31 * h + ord(c)) & 0xFFFFFFFF
    return (((h + 0x80000000) & 0xFFFFFFFF) - 0x80000000) % 50


def mapper(shard, doc_counter):
    st = LancasterStemmer()
    with open(shard, "r") as f:
        ohsu = json.JSONDecoder().decode(f.read())
        output_values = []
        doc_counter.add(len(ohsu))
        for article in ohsu:
            output_values += [(w, (article[".I"], 'a')) for w in article[".A"]]
            output_values += [(st.stem(w), (article[".I"], 't')) for w in alphabet.findall(article[".T"].lower())]
            if article.get('.W') is not None:
                body_words = (w for w in alphabet.findall(article[".W"].lower()))
                output_values += [(st.stem(w), (article[".I"], 'w')) for w in body_words]
    return output_values


def reducer(item, max_doc_count):
    key, values = item
    doc_count = len(set(v[0] for v in values))
    values = map(lambda x: tuple(x), values)

    idf = math.log(max_doc_count.value/float(doc_count))
    reduced = Counter(values)

    output_result = []
    for (id, t), n in dict(reduced).items():
        output_result += [(id, t, (1 + math.log(n))*idf,)]

    return key, output_result


def print_res(x):
    with open("./index_result/%s_part.txt" % hashcode(x[0]), "a") as f:
        print(x, file=f)


def run(sc):
    with open('data/shards.txt', 'r') as f:
        files = list(f.read().splitlines())
    doc_counter = sc.accumulator(0)
    data = sc.parallelize(files)
    data = data.flatMap(lambda shard: mapper(shard, doc_counter))
    max_doc_count = sc.broadcast(348566)
    data = data.groupByKey()
    data = data.map(lambda word: reducer(word, max_doc_count))
    data = data.sortByKey()
    data = data.repartition(1)
    data.foreach(print_res)