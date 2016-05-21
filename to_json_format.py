import json


with open('./ohsumed.88-91', 'r') as ohsu:
    content = []
    dict = {}
    cnt = 183
    while True:
        keys = ['.U', '.S', '.M', '.T', '.P', '.W']
        line = ohsu.readline().split()
        if not line:
            json.dump(content, open('./ohsu/ohsu%s.json' % cnt, 'w'), indent=1)
            break
        if line[0] == '.I':
            if int(line[1]) % 300 == 0:
                json.dump(content, open('./ohsu/ohsu%s.json' % cnt, 'w'), indent=1)
                cnt += 1
                content = []

            dict['.I'] = line[1]
        elif keys.__contains__(line[0]):
            text = ohsu.readline()
            dict[line[0]] = text[:-1]
        elif line[0] == '.A':
            authors = ohsu.readline()[:-1].split('; ')
            for i, author in enumerate(authors):
                authors[i] = author.replace('.', '').split(' ')[0]
            dict['.A'] = authors
            content.append(dict)
            dict = {}

