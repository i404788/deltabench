import os 
import sys
import tempfile
import filecmp
import csv
from collections import defaultdict
from time import time
from tqdm import tqdm
from glob import glob

from odess import SimilarityIndex

csv.field_size_limit(sys.maxsize)

def argmax(d: dict):
    return max(d, key=d.get)

def intersect_dict(d: dict, l: list):
    return {k: v for k,v in d.items() if k in l}

class TimeToDict:
    def __init__(self, d, key):
        self.d = d
        self.key = key

    def __enter__(self):
        self.stime = time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.d[self.key] += time() - self.stime

if __name__ == '__main__':
    si = SimilarityIndex()
    size_map = {} # Store sizes of files so we can sort them by that later
    file_keys = set()

    with open('./emails.csv') as f:
        reader = csv.DictReader(f, quotechar="\"")
        sys.stdout.write('Creating similarity index')
        for i, row in enumerate(reader):
            if i % 1000 == 0:
                sys.stdout.write('.')
                sys.stdout.flush()

            if len(row['message']) >= 64 * 1024:
                continue

            size_map[row['file']] = len(row['message'])
            si.add(row['file'], row['message'])
            file_keys.add(row['file'])
            
        sys.stdout.write('\n')
        f.seek(0)
        reader = csv.DictReader(f, quotechar="\"")
        sys.stdout.write('Creating pair file')
        i = 0
        with open('./emails_pairs.odess', 'w+') as o:
            for row in reader:
                if row['file'] not in file_keys:
                    continue

                similars = list(si.find_similar(row['file']))
                if not len(similars):
                    continue

                i += 1
                if i % 1000 == 0:
                    sys.stdout.write('.')
                    sys.stdout.flush()
                largest = argmax(intersect_dict(size_map, similars))
                for similar in similars:
                    if similar == largest: # Skip base
                        continue
                    
                    if similar not in file_keys: # Skip already covered keys
                        continue

                    file_keys.discard(similar)
                    o.write(f'{largest}:{similar}\n')

                file_keys.discard(largest)

        with open('./email_pairs.missed', 'w+') as o:
            for file in file_keys:
                o.write(f'{file}\n')
