import csv
import sys
import tempfile
import filecmp
import os
from time import time
from collections import defaultdict

csv.field_size_limit(sys.maxsize)

algo_list = [
    "xdelta3 -f {e} -s {base} {inp} {out}",
    "./gdelta_stream {e} -o {out} {base} {inp}",
    "./gdelta_org {e} -o {out} {base} {inp}"
]


def write_to_file(path: str, data: str):
    with open(path, 'w+') as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())


class TimeToDict:

    def __init__(self, d, key):
        self.d = d
        self.key = key

    def __enter__(self):
        self.stime = time()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.d[self.key] += time() - self.stime


if __name__ == '__main__':
    size_map = defaultdict(lambda: 0)
    fail_map = defaultdict(lambda: 0)
    decode_times = defaultdict(lambda: 0)
    encode_times = defaultdict(lambda: 0)
    timed_count = 0
    dataset = {}

    with open('./emails.csv', 'r') as csvf:
        reader = csv.DictReader(csvf, quotechar="\"")
        sys.stdout.write('Loading dataset')
        i = 0
        for row in reader:
            dataset[row['file']] = row['message']
            i += 1
            if i % 1000 == 0:
                sys.stdout.write('.')
                sys.stdout.flush()
        sys.stdout.write(f'Read {i} records')

    sys.stdout.write('\n')
    size_processed = set()
    with tempfile.TemporaryDirectory() as tmpdirname:
        with open('./emails_pairs.odess', 'r') as pairf:
            sys.stdout.write('Benchmarking pairs')
            i = 0
            lines = pairf.readlines()
            sys.stdout.write(f' {len(lines)} pairs')
            for line in lines:
                i += 1
                if i % 1000 == 0:
                    sys.stdout.write('.')
                    sys.stdout.flush()
                basek, targetk = line.strip().split(':')
                base, target = dataset[basek], dataset[targetk]
                
                if len(base) >= 64 * 1024 or len(target) >= 64 * 1024:
                    sys.stdout.write(f'*')
                    continue

                add_base = False
                if basek not in size_processed:
                    size_map['original'] += len(base)
                    size_processed.add(basek)
                    add_base = True

                size_map['original'] += len(target)
                

                write_to_file(f'{tmpdirname}/base.bin', base)
                write_to_file(f'{tmpdirname}/target.bin', target)

                for cmd in algo_list:
                    if add_base:
                        size_map[cmd] += os.path.getsize(f'{tmpdirname}/base.bin')

                    with TimeToDict(encode_times, cmd):
                        encode_args = dict(e='-e',
                                           base=f'{tmpdirname}/base.bin',
                                           inp=f'{tmpdirname}/target.bin',
                                           out=f'{tmpdirname}/out.delta')
                        if os.system(cmd.format_map(encode_args)):
                            # encode failed
                            fail_map[cmd] += 1
                            size_map[cmd] += os.path.getsize(f'{tmpdirname}/target.bin')
                            print(f"command failed {cmd.format_map(encode_args)}")

                    with TimeToDict(decode_times, cmd):
                        decode_args = dict(e='-d',
                                           base=f'{tmpdirname}/base.bin',
                                           inp=f'{tmpdirname}/out.delta',
                                           out=f'{tmpdirname}/out.target')
                        if os.system(cmd.format_map(decode_args)):
                            # decode failed
                            fail_map[cmd] += 1
                            size_map[cmd] += os.path.getsize(f'{tmpdirname}/target.bin')
                            print(f"command failed {cmd.format_map(decode_args)}")
                    
                    with open(f'{tmpdirname}/out.target', 'rb') as f:
                        os.fsync(f.fileno())
                        target_hat = f.read()
                        if target_hat != target.encode():
                            print(f"\nFile decoded from delta is different from the original: algo={cmd}, base=base.bin, target=target.bin, dir={tmpdirname}")
                            print('=====Reconstructed========')
                            print(target_hat)
                            print('=====Original========')
                            print(target)
                            breakpoint()
#                    filecmp.clear_cache()
#                    if not filecmp.cmp(f'{tmpdirname}/out.target', f'{tmpdirname}/target.bin'):
                    timed_count += 1
                    size_map[cmd] += os.path.getsize(f'{tmpdirname}/out.delta')

        print()
        print(f"Algorithm\t\t| Decode(s)\t\t | Encode(s)\t\t | Size(MB)\t\t | Fails")
        for algo in size_map:
            print(
                f"{algo}\t\t| {decode_times.get(algo)}\t\t | {encode_times.get(algo)}\t\t | {round(size_map.get(algo)/1e6,2)} | {fail_map.get(algo, 0)}"
            )
