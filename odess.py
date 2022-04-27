import xxhash
import numpy as np
from collections import defaultdict
from gear_matrix import gear_matrix
from typing import *

# TODO: figure out the logic here
sample_ratios = {
        512: 0x0100400303410010,
        256: 0x0100400303410000,
        128: 0x0000400303410000,
        4:   0x0000000100000001
    }

u64 = np.uint64
U64_MASK = u64(2 ** 64-1)

# Odess Index
class SimilarityIndex:
    def __init__(self, **kwargs):
        self._feature_index = FeatureIndex(**kwargs)
        # TODO: Could be a bidict?
        self._key_sf_map: Dict[str, Set[int]] = {}
        self._sf_key_map: Dict[int, Set[str]] = defaultdict(set)

    def add(self, key, value):
        self._feature_index.import_features(value)
        sfs = set(self._feature_index.generate_superfeatures())
        for sf in sfs:
            self._sf_key_map[sf].add(key)
        self._key_sf_map[key] = sfs

    def find_similar(self, key) -> Iterable[str]:
        if key not in self._key_sf_map:
            return []

        for sf in self._key_sf_map[key]:
            for k in self._sf_key_map[sf]:
                if k != key:
                    yield k

class FeatureIndex:
    def __init__(self, sample_ratio = 128, num_features = 12, num_superfeatures = 3):
        self._features = np.zeros(num_features)
        self._nfeatures = 12
        self._nsuperfeatures = 3
        self._sample_mask = u64(sample_ratios[sample_ratio])

        self._random_transform_a = np.random.randint(0, U64_MASK, (num_features,), dtype=np.uint64)
        self._random_transform_b = np.random.randint(0, U64_MASK, (num_features,), dtype=np.uint64)

    def generate_superfeatures(self) -> Iterable[int]:
        group_len = self._nfeatures // self._nsuperfeatures
        for i in range(self._nsuperfeatures):
            chunk = b''.join(self._features[i * group_len:(i+1)*group_len])
            yield xxhash.xxh64(chunk, 0x7fcaf1).intdigest()

    def import_features(self, data: bytes):
        h = u64(0)
        for b in data:
            # Rolling (gear) hash
            h = (h << u64(1))
            h = (h + u64(gear_matrix[ord(b) & 0xFF])) # Overflows
            if not (h & self._sample_mask): # At random intervals
                for i in range(self._nfeatures):
                    # Extract a sample (effectively a hash), and only keep the highest ones in the index to reduce overlap?
                    t = (h * u64(self._random_transform_a[i]) + u64(self._random_transform_b[i])) # Overflows
                    if t > self._features[i]:
                        self._features[i] = t


