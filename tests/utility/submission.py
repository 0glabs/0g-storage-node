import base64

from eth_utils import encode_hex, decode_hex
from math import log2
from utility.merkle_tree import add_0x_prefix, Leaf, MerkleTree
from utility.spec import ENTRY_SIZE, PORA_CHUNK_SIZE

def log2_pow2(n):
    return int(log2(((n ^ (n - 1)) >> 1) + 1))


def next_pow2(input):
    x = input
    x -= 1
    x |= x >> 16
    x |= x >> 8
    x |= x >> 4
    x |= x >> 2
    x |= x >> 1
    x += 1

    return x


def bytes_to_entries(size_bytes):
    if size_bytes % ENTRY_SIZE == 0:
        return size_bytes // ENTRY_SIZE
    else:
        return size_bytes // ENTRY_SIZE + 1


def create_submission(data):
    submission = []
    submission.append(len(data))
    submission.append(b"")
    submission.append([])

    offset = 0
    nodes = []
    for chunks in split_nodes(len(data)):
        node_hash = create_node(data, offset, chunks)
        nodes.append(node_hash)

        height = int(log2(chunks))
        submission[2].append([decode_hex(node_hash.decode("utf-8")), height])
        offset += chunks * ENTRY_SIZE

    root_hash = nodes[-1]
    for i in range(len(nodes) - 2, -1, -1):
        tree = MerkleTree()
        tree.add_leaf(Leaf(nodes[i]))
        tree.add_leaf(Leaf(root_hash))
        root_hash = tree.get_root_hash()

    return submission, add_0x_prefix(root_hash.decode("utf-8"))


def split_nodes(data_len):
    nodes = []

    chunks = bytes_to_entries(data_len)
    padded_chunks, chunks_next_pow2 = compute_padded_size(chunks)
    next_chunk_size = chunks_next_pow2

    while padded_chunks > 0:
        if padded_chunks >= next_chunk_size:
            padded_chunks -= next_chunk_size
            nodes.append(next_chunk_size)

        next_chunk_size >>= 1

    return nodes


def compute_padded_size(chunk_len):
    chunks_next_pow2 = next_pow2(chunk_len)

    if chunks_next_pow2 == chunk_len:
        return chunks_next_pow2, chunks_next_pow2

    min_chunk = 1 if chunks_next_pow2 < 16 else chunks_next_pow2 // 16
    padded_chunks = ((chunk_len - 1) // min_chunk + 1) * min_chunk

    return padded_chunks, chunks_next_pow2


def create_node(data, offset, chunks):
    batch = chunks
    if chunks > PORA_CHUNK_SIZE:
        batch = PORA_CHUNK_SIZE

    return create_segment_node(data, offset, ENTRY_SIZE * batch, ENTRY_SIZE * chunks)


def create_segment_node(data, offset, batch, size):
    tree = MerkleTree()
    n = len(data)
    for i in range(offset, offset + size, batch):
        start = i
        end = min(offset + size, i + batch)

        if start >= n:
            tree.add_leaf(Leaf(segment_root(b"\x00" * (end - start))))
        elif end > n:
            tree.add_leaf(Leaf(segment_root(data[start:] + b"\x00" * (end - n))))
        else:
            tree.add_leaf(Leaf(segment_root(data[start:end])))


    return tree.get_root_hash()



segment_root_cached_chunks = None
segment_root_cached_output = None
def segment_root(chunks):
    global segment_root_cached_chunks, segment_root_cached_output

    if segment_root_cached_chunks == chunks:
        return segment_root_cached_output


    data_len = len(chunks)
    if data_len == 0:
        return b"\x00" * 32


    tree = MerkleTree()
    for i in range(0, data_len, ENTRY_SIZE):
        tree.encrypt(chunks[i : i + ENTRY_SIZE])
    
    digest = tree.get_root_hash()

    segment_root_cached_chunks = chunks
    segment_root_cached_output = digest
    return digest


def generate_merkle_tree(data):
    chunks = bytes_to_entries(len(data))
    padded_chunks, _ = compute_padded_size(chunks)

    tree = MerkleTree()
    for i in range(padded_chunks):
        if i * ENTRY_SIZE > len(data):
            tree.encrypt(b"\x00" * ENTRY_SIZE)
        elif (i + 1) * ENTRY_SIZE > len(data):
            tree.encrypt(
                data[i * ENTRY_SIZE :] + b"\x00" * ((i + 1) * ENTRY_SIZE - len(data))
            )
        else:
            tree.encrypt(data[i * ENTRY_SIZE : (i + 1) * ENTRY_SIZE])

    return tree


def generate_merkle_tree_by_batch(data):
    chunks = bytes_to_entries(len(data))
    padded_chunks, _ = compute_padded_size(chunks)

    tree = MerkleTree()
    for i in range(0, padded_chunks, PORA_CHUNK_SIZE):
        if i * ENTRY_SIZE >= len(data):
            tree.add_leaf(
                Leaf(
                    segment_root(
                        b"\x00" * ENTRY_SIZE * min(PORA_CHUNK_SIZE, padded_chunks - i)
                    )
                )
            )
        elif (i + PORA_CHUNK_SIZE) * ENTRY_SIZE > len(data):
            tree.add_leaf(
                Leaf(
                    segment_root(
                        data[i * ENTRY_SIZE :]
                        + b"\x00"
                        * (
                            min(padded_chunks, i + PORA_CHUNK_SIZE) * ENTRY_SIZE
                            - len(data)
                        )
                    )
                )
            )
        else:
            tree.add_leaf(
                Leaf(
                    segment_root(
                        data[i * ENTRY_SIZE : (i + PORA_CHUNK_SIZE) * ENTRY_SIZE]
                    )
                )
            )

    return tree, add_0x_prefix(tree.decode_value(tree.get_root_hash()))


def submit_data(client, data):
    segments = data_to_segments(data)
    for segment in segments:
        client.zgs_upload_segment(segment)
    return segments


def data_to_segments(data):
    tree, root_hash = generate_merkle_tree_by_batch(data)
    chunks = bytes_to_entries(len(data))

    segments = []
    idx = 0
    while idx * PORA_CHUNK_SIZE < chunks:
        proof = tree.proof_at(idx)

        tmp = (
            data[
                idx
                * ENTRY_SIZE
                * PORA_CHUNK_SIZE : (idx + 1)
                * ENTRY_SIZE
                * PORA_CHUNK_SIZE
            ]
            if len(data) >= (idx + 1) * PORA_CHUNK_SIZE * ENTRY_SIZE
            else data[idx * ENTRY_SIZE * PORA_CHUNK_SIZE :]
            + b"\x00" * (chunks * ENTRY_SIZE - len(data))
        )

        segment = {
            "root": root_hash,
            "data": base64.b64encode(tmp).decode("utf-8"),
            "index": idx,
            "proof": proof,
            "fileSize": len(data),
        }

        segments.append(segment)
        idx += 1

    return segments