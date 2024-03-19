import sha3

from math import log2
from utility.spec import ENTRY_SIZE


def decompose(num):
    powers = []
    while num > 0:
        power = int(log2(num))
        powers += [power]
        num -= 1 << power
    return powers


def add_0x_prefix(val):
    return "0x" + val


class Hasher:
    def __init__(self, algorithm="keccak_256", encoding="utf-8", security=False):
        self.algorithm = algorithm
        self.security = security
        self.encoding = encoding

        if security:
            self.prefix00 = "\x00".encode(encoding)
            self.prefix01 = "\x01".encode(encoding)
        else:
            self.prefix00 = bytes()
            self.prefix01 = bytes()

    def _hasher(self):
        if self.algorithm == "keccak_256":
            return sha3.keccak_256()
        else:
            raise NotImplementedError

    def hash_data(self, data):
        buff = self.prefix00 + (
            data if isinstance(data, bytes) else data.encode(self.encoding)
        )

        hasher = self._hasher()
        hasher.update(buff)
        return hasher.hexdigest().encode(self.encoding)

    def hash_pair(self, left, right):
        buff = (
            self.prefix01
            + bytes.fromhex(left.decode("utf-8"))
            + bytes.fromhex(right.decode("utf-8"))
        )

        hasher = self._hasher()
        hasher.update(buff)
        return hasher.hexdigest().encode(self.encoding)


class Node:

    __slots__ = ("__value", "__parent", "__left", "__right")

    def __init__(self, value, parent=None, left=None, right=None):
        self.__value = value
        self.__parent = parent
        self.__left = left
        self.__right = right

        if left:
            left.__parent = self
        if right:
            right.__parent = self

    @property
    def value(self):
        return self.__value

    @property
    def left(self):
        return self.__left

    @property
    def right(self):
        return self.__right

    @property
    def parent(self):
        return self.__parent

    def set_left(self, node):
        self.__left = node

    def set_right(self, node):
        self.__right = node

    def set_parent(self, node):
        self.__parent = node

    def is_left_child(self):
        parent = self.__parent
        if not parent:
            return False

        return self == parent.left

    def is_right_child(self):
        parent = self.__parent
        if not parent:
            return False

        return self == parent.right

    def is_leaf(self):
        return isinstance(self, Leaf)

    @classmethod
    def from_children(cls, left, right, hasher):
        digest = hasher.hash_pair(left.__value, right.__value)
        return cls(value=digest, left=left, right=right, parent=None)

    def ancestor(self, degree):
        if degree == 0:
            return self

        if not self.__parent:
            return

        return self.__parent.ancestor(degree - 1)

    def recalculate_hash(self, hasher):
        self.__value = hasher.hash_pair(self.left.value, self.right.value)


class Leaf(Node):
    def __init__(self, value, leaf=None):
        super().__init__(value)

    @classmethod
    def from_data(cls, data, hasher):
        return cls(hasher.hash_data(data), leaf=None)


class MerkleTree:
    def __init__(self, encoding="utf-8"):
        self.__root = None
        self.__leaves = []
        self.encoding = encoding
        self.hasher = Hasher(encoding=encoding)

    def __bool__(self):
        return len(self.__leaves) != 0

    def encrypt(self, data):
        leaf = Leaf.from_data(data, self.hasher)
        self.add_leaf(leaf)

    @classmethod
    def from_data_list(cls, data, encoding="utf-8"):
        tree = cls(encoding)

        n = len(data)
        if n < ENTRY_SIZE or (n & (n - 1)) != 0:
            raise Exception("Input length is not power of 2")
        
        leaves = [Leaf.from_data(data[i:i + ENTRY_SIZE], tree.hasher) for i in range(0, n, ENTRY_SIZE)]
        tree.__leaves = leaves

        nodes = leaves
        while len(nodes) > 1:
            next_nodes = []
            for i in range(0, len(nodes), 2):
                next_nodes.append(Node.from_children(nodes[i], nodes[i+1], tree.hasher))

            nodes = next_nodes

        tree.__root = nodes[0]
        return tree

    def add_leaf(self, leaf):
        if self:
            subroot = self.get_last_subroot()
            self._append_leaf(leaf)

            if not subroot.parent:
                # Increase height by one
                self.__root = Node.from_children(subroot, leaf, self.hasher)
            else:
                parent = subroot.parent

                # Create bifurcation node
                new_node = Node.from_children(subroot, leaf, self.hasher)

                # Interject bifurcation node
                parent.set_right(new_node)
                new_node.set_parent(parent)

                # Recalculate hashes only at the rightmost branch of the tree
                curr = parent
                while curr:
                    curr.recalculate_hash(self.hasher)
                    curr = curr.parent
        else:
            self._append_leaf(leaf)
            self.__root = leaf

    def get_last_subroot(self):
        if not self.__leaves:
            raise ValueError

        last_power = decompose(len(self.__leaves))[-1]
        return self.get_tail().ancestor(degree=last_power)

    def get_tail(self):
        return self.__leaves[-1]

    def _append_leaf(self, leaf):
        self.__leaves.append(leaf)

    def get_root_hash(self):
        if not self.__root:
            return

        return self.__root.value

    def decode_value(self, val):
        return val.decode(self.encoding)

    def proof_at(self, i):
        if i < 0 or i >= len(self.__leaves):
            raise IndexError

        if len(self.__leaves) == 1:
            return {
                "lemma": [add_0x_prefix(self.decode_value(self.get_root_hash()))],
                "path": [],
            }

        proof = {"lemma": [], "path": []}
        proof["lemma"].append(add_0x_prefix(self.decode_value(self.__leaves[i].value)))

        current = self.__leaves[i]
        while current != self.__root:
            if current.parent != None and current.parent.left == current:
                # add right
                proof["lemma"].append(
                    add_0x_prefix(self.decode_value(current.parent.right.value))
                )
                proof["path"].append(True)
            else:
                # add left
                proof["lemma"].append(
                    add_0x_prefix(self.decode_value(current.parent.left.value))
                )
                proof["path"].append(False)

            current = current.parent

        # add root
        proof["lemma"].append(add_0x_prefix(self.decode_value(self.get_root_hash())))
        return proof
