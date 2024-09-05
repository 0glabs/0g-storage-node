import base64
import inspect
import platform
import rtoml
import time
import sha3

class PortMin:
    # Must be initialized with a unique integer for each process
    n = 11000


MAX_NODES = 100


def p2p_port(n):
    assert n <= MAX_NODES
    return PortMin.n + n


def rpc_port(n):
    return PortMin.n + MAX_NODES + n


def blockchain_p2p_port(n):
    return PortMin.n + 2 * MAX_NODES + n


def blockchain_rpc_port(n):
    return PortMin.n + 3 * MAX_NODES + n


def blockchain_rpc_port_core(n):
    return PortMin.n + 4 * MAX_NODES + n

def arrange_port(category: int, node_index: int) -> int:
    return PortMin.n + (100 + category) * MAX_NODES + node_index

def wait_until(predicate, *, attempts=float("inf"), timeout=float("inf"), lock=None):
    if attempts == float("inf") and timeout == float("inf"):
        timeout = 60
    attempt = 0
    time_end = time.time() + timeout

    while attempt < attempts and time.time() < time_end:
        if lock:
            with lock:
                if predicate():
                    return
        else:
            if predicate():
                return
        attempt += 1
        time.sleep(0.5)

    # Print the cause of the timeout
    predicate_source = inspect.getsourcelines(predicate)
    if attempt >= attempts:
        raise AssertionError(
            "Predicate {} not true after {} attempts".format(predicate_source, attempts)
        )
    elif time.time() >= time_end:
        raise AssertionError(
            "Predicate {} not true after {} seconds".format(predicate_source, timeout)
        )
    raise RuntimeError("Unreachable")


def is_windows_platform():
    return platform.system().lower() == "windows"


def initialize_config(config_path, config_parameters):
    with open(config_path, "w") as f:
        for k in config_parameters:
            value = config_parameters[k]
            if isinstance(value, str) and not (
                value.startswith('"') or value.startswith("'")
            ):
                if value == "true" or value == "false":
                    value = f"{value}"
                else:
                    value = f'"{value}"'

            f.write(f"{k}={value}\n")


def initialize_toml_config(config_path, config_parameters):
    with open(config_path, "w") as f:
        rtoml.dump(config_parameters, f)


def create_proof_and_segment(chunk_data, data_root, index=0):
    proof = {
        "lemma": [data_root],
        "path": [],
    }

    segment = {
        "root": data_root,
        "data": base64.b64encode(chunk_data).decode("utf-8"),
        "index": index,
        "proof": proof,
    }

    return proof, segment


def assert_equal(thing1, thing2, *args):
    if thing1 != thing2 or any(thing1 != arg for arg in args):
        raise AssertionError(
            "not(%s)" % " == ".join(str(arg) for arg in (thing1, thing2) + args)
        )


def assert_ne(thing1, thing2):
    if thing1 == thing2:
        raise AssertionError("not(%s)" % " != ".join([thing1, thing2]))


def assert_greater_than(thing1, thing2):
    if thing1 <= thing2:
        raise AssertionError("%s <= %s" % (str(thing1), str(thing2)))


def assert_greater_than_or_equal(thing1, thing2):
    if thing1 < thing2:
        raise AssertionError("%s < %s" % (str(thing1), str(thing2)))

# 14900K has the performance point 100 
def estimate_st_performance():
    hasher = sha3.keccak_256()
    input =  b"\xcc" * (1<<26)
    start_time = time.perf_counter()
    hasher.update(input)
    digest = hasher.hexdigest()
    return 10 / (time.perf_counter() - start_time)