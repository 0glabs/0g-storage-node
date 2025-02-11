# One Box Test

0G storage node provides one box test framework for developers to verify system functionalities via RPC.

## Prerequisites

- Requires python version: 3.8, 3.9 or 3.10, higher version is not guaranteed (e.g. failed to install `pysha3`).
- Installs dependencies under root folder: `pip3 install -r requirements.txt`

## Install Blockchain Nodes

Python test framework will launch blockchain nodes at local machine for storage nodes to interact with. There are 3 kinds of blockchains available:

- 0G blockchain (by default).
- Conflux eSpace (for chain reorg test purpose).
- BSC node (geth).

The blockchain node binaries will be compiled or downloaded from github to `tests/tmp` folder automatically. Alternatively, developers could also manually copy binaries of specific version to the `tests/tmp` folder.

## Run Tests

Change to the `tests` folder and run the following command to run all tests:

```
python test_all.py
```

or, run any single test, e.g.

```
python example_test.py
```

*Note, please ensure blockchain nodes installed before running any single test, e.g. run all tests at first.*

## Add New Test

Please follow the `example_test.py` to add a new `xxx_test.py` file under `tests` folder.
