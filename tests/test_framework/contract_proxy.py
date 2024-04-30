from gettext import npgettext
from config.node_config import TX_PARAMS
from utility.utils import assert_equal
from copy import copy


class ContractProxy:
    def __init__(self, contract, blockchain_nodes):
        self.contract = contract
        self.contract_address = contract.address
        self.blockchain_nodes = blockchain_nodes

    def _get_contract(self, node_idx=0):
        return (
            self.contract
            if node_idx == 0
            else self.blockchain_nodes[node_idx].get_contract(self.contract_address)
        )

    def _call(self, fn_name, node_idx, **args):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
        return getattr(contract.functions, fn_name)(**args).call()

    def _send(self, fn_name, node_idx, **args):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
        return getattr(contract.functions, fn_name)(**args).transact(copy(TX_PARAMS))
    
    def _logs(self, event_name, node_idx, **args):
        assert node_idx < len(self.blockchain_nodes)

        contract = self._get_contract(node_idx)
    
        return getattr(contract.events, event_name).create_filter(fromBlock =0, toBlock="latest").get_all_entries()

    def transfer(self, value, node_idx = 0):
        tx_params = copy(TX_PARAMS)
        tx_params["value"] = value

        contract = self._get_contract(node_idx)
        contract.receive.transact(tx_params)
    
    def address(self):
        return self.contract_address


class FlowContractProxy(ContractProxy):
    def submit(
        self, submission_nodes, node_idx=0, tx_prarams=None, parent_hash=None,
    ):
        assert node_idx < len(self.blockchain_nodes)

        combined_tx_prarams = copy(TX_PARAMS)

        if tx_prarams is not None:
            combined_tx_prarams.update(tx_prarams)
            

        contract = self._get_contract(node_idx)
        # print(contract.functions.submit(submission_nodes).estimate_gas(combined_tx_prarams))
        tx_hash = contract.functions.submit(submission_nodes).transact(combined_tx_prarams)
        receipt = self.blockchain_nodes[node_idx].wait_for_transaction_receipt(
            contract.w3, tx_hash, parent_hash=parent_hash
        )
        if receipt["status"] != 1:
            print(receipt)
            assert_equal(receipt["status"], 1)
        return tx_hash

    def num_submissions(self, node_idx=0):
        return self._call("numSubmissions", node_idx)

    def first_block(self, node_idx=0):
        return self._call("firstBlock", node_idx)

    def epoch(self, node_idx=0):
        return self.get_mine_context(node_idx)[0]
    
    def update_context(self, node_idx=0):
        return self._send("makeContext", node_idx)

    def get_mine_context(self, node_idx=0):
        return self._call("makeContextWithResult", node_idx)


class MineContractProxy(ContractProxy):
    def last_mined_epoch(self, node_idx=0):
        return self._call("lastMinedEpoch", node_idx)

    def set_quality(self, quality, node_idx=0):
        return self._send("setQuality", node_idx, _targetQuality=quality)
    


class IRewardContractProxy(ContractProxy):
    def reward_distributes(self, node_idx=0):
        return self._logs("DistributeReward", node_idx)