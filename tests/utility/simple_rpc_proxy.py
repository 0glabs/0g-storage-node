from jsonrpcclient import request, parse, Ok
import requests


class SimpleRpcProxy:
    def __init__(self, url, timeout=60):
        self.url = url
        self.timeout = timeout

    def __getattr__(self, name):
        return RpcCaller(self.url, name, self.timeout)


class RpcCaller:
    def __init__(self, url, method, timeout):
        self.url = url
        self.method = method
        self.timeout = timeout

    def __call__(self, *args, **argsn):
        r = request(self.method, *args)
        try:
            response = requests.post(self.url, json=r, timeout=self.timeout)
            parsed = parse(response.json())
            if isinstance(parsed, Ok):
                return parsed.result
            else:
                print("Failed to call RPC, method = %s(%s), error = %s" % (self.method, str(*args), parsed))
        except Exception as ex:
            print("Failed to call RPC, method = %s(%s), exception = %s" % (self.method, str(*args), ex))
        return None
