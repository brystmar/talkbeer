# Accepts a positive integer >0, returns a specified amount of random int(s) between 0 and that value.
# Uses the random.org API: https://api.random.org/json-rpc/2/
#
# 'Replacement' specifies whether the random numbers should be picked with replacement. The default (true) will cause
#    the numbers to be picked with replacement, i.e., the resulting numbers may contain duplicate values (like a series
#    of dice rolls). If you want the numbers picked to be unique (ex: raffle tickets drawn from a box), set to false.

# def return_random_nums(qty, min, max, replacement):


def return_random_nums(qty, min_val, max_val):
    # validation
    if qty >= 10000:
        print("Invalid random.org query: too many numbers requested ({}).".format(qty))
        quit()

    import urllib3
    import certifi
    import json
    from tbcred import random_apikey

    http = urllib3.PoolManager(timeout=3.0, cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

    endpoint = "https://api.random.org/json-rpc/2/invoke"
    headers = {"Content-Type": "application/json"}
    apikey = random_apikey
    replacement = False

    # API uses JSON-RPC, which requires these headers:
    jsonrpc = "2.0"
    method = "generateIntegers"
    request_id = 1

    # parameters for the generateIntegers request
    params = {
                "apiKey": apikey,
                "n": qty,
                "min": min_val,
                "max": max_val,
                "replacement": replacement
        }
    # request payload
    request = {
                "jsonrpc": jsonrpc,
                "method": method,
                "params": params,
                "id": request_id
        }

    # encode the request
    encoded_data = json.dumps(request).encode("utf-8")
    # call the API
    r = http.request("POST", endpoint, body=encoded_data, headers=headers)
    # unpack & decode the response
    response = json.loads(r.data.decode("utf-8"))

    if 'error' in response.keys():
        print("Random request failed:", "\n", response['error'])
        return None
    elif 'result' in response.keys():
        # response_timestamp = response['result']['random']['completionTime']
        # print("Random request completed at", response_timestamp)
        return response['result']['random']['data']
    else:
        print("Unexpected response structure from random.org API:", "\n", response)
        return None
