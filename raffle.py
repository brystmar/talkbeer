"""Framework for running a raffle/LIF based on the contents of a forum post.  Originally for SSF15's #elkhunter LIF.

Portions of this were hastily written and partially discarded.  Consider it a work in progress.
"""
from global_logger import logger
from models import User, Post, Post_Soup
from my_functions import make_soup
import certifi
import json
import os
import urllib3


class Guess(object):
    """Stores all pertinent info about each guess submitted as a LIF entry."""
    def __init__(self, user=User(), post=Post(), soup=Post_Soup(), number_guessed=None, order=None):
        # Allows initialization of the Guess class on a single line: foo = Guess(user=SomeUser, post=Post123, [...]).
        self.user = user
        self.post = post
        self.soup = soup
        self.number_guessed = number_guessed
        self.order = order


def compile_guesses(elk_data, thread_name, post_info):
    """Find the number each user guessed for the elkhunter LIF."""
    logger.debug("Starting elkhunter() with: {}".format(elk_data))

    # validation
    if elk_data is None:
        logger.debug("No elk_data provided.")
        logger.debug("Ending elkhunter()")
        return None
    else:
        print("\nWriting {num} posts to: {name} {info}.txt".format(num=len(elk_data),
                                                                   name=thread_name, info=post_info))
        logger.debug("Writing {num} posts to: {name} {info}.txt".format(num=len(elk_data),
                                                                        name=thread_name, info=post_info))

    guesses = []

    for datum in elk_data:
        guess = Guess()
        # r.id, r.soup, p.username, p.timestamp, p.page_number
        guess_soup = make_soup(datum[1])

        # remove quoted posts
        while guess_soup.find('div', class_="bbCodeBlock bbCodeQuote") is not None:
            guess_soup.find('div', class_="bbCodeBlock bbCodeQuote").decompose()

        words = guess_soup.blockquote.get_text().split()

        # find the number they guessed
        count = 1
        for w in words:
            try:
                number_guessed = int(w)
                if number_guessed < 1 or number_guessed > 1000:
                    # invalid guess
                    continue
                guess = Guess()
                guess.soup.soup = guess_soup
                guess.user.id = datum[0]
                guess.user.username = datum[2]
                guess.post.timestamp = datum[3]
                guess.post.thread_page = datum[4]
                guess.number_guessed = number_guessed
                guess.order = count

                guesses.append(guess)
                count += 1
            except Exception:
                continue

    logger.debug("Ending elkhunter() with: {}".format(guesses))
    return guesses


def run_raffle(num_winners, list_of_users):
    """Randomly select the winner(s) from the provided list of Users."""
    logger.debug('Starting run_raffle(), seeking {n} winners from {list}'.format(n=num_winners, list=list_of_users))
    noun = "winner" if num_winners == 1 else "winners"

    winning_indicies = return_random_nums(num_winners, 0, len(list_of_users))

    winners = []
    if num_winners == 1:
        winners = [list_of_users[winning_indicies]]
    else:
        for index in winning_indicies:
            winners.append(list_of_users[index])

    print("Raffle {noun} from the {qty} entries:".format(noun=noun, qty=len(list_of_users)))
    logger.debug("Raffle {noun} from the {qty} entries:".format(noun=noun, qty=len(list_of_users)))
    for w in winners:
        print("{winner}".format(winner=w.username))
        logger.debug("{winner}".format(winner=w.username))

    logger.debug('Ending run_raffle()')


def return_random_nums(qty, min_val, max_val):
    """Accepts a positive integer >0, returns a specified amount of random int(s) between 0 and that value.

    Uses the random.org API: https://api.random.org/json-rpc/2/

    'Replacement' specifies whether the random numbers should be picked with replacement. The default (true) will cause
        the numbers to be picked with replacement, i.e., the resulting numbers may contain duplicate values (like a series
        of dice rolls). If you want the numbers picked to be unique (ex: raffle tickets drawn from a box), set to false.
    """
    # validation
    if qty >= 10000:
        print("Invalid random.org query: too many numbers requested ({}).".format(qty))
        quit()

    # import environment variables
    from env_tools import apply_env
    apply_env()
    logger.info("Applied .env variables using env_tools")

    http = urllib3.PoolManager(timeout=3.0, cert_reqs='CERT_REQUIRED', ca_certs=certifi.where())

    endpoint = "https://api.random.org/json-rpc/2/invoke"
    headers = {"Content-Type": "application/json"}
    api_key = os.environ.get('API_KEY_RANDOM_ORG')
    replacement = False

    # API uses JSON-RPC, which requires these headers:
    json_rpc = "2.0"
    method = "generateIntegers"
    request_id = 1

    # parameters for the generateIntegers request
    params = {
                "apiKey": api_key,
                "n": qty,
                "min": min_val,
                "max": max_val,
                "replacement": replacement
        }

    request = {
                "jsonrpc": json_rpc,
                "method": method,
                "params": params,
                "id": request_id
        }

    # encode the request
    encoded_data = json.dumps(request).encode("utf-8")
    # call the API
    response = http.request("POST", endpoint, body=encoded_data, headers=headers)
    # unpack & decode the response
    response_decoded = json.loads(response.data.decode("utf-8"))

    if 'error' in response_decoded.keys():
        print("Random request failed:", "\n", response_decoded['error'])
        return None
    elif 'result' in response_decoded.keys():
        # response_timestamp = response['result']['random']['completionTime']
        # print("Random request completed at", response_timestamp)
        return response_decoded['result']['random']['data']
    else:
        print("Unexpected response structure from random.org API:", "\n", response_decoded)
        return None
