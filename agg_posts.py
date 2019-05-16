"""Parses data from posts on each page_number of a thread, then cleans & writes this data to the database.

Also:
* Identifies & adds users not currently in the database.
* Collects & writes likes for each post to the database.
* Allows users to select a templated option for writing an html file.
"""

from bs4 import BeautifulSoup
import datetime
import dateutil.parser
import logging as logging_util
import os
import re
import requests
# import sqlite3
from sqlalchemy import create_engine, event  # engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
from models import Biffers, Likes, Posts, Region_Map, Threads, Users
from models import Errors, Posts_Soup, Thread_Page
from models import URLs, Output_Options
from dotenv import load_dotenv
from urllib3.exceptions import InsecureRequestWarning

# initialize logging
logfile = 'logs/{}.log'.format(__file__)
logging_util.basicConfig(filename=logfile, filemode='w', level=logging_util.DEBUG, datefmt='%H:%M:%S',
                         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging_util.getLogger(__name__)

time_start = datetime.datetime.now()
# time_start = time_start.isoformat(timespec='seconds')
print("\n***** ***** **** ***** *****")
print(" Start:", time_start.strftime("%Y-%m-%d %H:%M:%S"))
print("***** ***** **** ***** *****\n")
logger.info('\n\n***** ***** **** ***** ***** |||| ***** ***** **** ***** *****\n')
logger.info('START agg_posts.py @ {}'.format(time_start.strftime("%Y-%m-%d %H:%M:%S")))

# Set the path for loading local variables from the .env file
basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))


class Config(object):
    db_user = os.environ.get('DB_USER')
    db_pw = os.environ.get('DB_PW')
    db_name = os.environ.get('DB_NAME')
    db_instance = os.environ.get('DB_INSTANCE')
    tb_credentials = {'login': os.environ.get('TB_USER'), 'password': os.environ.get('TB_PW')}
    api_key_random_org = os.environ.get('API_KEY_RANDOM_ORG')


def pause():
    """Pause the program until the user is ready to continue."""
    input("[==========]")


def make_soup(html_to_soup):
    """Convert an html chunk into a BeautifulSoup object."""
    return BeautifulSoup(html_to_soup, 'html.parser')


def find_substring(string, char):
    """Return the index of a given character within a substring."""
    return [st for st, letter in enumerate(string) if letter == char]


def commit(database_connection):
    """Commit changes to the provided database connection."""
    logger.debug("Attempting to commit {}".format(database_connection.__repr__()))
    database_connection.commit()
    logger.debug("Commit successful for {}".format(database_connection.__repr__()))


def close_db(db1):
    """Close the connection to the provided database."""
    try:
        db1.close()
        logger.debug("Closed db")
    except Exception:
        logger.error("Error attempting to close db", exc_info=True)


def pp(item):
    """Print an item, then pause until the user is ready to proceed."""
    print("")
    print(item)
    logger.debug("\n" + item)
    pause()


def pl(items):
    """Print all items in a provided list/tuple, show the number of items in the list, then pause."""
    for item in items:
        print(item)
        logger.debug(item)
    print("Total items: {}".format(len(items)))
    logger.debug("Total items: {}".format(len(items)))
    pause()


def pl_sorted(items):
    """Sort the provided list, print all items, show the number of items in the list, then pause."""
    new_list = list()
    dict_items = set(items)
    for di in dict_items:
        new_list.append(di)

    new_list.sort()

    for nl in new_list:
        print(items.count(nl), nl)
        logger.debug("{count} {n}".format(count=items.count(nl), n=nl))
    pause()


def pd(items):
    """Print all items in a provided dict, then pause until the user is ready to proceed."""
    for item in items:
        print("{item}: {value}".format(item=item, value=items[item]))
        logger.debug("{item}: {value}".format(item=item, value=items[item]))
    pause()
    print("")


def stop(reason):
    """Shortcut to the final steps of the module: generate an html file, update likes, raffle, and commit/close dbs."""
    global name, ongoing, http_session

    logger.info('Entered the stop() function because: {}'.format(reason))
    update_file(name)
    if ongoing:
        update_likes(name)

    # run_raffle(1, name)
    commit(db)
    commit(dbsql)
    close_db(db)
    close_db(dbsql)
    http_session.close()

    time_end_stop = datetime.datetime.now()

    print("\n***** ***** **** ***** *****")
    print("  End:", time_end_stop, "[stop function]")
    print("  Total time: {} seconds\n\n".format(round((time_end_stop - time_start).total_seconds(), 2)))
    print("***** ***** **** ***** *****\n")
    logger.info("END via the stop() function @ {}".format(time_end_stop.strftime("%Y-%m-%d %H:%M:%S")))
    logger.info("Total time: {} seconds\n\n".format(round((time_end_stop - time_start).total_seconds(), 2)))

    quit()


def return_list_of_values(my_list):
    """Convert a list of tuples (or tuple-like objects) to a list of strings."""
    logger.debug("Starting return_list_of_values() with: {}".format(my_list))
    if my_list is None or len(my_list) == 0:
        logger.debug("my_list is None, or len(my_list)==0")
        logger.debug("End return_list_of_values()")
        return None
    list_to_return = [my for my, in my_list]
    logger.debug("Ending return_list_of_values() with: {}".format(list_to_return))
    return list_to_return


def return_first_value(input_object):
    """Return the first value in a list, tuple, or tuple-like object."""
    logger.debug('Starting return_first_value() with: {}'.format(input_object))
    if input_object is None:
        logger.debug('Ending return_first_value() unchanged')
        return input_object
    try:
        # return_list_of_values(input_object)
        logger.debug('Ending return_first_value() with: {}'.format(str(input_object[0])[:20]))
        return input_object[0]
    except TypeError:
        logger.error('Error:', exc_info=True)
        logger.debug('Ending return_first_value() unchanged')
        return input_object


def remove_newlines(html_input):
    """Remove excess newlines and line breaks from a chunk of html."""
    logger.debug("Starting remove_newlines()")
    text_output = str(html_input)

    br2 = """<br/>
    <br/>"""
    br3 = """<br/>
    <br/>
    <br/>"""

    br2nl = """<br/>\n<br/>"""
    br3nl = """<br/>\n<br/>\n<br/>"""

    while '\n\n\n' in text_output:
        text_output = text_output.replace('\n\n\n', '\n\n')
    while br3 in text_output:
        text_output = text_output.replace(br3, br2)
    while br3nl in text_output:
        text_output = text_output.replace(br3nl, br2nl)

    logger.debug("Ending remove_newlines()")
    return text_output


def find_last_post(html):
    """Given an html page_number input, return page_number metadata in list form: [last_post, last_post_id, page_number]"""
    logger.debug("Starting find_last_post()")

    lp_soup = BeautifulSoup(html, 'html.parser')
    # focus on the specific div
    all_posts = lp_soup('div', class_="publicControls")
    # carve out info about the last post on the page_number
    last_post_info = all_posts[len(all_posts)-1].find('a', title='Permalink')
    # user-facing number for the last post
    last_post = int(last_post_info.text.replace('#', '').strip())
    # system id for the last post
    last_post_id = int(re.findall('#post-(\d+)', last_post_info.get('href'))[0])

    page_number = str(lp_soup.find('link', rel="canonical"))
    if page_number is not None:
        page_number = re.findall('href=\"(.+?)\"', page_number)[0]
        if page_number[-1] == '/':
            page_number = 1
        elif re.search('/page_number-\d+', page_number):
            page_number = int(re.findall('/page_number-(\d+)', page_number)[0])
        else:
            page_number = None

    logger.debug("Ending find_last_post() with: {}".format([last_post, last_post_id, page_number]))
    return [last_post, last_post_id, page_number]


def month_to_num(month):
    """Return the month as a 2-character, zero-padded numeric string because I'm a newbie."""
    logger.debug("Starting month_to_num() with: {}".format(month))
    if month == 'Jan':
        return '01'
    elif month == 'Feb':
        return '02'
    elif month == 'Mar':
        return '03'
    elif month == 'Apr':
        return '04'
    elif month == 'May':
        return '05'
    elif month == 'Jun':
        return '06'
    elif month == 'Jul':
        return '07'
    elif month == 'Aug':
        return '08'
    elif month == 'Sep':
        return '09'
    elif month == 'Oct':
        return '10'
    elif month == 'Nov':
        return '11'
    elif month == 'Dec':
        return '12'
    else:
        return 'Month Error'


def to_timestamp(ds):
    """Convert a text date/time sting to ISO-8601 formatting: 'Mar 26, 2018 at 9:48 PM' --> '2018-03-26 21:48:00.'"""
    logger.debug("Starting to_timestamp() with: {}".format(ds))

    year = int(re.findall(', (20\d\d) at', ds)[0])
    month = int(month_to_num(ds[:3]))
    day = int(re.findall(' (\d+),', ds)[0])
    minute = int(re.findall(' at \d+:(\d+) ', ds)[0])
    hour = int(re.findall(' at (\d+):', ds)[0])

    # update to 24-hour time
    if ds[-2].lower() == 'p' and hour != 12:
        hour += 12
    elif ds[-2].lower() == 'a' and hour == 12:
        hour -= 12
    else:
        pass

    logger.debug("Ending to_timestamp() with: {}".format(datetime.datetime(year, month, day, hour, minute)))
    return datetime.datetime(year, month, day, hour, minute)


def to_date(ds):
    """Convert a text date string to ISO-8601 formatting: 'Mar 26, 2018' --> '2018-03-26'."""
    logger.debug("Starting to_date() with: {}".format(ds))

    year = int(re.findall(', (20\d\d)', ds)[0])
    month = int(month_to_num(ds[:3]))
    day = int(re.findall(' (\d+),', ds)[0])

    logger.debug("Ending to_date() with: {}".format(datetime.datetime(year, month, day)))
    return datetime.datetime(year, month, day)


def replace_goto(goto_input):
    """Replace the goto links in quoted posts with local, relative anchors.

    Need to figure out how the js links should be formatted before implementing this.
    """
    logger.debug("Starting replace_goto() with: {}".format(goto_input))

    count = 0
    goto_output = goto_input
    all_quotes = goto_output.find_all('div', class_="attribution type")
    goto_output = str(goto_output)
    while count < len(all_quotes):
        # grab the anchor's post id
        if 'class="AttributionLink"' in all_quotes[count]:
            quote_id = re.findall('href=\".+?#post-(\d+)\"',
                                  str(all_quotes[count].find('a', class_="AttributionLink")))[0]
        else:
            # when a post gets deleted, it has no AttLink
            count += 1
            continue
        # save the original before we make any edits
        old_quote = str(all_quotes[count])
        new_quote = all_quotes[count]

        # delete the existing hyperlink
        goto_link = str(new_quote.find('a', class_="AttributionLink"))
        goto_id = re.findall('href=\".+?#post-(\d+?)\"', goto_link)[0]
        new_link = ' <a href="#' + goto_id + '" class="AttributionLink">&uarr;</a>'
        new_quote = str(new_quote).replace(goto_link, new_link)
        new_quote = new_quote.replace('\n', '')

        # add the new hyperlink + js
        # new_quote = new_quote.replace('</div>', "\n<a id='post" + quote_id
        # new_quote += "' class='AttributionLink'></a>" +js+ "</div>")

        goto_output = goto_output.replace(str(old_quote), str(new_quote))
        count += 1

    logger.debug("Ending replace_goto with: {}".format(goto_output))
    return goto_output


def remove_ols(html_input):
    """Replace all <ol> tags with <ul> tags."""
    output = str(html_input)
    while '<ol>' in output or '</ol>' in output:
        output = output.replace('<ol>', '<ul>')
        output = output.replace('</ol>', '</ul>')
    return output


def write_post_raw(post):
    """Write raw Post data to the database."""
    logger.debug("Starting write_post_raw() for {name} id={id}".format(name=post.thread_name, id=post.id))
    global db

    # create a Posts_Soup object
    ps = Posts_Soup(id=post.id, thread_name=post.thread_name, soup=post.soup)

    # add/update post data
    db.add(ps)
    logger.debug('Added/updated post_soup for {thread} id={id}'.format(id=post.id, thread=post.thread_name))
    print('Added/updated post_soup for {thread} id={id}'.format(id=post.id, thread=post.thread_name))


def write_post(post):
    """Write cleaned data for a Post to the database."""
    logger.debug("Starting write_post() for {name} id={id}".format(name=post.thread_name, id=post.id))
    global db

    # insert/update this post
    db.add(post)
    logger.debug("Added/updated post for {thread} id={id}".format(id=post.id, thread=post.thread_name))
    print("Added/updated post for {thread} id={id}".format(id=post.id, thread=post.thread_name))


def write_users(user_list):
    """Add users to the database.  If a user already exists, update their data."""
    logger.debug("Starting write_users() with: {}".format(user_list))
    global db

    for u in user_list:
        db.add(u)
        print('Added/updated user {user}, id: {id}'.format(user=u.username, id=u.id))
        logger.debug('Added/updated user {user}, id: {id}'.format(user=u.username, id=u.id))


def write_likes(like):
    """Write new/updated likes to the database."""
    logger.debug("Start of write_likes()")
    global db

    # insert/update this like
    db.add(like)
    logger.debug("Added/updated like for {post} by user {user}".format(post=like.post_id, user=like.user_id))
    print("Added/updated like for {post} by user {user}".format(post=like.post_id, user=like.user_id))


def get_user_data(uid):
    """Given a user_id, return a dictionary with their user data."""
    logger.debug("Starting get_user_data() with: {}".format(uid))

    # read & soupify the html for this user's page_number
    user_page_url = db.query(URLs.user_page).first()[0] + str(uid)
    user_soup = make_soup(s.get(user_page_url).text)

    user_name = user_soup.find('h1', class_="username").text
    user_uid = int(re.findall('/community/members/\S+?\.(\d+?)/', str(user_soup.find('link', rel="canonical")))[0])

    # there's no easy way to find the join date
    join_date = None
    join_date_text = user_soup.find('div', class_="section infoBlock").find_all('dd')

    for j in join_date_text:
        if ', 20' in j.text and ':' not in j.text:
            join_date = str(to_date(j.text))[:10]
            break

    # users have the option of sharing their location
    try:
        loc = user_soup.find('a', itemprop="address").text
    except Exception:
        loc = None

    # return user data as a dictionary
    user_dict = {'id': user_uid, 'username': user_name, 'joindate': join_date, 'location': loc}

    logger.debug("Ending get_user_data() with: {}".format(user_dict))
    return user_dict


def add_post_details(panel, post_info):
    """Insert a new data row at the bottom of the detail panel, which is the section to the left side of each post.

    Inputs:
    * panel - an html string of the existing panel
    * post_info - a list of (label, value) tuples to add
    """
    logger.debug("Starting add_post_details() with: {panel}, {info}".format(panel=panel, info=post_info))

    for pi in post_info:
        repl = '<dl class="pairsJustified"><dt>{pi0}:</dt><dd>{pi1}</dd></dl>\n</div>'.format(pi0=pi[0], pi1=pi[1])
        panel = panel.replace('</div>', repl)

    logger.debug("Ending add_post_details()")
    return panel


def update_file(thread_name):
    """Create an html file based on user input from a templated set of options.  Existing files are overwritten."""
    logger.debug('Start of update_file() for: {}'.format(thread_name))

    output_options = db.query(Output_Options)
    valid_options = []
    print("\nContent options for the {} html file:".format(thread_name))
    for o in output_options:
        print("{id}: {val}".format(id=o.__dict__['id'], val=o.__dict__['option']))
        valid_options.append(o.__dict__['id'])

    print("")
    val = False
    user_option = input("Option: ")
    while not val:
        try:
            user_option = int(user_option)
            if user_option in valid_options:
                val = True
                break
        except ValueError:
            logger.error('Cannot convert str({}) to int'.format(user_option), exc_info=True)
        except IndexError:
            logger.debug('User entry {u} is an invalid selection'.format(u=user_option))
        user_option = input("Option: ")

    user_option_text = output_options[user_option].option
    logger.debug('User selected option {opt}: {val}'.format(opt=user_option, val=user_option_text))

    # get the first page_number of the thread
    query = text('SELECT html FROM raw.thread_page WHERE name = :n and page = 0')
    result = dbsql.execute(query, n=thread_name)
    html = return_first_value(result.fetchone())

    # find the max page_number number we've recorded
    query = text('SELECT max(page) FROM raw.thread_page WHERE name = :n')
    result = dbsql.execute(query, n=thread_name)
    maxpage = return_first_value(result.fetchone())

    # remove ads flags
    if "enable_page_level_ads: true" in html:
        html = html.replace("enable_page_level_ads: true", "enable_page_level_ads: false")
    if "adsbygoogle" in html:
        html = html.replace("adsbygoogle", "noadsplzthx")

    # erase the header & quickReply sections
    soup = make_soup(html)
    current_page = int(re.findall('Page (\d+) of \d+', soup.find('span', class_="pageNavHeader").text)[0])
    current_maxpage = int(re.findall('Page \d+ of (\d+)', soup.find('span', class_="pageNavHeader").text)[0])
    if soup.find('a', title="Open quick navigation") is not None:
        soup.find('a', title="Open quick navigation").decompose()
    if soup.find('header') is not None:
        soup.find('header').decompose()
    if soup.find('div', class_="quickReply message") is not None:
        soup.find('div', class_="quickReply message").decompose()
    if soup.find('div', id="loginBar") is not None:
        soup.find('div', id="loginBar").decompose()

    # find current max page_number, replace with the new max page_number
    html = str(soup)  # soup re-arranges some of the tag attributes, which wouldn't work for replace() below
    html = html.replace('data-last="' + str(current_maxpage) + '"', 'data-last="' + str(maxpage) + '"')
    html = html.replace('Page ' + str(current_page) + ' of ' + str(current_maxpage), 'Page ' + str(current_page) + ' of ' + str(maxpage))
    html = html.replace('page_number-'  + str(current_maxpage), 'page_number-' + str(maxpage))
    html = html.replace('>' + str(current_maxpage) + '<', '>' + str(maxpage) + '<')
    html = html.replace('<div id="headerMover">', '<br/>')

    # replace everything after the </ol> with a basic footer
    html = html[:html.index('</ol>') + 5] + '\n<hr>\n\n</form>\n<i>fin</i>\n</body>\n</html>'

    # 'option' determines which SQL query to use
    if user_option == 4:  # only hint-related posts by users not ruled out as brystmar's sender, ordered by username
        if thread_name not in ['SSF14', 'SSF15', 'SSF16', 'SSF17', 'SSF18', 'Fest18']:
            stop("Possible sender data for brystmar only exists for SSF14+")

        query = text("""SELECT DISTINCT r.id, r.soup, p.username, p.timestamp, p.thread_page
                        FROM raw.posts_soup r
                        JOIN public.posts p ON r.id = p.id
                        JOIN public.biffers b ON p.user_id = b.user_id AND p.thread_name = b.thread_name
                        WHERE p.thread_name = :n
                            AND r.soup is not null
                            AND b.my_sender is null
                            AND p.hint = 1
                            AND p.user_id <> 456
                        ORDER BY p.username, r.id""")
        data = dbsql.execute(query, n=thread_name).fetchall()

    elif user_option == 1:  # all posts, in sequential order
        query = text("""SELECT r.id, r.soup, p.username, p.timestamp, p.thread_page
                        FROM raw.posts_soup r
                        JOIN public.posts p ON r.id = p.id
                        WHERE p.thread_name = :n AND r.soup is not null
                        ORDER BY r.id""")
        data = dbsql.execute(query, n=thread_name).fetchall()

    elif user_option == 2:  # known hauls only, in sequential order
        query = text("""SELECT r.id, r.soup, p.username, p.timestamp, p.thread_page
                    FROM raw.posts_soup r
                    JOIN public.posts p ON r.id = p.id
                    JOIN public.biffers b ON p.id = b.haul_id AND p.thread_name = b.thread_name
                    WHERE p.thread_name = :n
                        AND r.soup is not null
                    ORDER BY r.id""")
        data = dbsql.execute(query, n=thread_name).fetchall()

    elif user_option == 3:  # derived hauls only, in sequential order (2+ pics or 2+ non-quoted instagram posts)
        query = text("""SELECT r.id, r.soup, p.username, p.timestamp, p.thread_page
                    FROM raw.posts_soup r
                    JOIN public.posts p ON r.id = p.id
                    WHERE p.thread_name = :n
                        AND r.soup is not null
                        AND (p.pics >= 2 OR p.text like '%\n[instagram]%\n[instagram]%')
                    ORDER BY r.id""")
        data = dbsql.execute(query, n=thread_name).fetchall()

    elif user_option == 5:  # BYO SQL
        last_query = """SELECT r.id, r.soup, p.username, p.timestamp, p.thread_page
                    FROM raw.posts_soup r
                    JOIN public.posts p ON r.id = p.id
                    WHERE p.thread_name = '{name}'
                      AND """.format(name=thread_name).replace("                    ", "")
        user_query = input("Complete this query:\n{}\n".format(last_query))
        query = text(last_query + user_query.replace('"', "'"))
        logger.debug('Running user-entered query:\n{}'.format(query))
        data = dbsql.execute(query).fetchall()

    elif user_option == 0:
        return None

    # data = result.fetchall()

    # validation
    if data is None:
        logger.debug('No data returned from update_file query #{}'.format(user_option))
        stop("No data returned")

    # grab the post_id URL
    post_url = db.query(URLs.post).first()[0]

    # insert posts into the html shell
    remove = []
    postinfo = []
    count = 0
    for d in data:
        remove.clear()
        postinfo.clear()
        post_id = str(d[0])
        post_timestamp = d[3]
        page_number = str(d[4])

        postinfo.append(("Post ID", post_id))
        postinfo.append(("Page", '<a class="page_number" target="_blank" href="' + post_url +
                         post_id + '">' + page_number + '</a>'))

        sp = make_soup(remove_ols(d[1]))
        raw = str(sp)

        likes_id = 'likes-post-' + str(sp.a['thread_name'])
        remove.append(str(sp.find('div', id=likes_id)))
        remove.append(str(sp.find('h3', class_="userTitle userText")))
        remove.append(str(sp.find('div', class_="avatarHolder")))
        # remove.append(str(sp.find('div', class_="extrauserpanelinfo"))) #location & join date
        remove.append(str(sp.find('div', class_="messageMeta ToggleTriggerAnchor")))  # user/timestamp summary
        remove.append(str(sp.find('strong', class_='newIndicator')))  # new post indicators

        # adjust stuff in the user info section
        userpanelinfo = sp.find('div', class_="extraUserInfo")
        # get the hyperlink for the user's location
        locationlink = userpanelinfo.find('a')
        # add stuff to the user detail panel
        userpanelinfo = str(userpanelinfo)
        postinfo.append(("Date", post_timestamp.strftime("%Y-%m-%d %H:%M:%S")))

        raw = raw.replace(userpanelinfo, add_post_details(userpanelinfo, postinfo))

        # remove the location hyperlink (if the user shows their location)
        if locationlink is not None:
            loc = locationlink.text
            if len(loc) > 18:  # truncate the location string if it's too long
                spaces = list(reversed(find_substring(loc, ' ')))
                for space in spaces:
                    if space <= 18:
                        break
                loc = loc[:min(space, 18)] + '...'
            raw = raw.replace(str(locationlink), loc)

        if sp.find('div', id="pic_blob") is not None:
            for pic in sp.find_all('div', id="pic_blob"):
                remove.append(str(pic))
        if sp.find('div', id="media_blob") is not None:
            for media in sp.find_all('div', id="media_blob"):
                remove.append(str(media))

        r = 0
        while r < len(remove):
            if remove[r] is not None and remove[r] != 'None':
                raw = raw.replace(remove[r], '')
            r += 1

        count += 1

        if count == len(data):
            html = html.replace('</ol>', raw + '\n<br/>\n</ol>')
        else:
            html = html.replace('</ol>', raw + '\n<br/>\n<br/>\n</ol>')

    # add summary data to the footer
    footer_html = '\n<footer>\n<div class="footerLegal">\n<div class="pageWidth">\n<div class="pageContent">\n'
    footer_html += '<div id="copyright">A mediocre concatenation of {} posts by brystmar</div>\n'.format(count)
    footer_html += '</div>\n</div>\n</div>\n</footer>\n</html>'
    html = html.replace('</html>', footer_html)

    # write the output
    file_path = 'html-output/{subdir}/'.format(subdir=thread_name)
    file_name = '{name} {opt}.html'.format(name=thread_name, opt=user_option_text)
    print("\nWriting {qty} posts to: {file}".format(qty=len(data), file=file_name))
    logger.debug("Writing {qty} posts to: {file}".format(qty=len(data), file=file_name))

    with open(file_path + file_name, 'w') as file:
        logger.debug('Begin writing html file: /{}'.format(file_path + file_name))
        file.write(html)
        logger.debug('Done writing html')

    print('Done.\n')


def update_likes(thread_name):
    """Read & write likes for a user-specified set of posts within a specified thread."""
    logger.debug('Starting update_likes() for: {}'.format(thread_name))

    # Only prompt for SSF and Festivus threads
    if 'ssf' in thread_name.lower() or 'fest' in thread_name.lower():
        likes_input = input("Update likes? ")
    else:
        logger.debug("Thread {} doesn't contain SSF or Fest and is ineligible for updating likes".format(thread_name))
        return None

    if likes_input.lower() not in ['y', 'yes', '1']:
        logger.debug("User declined to update likes")
        return None
    else:
        logger.debug("User wants to update likes.")

    # Ensure our session is logged in
    global login_status
    if not login_status:
        logger.debug("beetsbot must log into talkbeer.com")

        login_status = login_to_talkbeer()

    # get post_id & timestamp for the most recently-recorded 'liked' post
    query = text("""SELECT post_id, max(timestamp)
                FROM public.likes
                WHERE post_id = (SELECT max(l.post_id) as maxpost 
                                FROM public.likes l
                                JOIN public.posts p on l.post_id = p.id
                                WHERE p.thread_name = :tn)
                GROUP BY post_id""")
    result = dbsql.execute(query, tn=thread_name)
    last_like = return_first_value(result.fetchall())

    if len(last_like) == 0:
        # no liked posts yet
        recent_post_text = "No likes found for {}.".format(thread_name)
    else:
        # how many days ago was that 'like' timestamp?
        time_ago = datetime.datetime.now() - dateutil.parser.parse(last_like[1])
        days_ago = round(time_ago.days + round(time_ago.seconds/(24*60*60), 2), 2)

        recent_post_text = "Most recently-liked {} post was {} days ".format(thread_name, days_ago)
        recent_post_text += "ago: id={} at {}.".format(last_like[0], last_like[1])

    print(recent_post_text + "\n")
    logger.debug(recent_post_text)
    starting_post = input("Enter post_id, timestamp, or # days to retrieve posts from: ")
    cutoff_date = None

    # figure out what the user entered, then submit the right query
    try:
        starting_post = int(starting_post)
        logger.debug("User entry for likes: {}".format(starting_post))
        if starting_post > 180:
            # user entered a post_id
            logger.debug("Detected a post_id")
            query = text("""SELECT DISTINCT id, timestamp
                        FROM public.posts
                        WHERE id >= :i
                            AND thread_name = :tn
                        ORDER BY id
                        LIMIT 1500""")
            result = dbsql.execute(query, i=starting_post, tn=thread_name)
        else:
            # user entered a number of days
            logger.debug("Detected a number of days")
            cutoff_date = str(datetime.datetime.now() + datetime.timedelta(-starting_post))[:10]
            query = text("""SELECT DISTINCT id, timestamp
                        FROM public.posts
                        WHERE timestamp >= :ts
                            AND thread_name = :tn
                        ORDER BY id LIMIT 1500""")
            result = dbsql.execute(query, ts=cutoff_date, tn=thread_name)
    except Exception:
        logger.error("Error in try/except in update_likes() function".format(starting_post), exc_info=True)
        if '-' in starting_post:
            # user entered a timestamp
            logger.debug("Detected a timestamp")
            query = text("""SELECT DISTINCT id, timestamp
                        FROM public.posts
                        WHERE timestamp >= :ts
                            AND thread_name = :tn
                        ORDER BY id
                        LIMIT 1500""")
            result = dbsql.execute(query, ts=cutoff_date, tn=thread_name)
        else:
            logger.debug("Unable to detect the type of user entry")
            return None

    # load post_ids into memory
    post_ids = result.fetchall()

    # update likes
    i = 0
    if post_ids is not None:
        logger.debug("Begin writing updated likes, total: {}".format(len(post_ids)))
        for pid in post_ids:
            read_likes(pid[0])
            i += 1
            if i % 10 == 0:
                commit(db)
                print("{i} of {qty} post_id={pid} COMMIT".format(i=i, qty=len(post_ids), pid=pid[0]))
                logger.debug("{i} of {qty} post_id={pid} COMMIT".format(i=i, qty=len(post_ids), pid=pid[0]))
            else:
                print("{i} of {qty} post_id={pid}".format(i=i, qty=len(post_ids), pid=pid[0]))
                logger.debug("{i} of {qty} post_id={pid}".format(i=i, qty=len(post_ids), pid=pid[0]))
    logger.debug("Done writing updated likes")
    commit(db)

    # if any unknown users were added to the 'likes' table, add them to the 'users' table too
    query = text("""SELECT distinct user_id
                    FROM public.likes
                    WHERE user_id not in (SELECT distinct id FROM public.users)
                    ORDER BY user_id""")
    result = dbsql.execute(query)
    users = result.fetchall()
    if users is None or len(users) < 1:
        logger.debug("No users to update")
    else:
        # add missing users to the 'users' table
        logger.debug("Found {} missing users to add".format(len(users)))
        for u in users:
            write_users(get_user_data(u[0]))

    logger.debug("End of update_likes()")


def read_likes(post_id):
    """Get data about likes for the specified post_id."""
    logger.debug("Start of read_likes()")

    global s
    likes_url = 'https://www.talkbeer.com/community/posts/{pid}/likes'.format(pid=post_id)
    html = s.get(likes_url).text
    likes_soup = make_soup(html)
    # soup = make_soup(s.get(url).text)
    likes = likes_soup.find_all('li', class_="primaryContent memberListItem")

    for li in likes:
        likes_time_blob = str(li.find('div', class_="extra"))
        likes_timestamp = to_timestamp(re.findall('.+(\w\w\w \d+, 20\d\d at \d+:\d+ [APap][Mm])', likes_time_blob)[0])
        likes_user_id = int(re.findall('href=\"members/.*\.(\d+?)/\"', str(li))[0])
        # print(timestamp, " <--> ", user_id)
        write_likes(post_id, likes_user_id, likes_timestamp)

    logger.debug("End of read_likes()")


def determine_thread():
    """Return the thread name and the highest page_number number that's been scraped & stored in the raw.thread_page table."""
    logger.debug('Start of determine_thread()')

    # If there's only one thread with new posts scraped, pick that one by default
    query = text("""WITH tp_maxpost as
                        (SELECT name, max(last_post_id) as last_post FROM raw.thread_page GROUP BY 1 ORDER BY 1),
                    p_maxpost as
                        (SELECT thread_name as name, max(id) as last_post FROM public.posts GROUP BY 1 ORDER BY 1)
                    
                    SELECT distinct tp.name
                    FROM tp_maxpost tp
                    JOIN p_maxpost p ON tp.name = p.name
                    WHERE tp.last_post > p.last_post
                    ORDER BY 1""")
    result = dbsql.execute(query)
    to_agg = return_list_of_values(result.fetchall())
    logger.debug('to_agg={}'.format(to_agg))

    # if there's only one thread name returned, use that thread name and don't prompt the user
    global page_number, ongoing
    thread_name = None
    if to_agg is None or len(to_agg) != 1:  # prompt user for the thread to parse
        # retrieve list of all thread nicknames
        result = dbsql.execute(text('SELECT distinct name FROM raw.thread_page order by 1'))
        thread_names = return_list_of_values(result.fetchall())  # returns a list of strings instead of a list of tuples
        logger.debug('Thread names: {}'.format(thread_names))

        # show existing threads
        print('Existing threads:')
        for tn in thread_names:
            print(tn)
        print("")

        # user input
        while thread_name not in thread_names:
            thread_name = input("Enter thread to parse: ")
            if thread_name is None or thread_name == '':
                db.close()
                print("")
                logger.debug("User didn't enter a thread name.  Quitting.")
                quit()
            # input should be case insensitive
            for tn in thread_names:
                if tn.lower() == thread_name.lower():
                    thread_name = tn
    elif len(to_agg) == 1:
        thread_name = to_agg[0]
        print("Parsing thread: {}\n".format(thread_name))
        logger.debug("Parsing thread: {}\n".format(thread_name))
    else:
        stop("Error finding the thread to scrape.")

    logger.info("User wants to parse data from {}".format(thread_name))

    # compare the last post from scraped thread data to the last post in the posts table
    query = text('SELECT max(last_post_id) FROM raw.thread_page WHERE name = :a')
    result = dbsql.execute(query, a=thread_name)
    lp_threads = return_first_value(result.fetchone())

    query = text('SELECT max(id) FROM public.posts WHERE thread_name = :a')
    result = dbsql.execute(query, a=thread_name)
    lp_posts = return_first_value(result.fetchone())

    # is this thread ongoing?
    query = text('SELECT distinct name FROM public.threads WHERE name = :a and ongoing = :b')
    result = dbsql.execute(query, a=thread_name, b='Y')
    ongoing = return_first_value(result.fetchone())

    if ongoing == thread_name:
        ongoing = True
    else:
        ongoing = False

    # load the pertinent thread data into the 'data' variable
    if lp_threads is None:
        stop("No thread data for {}".format(thread_name))
    elif lp_threads == lp_posts:
        stop("No new posts found for {}".format(thread_name))
    elif lp_posts is None:
        page_number = 1  # no post data recorded for this thread yet
    elif lp_threads > lp_posts:
        # found new posts that we need to parse
        # find the page_number containing the first post that's not in the db
        result = db.execute(text('SELECT distinct thread_page, num FROM public.posts WHERE id = :a', a=lp_posts))
        check_posts_on_page = result.fetchone()
        # if this is the 20th post on the page_number, skip to the next page_number
        if check_posts_on_page[1] % 20 == 0:
            page_number = check_posts_on_page[0] + 1
        else:
            page_number = check_posts_on_page[0]
    else:
        stop("More posts exist in the posts table than raw thread html.  Re-scrape the thread for {}".format(thread_name))

    logger.debug('End of determine_thread(), found thread: {}'.format(thread_name))
    print('End of determine_thread(), found thread: {}'.format(thread_name))

    return thread_name


def thumbnail_fix(post_data):
    """Adjust tb-native thumbnails to use the same schema as tb-native full images."""
    logger.debug("Start of thumbnail_fix()")

    top_class = "messageText SelectQuoteContainer ugc baseHtml"
    thumbs = post_data.find('blockquote', class_=top_class).find_all('a', class_="LbTrigger")
    if thumbs is None or isinstance(thumbs, list):
        return post_data

    pp(post_data)
    print("")

    count = 0
    post_data = str(post_data)
    while count < len(thumbs):
        # ex: https://www.talkbeer.com/community/attachments/994f94d8-9eba-4450-bff0-d83a3e65c3f7-jpeg.458/
        a_href = str(thumbs[count].get('href'))
        pic_id = re.findall('.*community\/attachments\/.*\.(\d+)\/', a_href)[0]

        img_tag = thumbs[count].find('img')
        thumb_src = str(img_tag.get('src'))
        new_src = 'https://www.talkbeer.com/community/attachments/{id}/'.format(id=pic_id)
        new_img = str(img_tag).replace(thumb_src, new_src)

        post_data = post_data.replace(str(thumbs[count]), new_img)
        count += 1

    logger.debug("End of thumbnail_fix()")
    return make_soup(post_data)


def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    """Add DEBUG-level logging to every database request using the cursor."""
    # logger.debug("Received statement: %s", statement)
    pass


def connect_to_db():
    """Connect to the CloudSQL database, returning both the ORM connection (db) and a cursor (dbsql)."""
    cloud_server = 'postgresql+psycopg2://{user}:{pw}@/{db_name}'.format(user=Config.db_user, pw=Config.db_pw,
                                                                         db_name=Config.db_name)
    cloud_server += '?host=/cloudsql/{db_instance}'.format(db_instance=Config.db_instance)
    logger.debug('Cloud DB: {}'.format(cloud_server))

    # create the structural SQLAlchemy objects
    cloud_engine = create_engine(cloud_server, echo=True)

    # add debug logging for each query before & after execution
    event.listen(cloud_engine, "before_cursor_execute", before_cursor_execute)

    Session = sessionmaker(bind=cloud_engine)
    logger.debug('DB session object created')

    return [Session(), cloud_engine.connect()]


def initialize_http_session():
    """Open & initialize an http session to talkbeer.com

    Although Gene gave me his blessing to scrape data from tb, the talkbeer forum software doesn't
    # allow scraping by default.  We need to mask the request as a different user agent.
    """
    # requests.packages.urllib3.disable_warnings(InsecureRequestWarning)  # suppress SSL warning messages
    new_http_session = requests.session()

    new_http_session.headers.update({'User-Agent': 'Mozilla/66.0.2'})
    new_http_session.verify = False  # disable SSL verification
    logger.debug('HTTP session created & configured')

    return new_http_session


def login_to_talkbeer(session, credentials):
    """Log into talkbeer using the provided credentials."""
    logger.debug("Attempting to talkbeer login for user {}".format(credentials['login']))
    global db

    login_url = db.query(URLs.login).first()[0]
    request = session.post(login_url, data=credentials)

    if request.ok:
        logger.debug("{} successfully logged into talkbeer.com".format(credentials['login']))
        return True
    else:
        logger.debug("Login unsuccessful using credentials: {}".format(credentials))
        logger.debug(request.json)
        logger.debug("Terminating session.")

        session.close()
        close_db(db)
        close_db(dbsql)
        quit()


# connect to the db; separate out each connection
database_connections = connect_to_db()
db = database_connections[0]
dbsql = database_connections[1]

http_session = initialize_http_session()
login_status = False  # since we haven't logged in yet

# initialize global variables
page_number = 0
ongoing = False
ulist = []

# determine which thread to parse
name = determine_thread()  # sets the global name & page_number variables

# get html for the thread pages we want to parse and the thread's base URL
pages = db.query(Thread_Page).filter(Thread_Page.name == name, Thread_Page.page >= page_number)

# sql = text('SELECT distinct page, html FROM raw.thread_page WHERE name = :n and page >= :p ORDER BY page')
# data = dbsql.execute(sql, n=name, p=page_number)  # returns a list of tuples

# get the thread's base url
sql = text('SELECT distinct url FROM raw.thread_page WHERE name = :n and page = 1 LIMIT 1')
result = dbsql.execute(sql, n=name)
url = return_first_value(result.fetchone)

# user_id list of all BIF participants
sql = text('SELECT distinct user_id FROM public.biffers WHERE thread_name = :tn ORDER BY 1')
biffers_list = dbsql.execute(sql, tn=name)

# iterate through the thread pages to extract data about each post
page_counter = 1
for d in pages:
    # if d[0] > 1: break
    print("Parsing page_number", d[0], "(" + str(page_counter), "of", str(len(data)) + ")")  # + ". Users:", len(ulist))
    # read, soup-ify the page_number
    soup = make_soup(d[1])
    page_number = d[0]

    posts = soup.find_all('li')  # class_="message   ")
    # loop through each post on this page_number
    for p in posts:
        # get the post_id
        try:
            post_id = int(re.findall('post-(\d+)', p['id'])[0])
        except Exception:
            logger.debug("Couldn't retrieve the post_id for a post on page_number {}".format(page_counter))
            continue

        # replace any <ol>s with <ul>s
        p = make_soup(remove_ols(p))

        # tb-native thumbnails need to be adjusted to use a similar schema to tb-hosted images
        p = thumbnail_fix(p)

        # create a slice that contains user data
        ud = p.find('div', class_="messageUserInfo")
        # extract the post_id
        temp = str(p)[:str(p).find('>') + 1]
        hint = 0

        pnhpot_class = "item muted postNumber hashPermalink OverlayTrigger"
        post_num = int(p.find('a', class_=pnhpot_class).text.replace('#','').strip())
        if page_number == 1:
            if post_num == 1:
                post_url = url
            else:
                post_url = url + '#post-' + str(post_id)
        else:
            post_url = url + 'page_number-' + str(page_number) + '#post-' + str(post_id)

        username = p.find('h3', class_="userText").a.text
        user_id = int(re.findall('/.+\.(\d+?)/\"', str(p.find('h3', class_="userText")))[0])

        # date & time are displayed in one of two ways
        time_blob = p.find('a', class_="datePermalink")
        if '<abbr' in str(time_blob):
            timestamp = to_timestamp(time_blob.text.strip())
        else:
            timestamp = to_timestamp(re.findall('title=\"(.+)\"', str(time_blob.find('span', class_="DateTime")))[0])

        # remove user signature, if applicable
        try: p.find('div', class_="baseHtml signature messageText ugc").decompose()
        except Exception: pass

        # create a variable to store the post without any quotes for proper gifs/pics/media counting
        p_noquotes = p.find('div', class_="messageInfo primaryContent")
        qblocks = p_noquotes.find_all('div', class_="bbCodeBlock bbCodeQuote")

        # replace the goto links in quoted posts with local, relative anchors
        p = replace_goto(p)  # function returns a string
        p_noquotes = str(p_noquotes)

        # add anchor tags for each post
        p = '<div id="' + str(post_id) + '"><a name="' + str(post_id) + '"></a>\n' + str(p) + '\n</div>'

        # if there are images, append a text link to the image before the image itself
        p = make_soup(p)  # can't be consolidated into the next line since BS re-arranges the attributes in each tag
        pics = p.find('blockquote', class_="messageText SelectQuoteContainer ugc baseHtml").find_all('img')
        p = str(p)

        # remove quotes from the message body in the noquotes variable
        i = 0
        while i < len(qblocks):
            p_noquotes = p_noquotes.replace(str(qblocks[i]), '')
            i += 1

        i = 0
        gifs = 0
        pic_counter = 0
        while i < len(pics):  # iterate through the pics
            # need a string for regex and find/replace
            pic_blob = str(pics[i])
            # ignore smilies & other pics without a full source URL
            if 'data-url' not in pic_blob and 'src="https://www.talkbeer.com/community/attachments/' not in pic_blob:
                # tb-native pics are stored differently
                i += 1
                continue

            # extract the image URL
            if 'data-url' in pic_blob:
                # pic_link = re.findall('data-url=\"(.+?)\"', pic_blob)[0]
                pic_link = pics[i].get('data-url')
            elif 'src="https://www.talkbeer.com/community/attachments/' in pic_blob:
                pic_link = pics[i].get('src')
            p = p.replace(pic_blob, pic_link + '\n' + '<div id="pic_blob">' + pic_blob + '</div>')  # p is str

            i += 1

            if pic_link in p_noquotes: pic_counter += 1
            if pic_link in p_noquotes and ('.gif' in pic_link or '-gif.' in pic_link): gifs += 1

        # subtract gif count from the pic count
        pic_counter = pic_counter - gifs

        # if there are youtube video embeds, add a text link to the video before the video itself
        p = make_soup(p)  # can't be consolidated into the next line since BS re-arranges the attributes in each tag
        media = p.find('div', class_="messageContent").find_all('iframe')
        p = str(p)

        m = 0
        media_counter = 0
        while m < len(media):
            # if the iframe blob is encapsulated inside any <span> headers, change the blob to reflect that
            if media[m].parent.name == 'span' and media[m].parent.parent.name == 'span':
                media_blob = str(media[m].parent.parent)
            elif media[m].parent.name == 'span':
                media_blob = str(media[m].parent)
            else:
                media_blob = str(media[m])

            media_link = re.findall('src=\"(.+?)\"', media_blob)[0]
            # youtube videos are an iframe within a distinct <span>
            #  instagram posts are entirely enclosed in an iframe, no <span>
            #  gfycat embeds are a mix
            # adding a try/except (ugh) to cover these scenarios
            try:
                media_type = re.findall('mediaembed=\"(.+?)\"', media_blob)[0]
            except Exception:
                if 'youtube.com' in media_blob:
                    media_type = 'youtube'
                    media_link = media_link.replace('embed/', 'watch?v=').strip()
                elif 'src="/' in media_blob:
                    # uses the domain name with //domain.com/ prefix
                    media_type = re.findall('src=\".*\W([0-z]+)\.[0-z]+?/', media_blob)[0]
                else:
                    # uses the domain name as the media type
                    media_type = re.findall('src=\".*\.(.+?)\.', media_blob)[0]

            if media_type in('imgur', 'instagram'):  # count these as pics
                if '.gif' in media_blob or '-gif.' in media_blob:
                    media_type = media_type + ' iframe gif'
                    gifs += 1
                else:
                    media_type = media_type + ' iframe pic'
                    pic_counter += 1
            else:
                if media_link in p_noquotes: media_counter += 1

            p = p.replace(media_blob, '[' + media_type + '] ' + media_link + '\n' + '<div id="media_blob">' + media_blob + '</div>')  # p is still a string here
            m += 1

        # if there are quotes, add ASCII formatting for quoted text
        p = make_soup(p)  # can't be consolidated into the next line since BS re-arranges the attributes in each tag
        quotes = p.find_all('aside')
        p = str(p)

        q = 0
        while q < len(quotes):
            quote_blob = str(quotes[q])
            newquote = remove_newlines(quote_blob).replace('Click to expand...', '')
            # quote_blob = quote_blob.replace('&uarr;', '')
            newquote = newquote.replace(' said:\n', ' said:')
            newquote = newquote.replace('\n', '\n&#62; ')
            newquote = newquote.replace('&#62; <div class="attribution type">', '<div class="attribution type">')

            # add an extra newline after the last quote
            if q == len(quotes) - 1: newquote = newquote + '\n &nbsp; '

            p = p.replace(quote_blob, newquote)  # p is still a string here
            q += 1

        # extract the post's text into two variables: one with quoted posts, one without
        message = remove_newlines(str(make_soup(p).find('blockquote', class_="messageText SelectQuoteContainer ugc baseHtml").text).strip()).replace('Click to expand...', '')
        message_nq = remove_newlines(str(make_soup(p_noquotes).find('blockquote', class_="messageText SelectQuoteContainer ugc baseHtml").text).strip()).replace('Click to expand...', '').lower()

        if 'hint' in message_nq or 'target' in message_nq:
            for blist in biffers_list:  # only count the post as a hint if it's posted by a BIF participant
                if user_id in blist:
                    hint = 1

        # write the post's html to the db
        write_post_raw(post_id, name, remove_newlines(p))

        write_post(post_id, username, message, timestamp, gifs, pic_counter, media_counter, post_num, page_number,
                   name, post_url, user_id, hint)

        # iterate through user data for everyone who posted
        fields = ud.find_all('dd')  # returns a tuple: [text-based date, location]
        # some users hide their location
        if len(fields) == 1:
            joindate = str(to_date(fields[0].text))[:10]
            location = None
        elif len(fields) == 2:
            joindate = str(to_date(fields[0].text))[:10]
            location = fields[1].text
        else:
            joindate = None
            location = None

        # create a dictionary for this user
        user = {
            'id': user_id,
            'username': username,
            'joindate': joindate,
            'location': location,
            }
        # add unique users to the list
        if user not in ulist:
            ulist.append(user)

    # commit after each page_number
    commit(db)
    page_counter += 1
    # pause()

# write user data to the db
write_users(ulist)
commit(db)

# update the single-page_number html file
update_file(name)
if ongoing:
    update_likes(name)

# update user data
if not login_status:
    login_status = login_to_talkbeer(http_session, Config.tb_credentials)

# run_raffle(1, name)
commit(db)
commit(dbsql)
close_db(db)
close_db(dbsql)
http_session.close()

time_end = datetime.datetime.now()

print("")
print("***** ***** **** ***** *****")
print("  End:", time_end.strftime("%Y-%m-%d %H:%M:%S"), "[normal path]")
print("  Total time: {} seconds\n\n".format(round((time_end - time_start).total_seconds(), 2)))
print("***** ***** **** ***** *****")
print("")

logger.info("END via the normal path @ {}".format(time_end.strftime("%Y-%m-%d %H:%M:%S")))
logger.info("Total time: {} seconds\n\n".format(round((time_end - time_start).total_seconds(), 2)))

quit()
