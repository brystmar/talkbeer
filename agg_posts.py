##################################################################################
# Parses posts from each thread page, then combines all posts onto a single page #
##################################################################################

from bs4 import BeautifulSoup
import datetime
import dateutil.parser
import logging as logging_util
import os
import re
import requests
from sqlalchemy import create_engine, event, engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.sql import text
# import sqlite3
from models import Biffers, Likes, Posts, Region_Map, Threads, Users
from models import Errors, Posts_Soup, Thread_Page, URLs, Output_Options
from dotenv import load_dotenv

# initialize logging
logfile = 'logs/system_log.log'
logging_util.basicConfig(filename=logfile, filemode='w', level=logging_util.DEBUG, datefmt='%H:%M:%S',
                         format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging_util.getLogger(__name__)

time_start = datetime.datetime.now()
time_start = time_start.isoformat(timespec='seconds')
time_start = str(time_start).replace("T", " ")
print("\n***** ***** **** ***** *****")
print(" Start:", time_start)
print("***** ***** **** ***** *****\n")
logger.info('\n\n***** ***** **** ***** ***** |||| ***** ***** **** ***** *****\n')
logger.info('START agg_posts.py @ {}'.format(time_start))


def pause(): input("[====]")


def make_soup(html_to_soupify): return BeautifulSoup(html_to_soupify, 'html.parser')


def find_substring(text, char):
    return [i for i, letter in enumerate(text) if letter == char]


def commit(db):
    pp("Pause before commit...")
    logger.debug("Attempting to commit db {}".format(str(db)))
    # db.commit()
    logger.debug("Commit successful for db {}".format(str(db)))
    return 0


def pp(item):
    print("")
    print(item)
    logger.debug("\n" + item)
    pause()


def pl(items):
    for i in items:
        print(i)
        logger.debug(i)
    print("Total items: {}".format(len(items)))
    logger.debug("Total items: {}".format(len(items)))
    pause()


def pl_sorted(items):
    newlist = list()
    ditems = set(items)
    for d in ditems:
        newlist.append(d)

    newlist.sort()

    for n in newlist:
        print(items.count(n), n)
        logger.debug("{count} {n}".format(count=items.count(n), n=n))
    pause()


def pd(items):
    for item in items:
        print("{item}: {value}".format(item, items[item]))
        logger.debug("{item}: {value}".format(item, items[item]))
    pause()
    print("")


def qmarks(num):
    # returns a string of question marks in the length requested, ex: (?,?,?,?)
    i = 0
    q = "("
    while i < num:
        q += "?,"
        i += 1
    return q[:-1] + ")"


def stop(text):
    global name, ongoing

    logger.info('Entered the stop() function because: {}'.format(text))
    print(text)
    update_file(name)
    if ongoing:
        update_likes(name)
    commit(conn_tbdb)
    # run_raffle(1, name)

    try:
        db.close()
    except:
        logger.error('Error attempting to close the db', exc_info=True)

    time_end = datetime.datetime.now()
    time_end = time_end.isoformat(timespec='seconds')
    time_end = str(time_end).replace("T", " ")

    logger.info('END via the stop() function @ {}\n\n'.format(time_end))
    print("\n***** ***** **** ***** *****")
    print("  End:", time_end, "[stop function]")
    print("***** ***** **** ***** *****\n")

    quit()


def return_list_of_values(mylist):
    # returns a list of strings instead of a list of tuples
    logger.debug("Starting return_list_of_values() with: {}".format(mylist))
    if mylist is None or len(mylist) == 0:
        logger.debug("mylist is None, or len(mylist)==0")
        logger.debug("End return_list_of_values()")
        return None

    list_to_return = [m for m, in mylist]
    logger.debug("Ending return_list_of_values() with: {}".format(list_to_return))
    return list_to_return


def return_first_value(input):
    # returns the first value in the set if input is a list or tuple, otherwise it returns the input untouched
    logger.debug('Starting return_first_value() with: {}'.format(input))
    if input is None:
        logger.debug('Ending return_first_value() unchanged')
        return input
    try:
        # return_list_of_values(input)
        logger.debug('Ending return_first_value() with: {}'.format(input[0][:20]))
        return input[0]
    except TypeError:
        logger.error('Error:', exc_info=True)
        logger.debug('Ending return_first_value() unchanged')
        return input


def remove_newlines(text):
    text = str(text)

    br2 = '''<br/>
    <br/>'''
    br3 = '''<br/>
    <br/>
    <br/>'''

    br2nl = '''<br/>\n<br/>'''
    br3nl = '''<br/>\n<br/>\n<br/>'''

    while '\n\n\n' in text:
        text = text.replace('\n\n\n', '\n\n')
    while br3 in text:
        text = text.replace(br3, br2)
    while br3nl in text:
        text = text.replace(br3nl, br2nl)
    return text


def find_last_post(html):
    lp_soup = BeautifulSoup(html, 'html.parser')
    # focus on the specific div
    posts = lp_soup('div', class_="publicControls")
    # carve out info about the last post on the page
    lpinfo = posts[len(posts)-1].find('a', title='Permalink')
    # user-facing number for the last post
    last_post = int(lpinfo.text.replace('#','').strip())
    # system id for the last post
    last_post_id = int(re.findall('#post-(\d+)', lpinfo.get('href'))[0])

    # page number
    page = str(lp_soup.find('link', rel="canonical"))
    if page is not None:
        page = re.findall('href=\"(.+?)\"', page)[0]
        if page[-1] == '/':
            page = 1
        elif re.search('/page-\d+', page):
            page = int(re.findall('/page-(\d+)', page)[0])
        else:
            page = None

    return [last_post, last_post_id, page]


def month_to_num(m):
    if m == 'Jan':
        return '01'
    elif m == 'Feb':
        return '02'
    elif m == 'Mar':
        return '03'
    elif m == 'Apr':
        return '04'
    elif m == 'May':
        return '05'
    elif m == 'Jun':
        return '06'
    elif m == 'Jul':
        return '07'
    elif m == 'Aug':
        return '08'
    elif m == 'Sep':
        return '09'
    elif m == 'Oct':
        return '10'
    elif m == 'Nov':
        return '11'
    elif m == 'Dec':
        return '12'
    else:
        return 'Month Error'


def to_timestamp(ds):
    # converts a text date/time sting to ISO-8601 formatting: 'Mar 26, 2018 at 9:48 PM' --> '2018-03-26 21:48:00'
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

    return datetime.datetime(year, month, day, hour, minute)


def to_date(ds):
    # converts a text date string to ISO-8601 formatting: 'Mar 26, 2018' --> '2018-03-26'
    year = int(re.findall(', (20\d\d)', ds)[0])
    month = int(month_to_num(ds[:3]))
    day = int(re.findall(' (\d+),', ds)[0])
    return datetime.datetime(year, month, day)


def replace_goto(p):
    i = 0
    # print('entering goto...')
    quotes = p.find_all('div', class_="attribution type")
    p = str(p)
    while i < len(quotes):
        # grab the anchor's post id
        if 'class="AttributionLink"' in quotes[i]:
            qid = re.findall('href=\".+?#post-(\d+)\"', str(quotes[i].find('a', class_="AttributionLink")))[0]
        else:
            # when a post gets deleted, it has no AttLink
            i += 1
            continue
        # save the original before we make any edits
        old_quote = str(quotes[i])
        new_quote = quotes[i]
        # print("OLD quote:", "\n", old_quote)
        # print("")
        # delete the existing hyperlink
        goto_link = str(new_quote.find('a', class_="AttributionLink"))
        goto_id = re.findall('href=\".+?#post-(\d+?)\"', goto_link)[0]
        new_link = ' <a href="#' + goto_id + '" class="AttributionLink">&uarr;</a>'
        new_quote = str(new_quote).replace(goto_link, new_link)
        new_quote = new_quote.replace('\n', '')

        # add the new hyperlink + js
        # new_quote = new_quote.replace('</div>', "\n<a id='post" +qid+ "' class='AttributionLink'></a>" +js+ "</div>")

        # print("\n", "NEW quote:", "\n", new_quote)
        # pp("*=*=*=*=*=*=*=*=*=*=*=*=*")
        # print(str(old_quote) in p)
        # print(str(new_quote) in p)
        p = p.replace(str(old_quote), str(new_quote))
        # print(str(old_quote) in p)
        # pp(str(new_quote) in p)
        i += 1
    # print("#########################")
    # print(p)
    # pp('exiting goto...')
    return p


def remove_ols(p):
    return str(p).replace('<ol>', '<ul>').replace('</ol>', '</ul>')


def write_post_raw(post_id, thread_name, p):
    # validation
    dbsql.execute('SELECT distinct id, thread_name, soup FROM raw.posts_soup WHERE id = ?', (post_id,))
    val = tbdb.fetchone()

    if val is None:
        # new post
        dbsql.execute('INSERT INTO raw.posts_soup (id, thread_name, soup) VALUES ' + qmarks(3),
                     (post_id, thread_name, p))
        logger.debug('Wrote new post_soup id={id} for {thread}'.format(id=post_id, thread=thread_name))
    elif None in val:
        # overwrite incomplete data
        dbsql.execute('UPDATE raw.posts_soup SET thread_name = ?, soup = ? WHERE id = ?',
                     (thread_name, p, post_id))
        logger.debug('Updated post_soup id={id} for {thread}'.format(id=post_id, thread=thread_name))
    else:
        return 0


def write_post(p, post_id, username, message, timestamp, gifs, pics, other_media, num, thread_page, thread_name, url, user_id, hint):
    # validation
    dbsql.execute('SELECT * FROM public.posts WHERE id = ?', (post_id,))
    val = tbdb.fetchone()

    if val is None:
        dbsql.execute('''INSERT INTO public.posts (id, username, text, timestamp, gifs, pics, other_media, num,
                                            thread_page, thread_name, url, user_id, hint)
                        VALUES ''' + qmarks(13), (post_id, username, message, timestamp, gifs, pics, other_media,
                                                  num, thread_page, thread_name, url, user_id, hint))
        logger.debug("Inserted new post {id} for {thread}".format(id=post_id, thread=thread_name))
    else:
        dbsql.execute('''UPDATE public.posts SET username = ?, text = ?, timestamp = ?, gifs = ?, pics = ?,
                            other_media = ?, num = ?, thread_page = ?, thread_name = ?, url = ?, user_id = ?, hint = ?
                        WHERE id = ?''', (username, message, timestamp, gifs, pics, other_media, num,
                                          thread_page, thread_name, url, user_id, post_id, hint))
        print("Updated post {id} for {thread}".format(id=post_id, thread=thread_name))
        logger.debug("Updated post {id} for {thread}".format(id=post_id, thread=thread_name))


def write_users(ulist):
    # ensure the input is properly formatted as a list of dictionaries
    if not isinstance(ulist, list):  # ulist is a dictionary with attributes: id, username, location, joindate
        ulist = [ulist]

    for u in ulist:
        # validation
        dbsql.execute('SELECT distinct id, username, joindate FROM public.users WHERE id = ?', (u['id'],))
        val = tbdb.fetchone()

        if val is None:
            dbsql.execute('INSERT INTO public.users (id, username, location, joindate) VALUES ' + qmarks(4),
                         (u['id'], u['username'], u['location'], u['joindate']))
            print('Inserted user {user}, id: {id}'.format(user=u['username'], id=u['id']))
            logger.debug('Inserted user {user}, id: {id}'.format(user=u['username'], id=u['id']))
        elif None in val:
            if u['location'] is not None:
                dbsql.execute('UPDATE public.users SET username = ?, location = ?, joindate = ? WHERE id = ?',
                             (u['username'], u['location'], u['joindate'], u['id']))
            else:
                dbsql.execute('UPDATE public.users SET username = ?, joindate = ? WHERE id = ?',
                             (u['username'], u['joindate'], u['id']))
            logger.debug('Updated user {user}, id: {id}'.format(user=u['username'], id=u['id']))


def get_userdata(uid):
    # read & soupify the html for this user's page
    url = 'https://www.talkbeer.com/community/members/{}'.format(uid)
    html = s.get(url).text
    soup = make_soup(html)

    username = soup.find('h1', class_="username").text
    user_id = int(re.findall('/community/members/\S+?\.(\d+?)/', str(soup.find('link', rel="canonical")))[0])

    # there's no easy way to find the join date
    joindate = None
    jointext = soup.find('div', class_="section infoBlock").find_all('dd')

    for j in jointext:
        if ', 20' in j.text and ':' not in j.text:
            joindate = str(to_date(j.text))[:10]
            break

    # users have the option of sharing their location
    try:
        location = soup.find('a', itemprop="address").text
    except:
        location = None

    # return user data as a dictionary
    return {'id': user_id, 'username': username, 'joindate': joindate, 'location': location}


def add_post_details(panel, postinfo):
    # adds new items in the detail panel to the left of each post
    #  postinfo must be a list of tuples: (label,value)
    for pi in postinfo:
        repl = '<dl class="pairsJustified"><dt>{pi0}:</dt><dd>{pi1}</dd></dl>\n</div>'.format(pi0=pi[0], pi1=pi[1])
        panel = panel.replace('</div>', repl)
    return panel


def elkhunter(data, name, postinfo):
    # validation
    if data is None:
        stop("No data returned")
    else:
        print("\nWriting {num} posts to: {name} {info}.txt".format(num=len(data), name=name, info=postinfo))
        logger.debug("Writing {num} posts to: {name} {info}.txt".format(num=len(data), name=name, info=postinfo))

    guesses = []

    for d in data:
        # r.id, r.soup, p.username, p.timestamp, p.page_number
        soup = make_soup(d[1])

        # remove quoted posts
        while soup.find('div', class_="bbCodeBlock bbCodeQuote") is not None:
            soup.find('div', class_="bbCodeBlock bbCodeQuote").decompose()

        text = soup.blockquote.get_text()
        words = text.split()

        # find the number they guessed
        i = 1
        for w in words:
            try:
                num = int(w)
                if num > 1000 or num < 1:
                    continue
                guesses.append([num, d[2], d[3], d[0], d[4], '#' + str(i)])
                i += 1
            except:
                continue
    return guesses


def run_raffle(num_winners, name):
    logger.debug('About to run the raffle, seeking {num} winners in {name}'.format(num=num_winners, name=name))
    noun = "winner" if num_winners == 1 else "winners"

    from rng import return_random_nums
    query = '''SELECT distinct p.username, p.id, p.timestamp
                FROM public.posts p
                JOIN public.biffers b ON p.user_id = b.user_id AND p.thread_name = b.thread_name
                WHERE b.thread_name = ? ORDER BY p.id'''
    dbsql.execute(query, (name,))
    raffle = tbdb.fetchall()
    winners = return_random_nums(num_winners, 0, len(raffle)-1)

    num_posts = []
    for r in raffle:
        num_posts.append(r[0])  # new list for counting how many posts each user submitted
    print("Raffle {noun} from the {qty} entries:".format(noun=noun, qty=len(raffle)))
    logger.debug("Raffle {noun} from the {qty} entries:".format(noun=noun, qty=len(raffle)))
    for w in winners:
        print("{winner} -- {np} user posts.".format(winner=raffle[w], np=num_posts.count(raffle[w][0])))
        logger.debug("{winner} -- {np} user posts.".format(winner=raffle[w], np=num_posts.count(raffle[w][0])))

    logger.debug('Raffle complete')


def update_file(name):
    logger.debug('Entering update_file()')

    # prompt user for which posts to include and the display order
    options = {0: '[Skip]', 1: 'All posts in order', 2: 'Hauls only (known)', 3: 'Hauls only (derived)',
               4: 'Possible senders to brystmar', 5: 'BYO SQL'}
    output_options = db.query(Output_Options)
    print("\nContent options for the {} html file:".format(name))
    for o in output_options:
        print('{id}: {val}'.format(id=o.id, val=o.option))

    print("")
    valid_options = return_list_of_values(db.query(Output_Options.id).all())
    val = False
    while val is False:
        user_option = input("Option: ")
        try:
            user_option = int(user_option)
            if user_option in valid_options:
                val = True
            else:
                logger.debug('User entry {u} is an invalid selection from {opt}'.format(u=user_option,
                                                                                        opt=valid_options))
        except ValueError:
            logger.error('Cannot convert str({}) to int'.format(user_option), exc_info=True)
            continue

    logger.debug('User selected option {opt}: {val}'.format(opt=user_option, val=output_options[user_option].option))

    # get the first page of the thread
    query = text('SELECT html FROM raw.thread_page WHERE name = :a and page = 0')
    result = dbsql.execute(query, a=name)
    html = return_first_value(result.fetchone())

    # find the max page number we've recorded
    query = text('SELECT max(page) FROM raw.thread_page WHERE name = :a')
    result = dbsql.execute(query, a=name)
    maxpage = return_first_value(result.fetchone())

    # remove ads flags
    if "enable_page_level_ads: true" in html:
        html = html.replace("enable_page_level_ads: true", "enable_page_level_ads: false")
    if "adsbygoogle" in html:
        html = html.replace("adsbygoogle", "ads123byN0Pty")

    # erase the header & quickReply sections
    soup = make_soup(html)
    current_page = int(re.findall('Page (\d+) of \d+', soup.find('span', class_="pageNavHeader").text)[0])
    current_maxpage = int(re.findall('Page \d+ of (\d+)', soup.find('span', class_="pageNavHeader").text)[0])
    if soup.find('a', title="Open quick navigation") is not None: soup.find('a', title="Open quick navigation").decompose()
    if soup.find('header') is not None: soup.find('header').decompose()
    if soup.find('div', class_="quickReply message") is not None: soup.find('div', class_="quickReply message").decompose()
    if soup.find('div', id="loginBar") is not None: soup.find('div', id="loginBar").decompose()

    # find current max page, replace with the new max page
    html = str(soup)  # soup re-arranges some of the tag attributes, which wouldn't work for replace() below
    html = html.replace('data-last="' + str(current_maxpage) + '"', 'data-last="' + str(maxpage) + '"')
    html = html.replace('Page ' + str(current_page) + ' of ' + str(current_maxpage), 'Page ' + str(current_page) + ' of ' + str(maxpage))
    html = html.replace('page-'  + str(current_maxpage), 'page-' + str(maxpage))
    html = html.replace('>' + str(current_maxpage) + '<', '>' + str(maxpage) + '<')
    html = html.replace('<div id="headerMover">', '<br/>')

    # replace everything after the </ol> with a basic footer
    html = html[:html.index('</ol>') + 5] + '\n<hr>\n\n</form>\n<i>fin</i>\n</body>\n</html>'

    # 'option' determines which SQL query to use
    if user_option == 4:  # only hint-related posts by users not ruled out as brystmar's sender, ordered by username
        if name not in ['SSF14', 'SSF15', 'SSF16', 'SSF17', 'Fest18']:
            stop("Possible sender data for brystmar only exists for SSF14+")

        dbsql.execute('''SELECT DISTINCT r.id, r.soup, p.username, p.timestamp, p.thread_page
                    FROM raw.posts_soup r
                    JOIN public.posts p ON r.id = p.id
                    JOIN public.biffers b ON p.user_id = b.user_id AND p.thread_name = b.thread_name
                    WHERE p.thread_name = ? AND r.soup is not null
                        AND b.my_sender is null AND p.hint = 1 AND p.user_id <> 456
                    ORDER BY p.username, r.id''', (name, ))
    elif user_option == 1:  # all posts, in sequential order
        dbsql.execute('''SELECT r.id, r.soup, p.username, p.timestamp, p.thread_page
                    FROM raw.posts_soup r
                    JOIN public.posts p ON r.id = p.id
                    WHERE p.thread_name = ? AND r.soup is not null
                    ORDER BY r.id''', (name, ))
    elif user_option == 2:  # known hauls only, in sequential order
        dbsql.execute('''SELECT r.id, r.soup, p.username, p.timestamp, p.thread_page
                    FROM raw.posts_soup r
                    JOIN public.posts p ON r.id = p.id
                    JOIN public.biffers b ON p.id = b.haul_id AND p.thread_name = b.thread_name
                    WHERE p.thread_name = ? AND r.soup is not null
                    ORDER BY r.id''', (name, ))
    elif user_option == 3:  # derived hauls only, in sequential order (2+ pics or 2+ non-quoted instagram posts)
        dbsql.execute('''SELECT r.id, r.soup, p.username, p.timestamp, p.thread_page
                    FROM raw.posts_soup r
                    JOIN public.posts p ON r.id = p.id
                    WHERE p.thread_name = ? AND r.soup is not null
                        AND (p.pics >= 2) OR p.text like '%\n[instagram]%\n[instagram]%'
                    ORDER BY r.id''', (name, ))
    # elif option == 5: #elkhunter LIF
        # dbsql.execute("""SELECT r.id, r.soup, p.username, p.timestamp, p.thread_page
    #                       FROM raw.posts_soup r
    #                       JOIN public.posts p ON r.id = p.id
    #                       WHERE p.thread_name = ?
    #                           AND lower(p.text) like '%#elkhunterlif%'
    #                           AND r.id > 1921012
    #                           ORDER BY r.id""", (name, ))
        # html = elkhunter(tbdb.fetchall(), name, options[option])
    elif user_option == 5:  # BYO SQL
        last_query = """SELECT r.id, r.soup, p.username, p.timestamp, p.thread_page
                    FROM raw.posts_soup r
                    JOIN public.posts p ON r.id = p.id
                    WHERE p.thread_name = 'Fest18'
                        AND r.id > 1929291
                        AND p.user_id in(SELECT user_id FROM public.biffers WHERE thread_name = 'Fest18' AND haul_id is null)
                    ORDER BY p.user_id, r.id"""
        print("Last query entered: {}\n".format(last_query))
        query = input("Enter SQL to execute: ")
        logger.debug('Running user-entered query:\n{}'.format(query))
        dbsql.execute(query)

        # Fest18 haul posts:
        #   """SELECT r.id, r.soup, p.username, p.timestamp, p.thread_page
        #   FROM raw.posts_soup r
        #   JOIN posts p ON r.id = p.id
        #   WHERE p.thread_name = 'Fest18'
        #       AND r.id > 1929291
        #       AND (p.pics > 0 OR p.other_media > 0)
        #   ORDER BY r.id"""
        # Fest18 haul posts2:
        #   """SELECT r.id, r.soup, p.username, p.timestamp, p.thread_page
        #   FROM raw.posts_soup r
        #   JOIN posts p ON r.id = p.id
        #   WHERE p.thread_name = 'Fest18'
        #       AND r.id > 1929291
        #       AND (p.pics > 0 OR p.other_media > 0)
        #       AND r.id not in(SELECT distinct haul_id FROM public.biffers)
        #   ORDER BY r.id"""

    elif user_option == 0:
        return 0

    data = tbdb.fetchall()
    # validation
    if data is None:
        logger.debug('No data returned from update_file query {}'.format(user_option))
        stop("No data returned")
    print("\nWriting {qty} posts to: {name} {opt}.html".format(qty=len(data), name=name, opt=options[user_option]))
    logger.debug("Writing {qty} posts to: {name} {opt}.html".format(qty=len(data), name=name, opt=options[user_option]))

    # grab the post_id URL
    dbsql.execute('SELECT post FROM public.urls limit 1')
    post_url = return_first_value(tbdb.fetchone())

    # insert posts into the html shell
    remove = []
    postinfo = []
    count = 0
    for d in data:
        remove.clear()
        postinfo.clear()
        post_id = str(d[0])
        page_number = str(d[4])

        postinfo.append(("Post ID", post_id))
        postinfo.append(("Page", '<a class="page_number" target="_blank" href="' + post_url + post_id + '">' + page_number + '</a>'))

        sp = make_soup(remove_ols(d[1]))
        raw = str(sp)

        likes_id = 'likes-post-' + str(sp.a['name'])
        remove.append(str(sp.find('div', id=likes_id)))
        remove.append(str(sp.find('h3', class_="userTitle userText")))
        remove.append(str(sp.find('div', class_="avatarHolder")))
        # remove.append(str(sp.find('div', class_="extrauserpanelinfo"))) #location & join date
        remove.append(str(sp.find('div', class_="messageMeta ToggleTriggerAnchor")))  # user/timestamp summary
        remove.append(str(sp.find('strong', class_='newIndicator')))  # new post indicators

        # adjust stuff in the user info section
        userpanelinfo = sp.find('div', class_="extraUserInfo")
        # get the hyperlink for the user's location
        llink = userpanelinfo.find('a')
        # add stuff to the user detail panel
        userpanelinfo = str(userpanelinfo)
        if d[3].count(':') == 2 and d[3][len(d[3])-3:] == ':00':
            postinfo.append(("Date", d[3][:-3]))
        else:
            postinfo.append(("Date", d[3]))

        raw = raw.replace(userpanelinfo, add_post_details(userpanelinfo, postinfo))

        # remove the location hyperlink (if the user shows their location)
        if llink is not None:
            loc = llink.text
            if len(loc) > 18:  # truncate the location string if it's too long
                spaces = list(reversed(find_substring(loc, ' ')))
                for s in spaces:
                    if s <= 18:
                        break
                loc = loc[:min(s, 18)] + '...'
            raw = raw.replace(str(llink), loc)

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

    """if option == 5: #write results to a csv
        with open(name + ' ' + options[option] + '.txt', 'w') as file:
            for guess in html:
                i = 1
                for g in guess:
                    if i == len(guess):
                        file.write(str(g))
                    else:
                        file.write(str(g) + ',')
                    i += 1
                file.write('\n')
        else:"""
    # add summary data to the footer
    footer_html = '\n<footer>\n<div class="footerLegal">\n<div class="pageWidth">\n<div class="pageContent">\n'
    footer_html += '<div id="copyright">A mediocre concatenation of {} posts by brystmar</div>\n'.format(count)
    footer_html += '</div>\n</div>\n</div>\n</footer>\n</html>'
    html = html.replace('</html>', footer_html)

    # write the output
    file_path = 'html-output/{subdir}/'.format(subdir=name)
    file_name = '{name} {opt}.html'.format(name=name, opt=options[user_option])
    with open(file_path + file_name, 'w') as file:
        logger.debug('Begin writing html file: {}'.format(file_path + file_name))
        file.write(html)
        logger.debug('Done writing html')

    print('Done.\n')


def update_likes(name):
    logger.debug('Entering update_likes() function')

    # prompt to update
    if 'ssf' in name.lower() or 'fest' in name.lower():
        likes_input = input("Update likes? ")
    else:
        logger.debug("Thread {} doesn't contain SSF or Fest, so ineligible for updating likes".format(name))
        return 0

    if likes_input.lower() not in ['y', 'yes', '1']:
        logger.debug("User declined to update likes")
        return 0
    else:
        logger.debug("Updating likes for {}".format(name))

    # must be logged in to view likes
    global login_status
    if login_status is False:
        logger.debug("beetsbot not logged into talkbeer.com")

        # log in using the provided credentials
        global url_login, creds
        s.post(url_login, data=creds)
        login_status = True
        logger.debug("beetsbot successfully logged into talkbeer.com")

    # get post_id & timestamp for the most recently-recorded 'liked' post
    query = 'SELECT post_id, max(timestamp) FROM public.likes WHERE post_id = (SELECT max(l.post_id) as maxpost '
    query += 'FROM public.likes l JOIN public.posts p on l.post_id = p.id WHERE p.thread_name = ?) GROUP BY post_id;'
    dbsql.execute(query, (name,))
    last_like = return_first_value(tbdb.fetchall())

    if len(last_like) == 0:
        # no liked posts yet
        recent_post_text = "No likes found for {}.".format(name)
    else:
        # how many days ago was that 'like' timestamp?
        time_ago = datetime.datetime.now() - dateutil.parser.parse(last_like[1])
        days_ago = round(time_ago.days + round(time_ago.seconds/(24*60*60), 2), 2)

        recent_post_text = "Most recently-liked {} post was {} days ".format(name, days_ago)
        recent_post_text += "ago: id={} at {}.".format(last_like[0], last_like[1])

    print(recent_post_text + "\n")
    logger.debug(recent_post_text)
    starting_post = input("Enter post_id, timestamp, or # days to retrieve posts from: ")

    # figure out what the user entered, then submit the right query
    try:
        starting_post = int(starting_post)
        logger.debug("User entry for likes: {}".format(starting_post))
        if starting_post > 180:
            # user entered a post_id
            logger.debug("Detected a post_id")
            query = """SELECT DISTINCT id, timestamp
                        FROM public.posts
                        WHERE id >= ?
                            AND thread_name = ?
                        ORDER BY id
                        LIMIT 1500"""
            dbsql.execute(query, (starting_post, name))
        else:
            # user entered a number of days
            logger.debug("Detected a number of days")
            cutoff_date = str(datetime.datetime.now() + datetime.timedelta(-starting_post))[:10]
            query = """SELECT DISTINCT id, timestamp
                        FROM public.posts
                        WHERE timestamp >= ?
                            AND thread_name = ?
                        ORDER BY id LIMIT 1500"""
            dbsql.execute(query, (cutoff_date, name))
    except:
        logger.error("Error attempting to convert user entry {} to int".format(starting_post), exc_info=True)
        if '-' in starting_post:
            # user entered a timestamp
            logger.debug("Detected a timestamp")
            query = """SELECT DISTINCT id, timestamp
                        FROM public.posts
                        WHERE timestamp >= ?
                            AND thread_name = ?
                        ORDER BY id
                        LIMIT 1500"""
            dbsql.execute(query, (starting_post, name))
        else:
            logger.debug("Unable to detect the type of user entry")
            return 0

    # load post_ids into memory
    post_ids = tbdb.fetchall()

    # update likes
    i = 0
    if post_ids is not None:
        logger.debug("Begin updating likes, total: {}".format(len(post_ids)))
        for pid in post_ids:
            read_likes(pid[0])
            i += 1
            if i % 10 == 0:
                commit(conn_tbdb)
                print("{i} of {qty} post_id={pid} COMMIT".format(i=i, qty=len(post_ids), pid=pid[0]))
                logger.debug("{i} of {qty} post_id={pid} COMMIT".format(i=i, qty=len(post_ids), pid=pid[0]))
            else:
                print("{i} of {qty} post_id={pid}".format(i=i, qty=len(post_ids), pid=pid[0]))
                logger.debug("{i} of {qty} post_id={pid}".format(i=i, qty=len(post_ids), pid=pid[0]))
    logger.debug("End updating likes")
    commit(conn_tbdb)

    # if any unknown users were added to the 'likes' table, add them to the 'users' table too
    query = '''SELECT distinct user_id FROM public.likes WHERE user_id not in
                    (SELECT distinct id FROM public.users)
                ORDER BY user_id'''
    dbsql.execute(query)
    users = tbdb.fetchall()
    if users is None or len(users) < 1:
        logger.debug("No users to update")
        return 0
    else:
        # add missing users to the 'users' table
        logger.debug("Add {} missing users".format(len(users)))
        for u in users:
            write_users(get_userdata(u[0]))


def read_likes(post_id):
    logger.debug("Start of read_likes()")
    global s
    url = 'https://www.talkbeer.com/community/posts/{pid}/likes'.format(post_id)
    html = s.get(url).text
    soup = make_soup(html)
    # soup = make_soup(s.get(url).text)
    likes = soup.find_all('li', class_="primaryContent memberListItem")

    for li in likes:
        time_blob = str(li.find('div', class_="extra"))
        timestamp = to_timestamp(re.findall('.+(\w\w\w \d+, 20\d\d at \d+:\d+ [APap][Mm])', time_blob)[0])
        user_id = int(re.findall('href=\"members/.*\.(\d+?)/\"', str(li))[0])
        # print(timestamp, " <--> ", user_id)
        write_likes(post_id, user_id, timestamp)

    logger.debug("End of read_likes()")


def write_likes(post_id, user_id, timestamp):
    logger.debug("Start of write_likes()")
    # validation
    dbsql.execute('SELECT post_id, user_id, timestamp FROM public.likes WHERE post_id = ? and user_id = ?',
                 (post_id, user_id))
    val = tbdb.fetchall()

    if val is None:
        dbsql.execute('INSERT INTO public.likes (post_id, user_id, timestamp) VALUES ' + qmarks(3),
                     (post_id, user_id, timestamp))
    else:
        dbsql.execute('DELETE FROM public.likes WHERE post_id = ? and user_id = ?', (post_id, user_id))
        dbsql.execute('INSERT INTO public.likes (post_id, user_id, timestamp) VALUES ' + qmarks(3),
                     (post_id, user_id, timestamp))

    logger.debug("End of write_likes()")


def determine_thread():  # returns the thread name and the highest page number that's been parsed
    # if there's only one thread with new posts scraped, pick that one by default
    logger.debug('Start of determine_thread()')

    query = text('''WITH tp_maxpost as
                        (SELECT name, max(last_post_id) as last_post FROM raw.thread_page GROUP BY 1 ORDER BY 1),
                    p_maxpost as
                        (SELECT thread_name as name, max(id) as last_post FROM public.posts GROUP BY 1 ORDER BY 1)
                    
                    SELECT distinct tp.name
                    FROM tp_maxpost tp
                    JOIN p_maxpost p ON tp.name = p.name
                    WHERE tp.last_post > p.last_post
                    ORDER BY 1''')
    result = dbsql.execute(query)
    to_agg = return_list_of_values(result.fetchall())
    logger.debug('to_agg={}'.format(to_agg))

    # if there's only one thread name returned, use that thread name and don't prompt the user
    global name, page, ongoing
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
        while name not in thread_names:
            name = input("Enter thread to parse: ")
            if name is None or name == '':
                db.close()
                print("")
                logger.debug("User didn't enter a thread name.  Quitting.")
                quit()
            # be case insensitive for user entry
            for tn in thread_names:
                if tn.lower() == name.lower():
                    name = tn
    elif len(to_agg) == 1:
        name = to_agg[0]
        print("Parsing thread: {}\n".format(name))
        logger.debug("Parsing thread: {}\n".format(name))
    else:
        stop("Error finding the thread to scrape.")

    logger.info("User wants to parse data from {}".format(name))

    # compare the last post from scraped thread data to the last post in the posts table
    query = text('SELECT max(last_post_id) FROM raw.thread_page WHERE name = :a')
    result = dbsql.execute(query, a=name)
    lp_threads = return_first_value(result.fetchone())

    query = text('SELECT max(id) FROM public.posts WHERE thread_name = :a')
    result = dbsql.execute(query, a=name)
    lp_posts = return_first_value(result.fetchone())

    # is this thread ongoing?
    query = text('SELECT distinct name FROM public.threads WHERE name = :a and ongoing = :b')
    result = dbsql.execute(query, a=name, b='Y')
    ongoing = return_first_value(result.fetchone())

    if ongoing == name:
        ongoing = True
    else:
        ongoing = False

    # load the pertinent thread data into the 'data' variable
    if lp_threads is None:
        stop("No thread data for {}".format(name))
    elif lp_threads == lp_posts:
        stop("No new posts found for {}".format(name))
    elif lp_posts is None:
        page = 1  # no post data recorded for this thread yet
    elif lp_threads > lp_posts:
        # found new posts that we need to parse
        # find the page containing the first post that's not in the db
        result = db.execute(text('SELECT distinct thread_page, num FROM public.posts WHERE id = :a', a=lp_posts))
        check_posts_on_page = result.fetchone()
        # if this is the 20th post on the page, skip to the next page
        if check_posts_on_page[1] % 20 == 0:
            page = check_posts_on_page[0] + 1
        else:
            page = check_posts_on_page[0]
    else:
        stop("More posts exist in the posts table than raw thread html.  Re-scrape the thread for {}".format(name))

    logger.debug('End of determine_thread()')


def thumbfix(p):
    # adjusts tb-native thumbnails to use the same schema as tb-native full images
    top_class = "messageText SelectQuoteContainer ugc baseHtml"
    thumbs = p.find('blockquote', class_=top_class).find_all('a', class_="LbTrigger")
    if thumbs is None or isinstance(thumbs, list):
        return p

    pp(p)
    print("")

    i = 0
    p = str(p)
    while i < len(thumbs):
        # ex: https://www.talkbeer.com/community/attachments/994f94d8-9eba-4450-bff0-d83a3e65c3f7-jpeg.458/
        a_href = str(thumbs[i].get('href'))
        pic_id = re.findall('.*community\/attachments\/.*\.(\d+)\/', a_href)[0]

        img_tag = thumbs[i].find('img')
        thumb_src = str(img_tag.get('src'))
        new_src = 'https://www.talkbeer.com/community/attachments/{id}/'.format(id=pic_id)
        new_img = str(img_tag).replace(thumb_src, new_src)

        p = p.replace(str(thumbs[i]), new_img)
        i += 1

    return make_soup(p)


def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    # logger.debug("Received statement: %s", statement)
    pass


# open & initialize the http session
s = requests.session()
# talkbeer's forum software doesn't allow scraping, so mask the request as a different user agent
s.headers.update({'User-Agent': 'Mozilla/65.0.1'})
s.verify = False  # disable SSL verification
from urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)  # suppress SSL warning messages

# set the talkbeets account credentials in case we need to log in later
from tbcred import url_login, user_sys, pw_sys
creds = {'login': user_sys, 'password': pw_sys}
login_status = False

# open & initialize the db
# conn_tbdb = sqlite3.connect('talkbeer.sqlite')

# set path for loading local .env variables
basedir = os.path.abspath(os.path.dirname(__file__))
load_dotenv(os.path.join(basedir, '.env'))

db_user = os.environ.get('DB_USER')
db_pw = os.environ.get('DB_PW')
db_name = os.environ.get('DB_NAME')
db_instance = os.environ.get('DB_INSTANCE')

cloud_server = 'postgresql+psycopg2://{user}:{pw}@/{db_name}'.format(user=db_user, pw=db_pw, db_name=db_name)
cloud_server += '?host=/cloudsql/{db_instance}'.format(db_instance=db_instance)
logger.debug('Cloud DB: {}'.format(cloud_server))

# create the structural SQLAlchemy objects
cloud_engine = create_engine(cloud_server, echo=True)

# add debug logging for each query before & after execution
event.listen(cloud_engine, "before_cursor_execute", before_cursor_execute)

Session = sessionmaker(bind=cloud_engine)
db = Session()
dbsql = cloud_engine.connect()

# tbdb = conn_tbdb  # TODO: clean up usage of tbdb vs conn_tbdb
# cursor = engine.interfaces.ExecutionContext.create_cursor()
# tbdb = conn_tbdb.create_cursor()
logger.debug('DB session object created')

# initialize global variables
page = 0
name = 'dummy'  # name must be a global variable before the determine_thread function
ongoing = False  # same with this variable
ulist = list()

# determine which thread to parse
determine_thread()  # sets the global name & page variables

# get html for the thread pages we want to parse and the thread's base URL
dbsql.execute('SELECT distinct page, html FROM raw.thread_page WHERE name = ? and page >= ? ORDER BY page', (name, page))
data = tbdb.fetchall()  # returns a list of tuples

# get the thread's base url
dbsql.execute('SELECT distinct url FROM raw.thread_page WHERE name = ? and page = 1 LIMIT 1', (name,))
url = return_first_value(tbdb.fetchone())

# user_id list of all BIF participants
dbsql.execute('SELECT distinct user_id FROM public.biffers WHERE thread_name = ? ORDER BY 1', (name,))
biffers_list = tbdb.fetchall()

# iterate through the thread pages to extract data about each post
page_counter = 1
for d in data:
    # if d[0] > 1: break
    print("Parsing page", d[0], "(" + str(page_counter), "of", str(len(data)) + ")")  # + ". Users:", len(ulist))
    # read, soup-ify the page
    soup = make_soup(d[1])
    page = d[0]

    posts = soup.find_all('li')  # class_="message   ")
    for p in posts:  # loop through each post on this page
        # gets the post_id
        try:
            post_id = int(re.findall('post-(\d+)', p['id'])[0])
        except:
            continue

        # replace any <ol>s with <ul>s
        p = make_soup(remove_ols(p))

        # tb-native thumbnails need to be adjusted to use a similar schema to tb-hosted images
        p = thumbfix(p)

        # create a slice that contains user data
        ud = p.find('div', class_="messageUserInfo")
        # extract the post_id
        temp = str(p)[:str(p).find('>') + 1]
        hint = 0

        pnhpot_class = "item muted postNumber hashPermalink OverlayTrigger"
        post_num = int(p.find('a', class_=pnhpot_class).text.replace('#','').strip())
        if page == 1:
            if post_num == 1:
                post_url = url
            else:
                post_url = url + '#post-' + str(post_id)
        else:
            post_url = url + 'page-' + str(page) + '#post-' + str(post_id)

        username = p.find('h3', class_="userText").a.text
        user_id = int(re.findall('/.+\.(\d+?)/\"', str(p.find('h3', class_="userText")))[0])
        # print("Post #" + post_num)

        # date & time are displayed in one of two ways
        time_blob = p.find('a', class_="datePermalink")
        if '<abbr' in str(time_blob):
            timestamp = to_timestamp(time_blob.text.strip())
        else:
            timestamp = to_timestamp(re.findall('title=\"(.+)\"', str(time_blob.find('span', class_="DateTime")))[0])

        # remove user signature, if applicable
        try: p.find('div', class_="baseHtml signature messageText ugc").decompose()
        except: pass

        # create a variable to store the post without any quotes for proper gifs/pics/media counting
        p_noquotes = p.find('div', class_="messageInfo primaryContent")
        qblocks = p_noquotes.find_all('div', class_="bbCodeBlock bbCodeQuote")

        # replace the goto links in quoted posts with local, relative anchors
        p = replace_goto(p) # function returns a string
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
            except:
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

        # print("*=*=*=*=*=*=*=*=*=* MESSAGE *=*=*=*=*=*=*=*=*=*")
        # print(",", "Post: #" + str(post_num) + ",", "gifs:", str(gifs) + ",", "Pics:", str(pic_counter) + ",")
        # print("Media:", str(media_counter) + ",", "Hint:", str(hint) + ",")
        # print("Timestamp:", timestamp, "Username:", username, "\n")
        write_post(str(p), post_id, username, message, timestamp, gifs, pic_counter, media_counter, post_num, page, name, post_url, user_id, hint)

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
            'id':       user_id,
            'username': username,
            'joindate': joindate,
            'location': location,
            }
        # add unique users to the list
        if user not in ulist:
            ulist.append(user)

    # commit after each page
    commit(conn_tbdb)
    page_counter += 1
    # pause()

# write user data to the db
write_users(ulist)
commit(conn_tbdb)

# update the single-page html file
update_file(name)
if ongoing:
    update_likes(name)

# update user data
if login_status is False:
    s.post(url_login, data=creds)  # log in with the provided credentials
    login_status = True

# run_raffle(1, name)
commit(conn_tbdb)
conn_tbdb.close()

time_end = datetime.datetime.now()
time_end = time_end.isoformat(timespec='seconds')
time_end = str(time_end).replace("T", " ")
print("")
print("***** ***** **** ***** *****")
print("  End:", time_end, "[normal path]")
print("***** ***** **** ***** *****")
print("")
