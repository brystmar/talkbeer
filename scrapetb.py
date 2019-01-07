#############################################################
### Stores posts from a talkbeer.com thread in a local db ###
#############################################################

from bs4 import BeautifulSoup
import sqlite3, re, html, datetime, requests
#from agg_posts import get_userdata, write_users

time_start = datetime.datetime.now()
time_start = time_start.isoformat(timespec='seconds')
time_start = str(time_start).replace("T", " ")
print("***** ***** **** ***** *****")
print(" Start:", time_start)
print("***** ***** **** ***** *****")
print("")
filename = __file__

def pause(): input("[====]")
def bs(html): return BeautifulSoup(html, 'html.parser')
def commit(db):
    db.commit()
    return 0
def pp(item):
    print("")
    print(item)
    pause()
def pl(items):
    for i in items: print(i)
    print("Total items:",len(items))
    pause()
def pl_sorted(items):
    newlist = list()
    ditems = set(items)
    for d in ditems:
        newlist.append(d)
    newlist.sort()
    for n in newlist:
        print(items.count(n), n)
    pause()
def month_to_num(m):
    if m == 'Jan': return '01'
    elif m == 'Feb': return '02'
    elif m == 'Mar': return '03'
    elif m == 'Apr': return '04'
    elif m == 'May': return '05'
    elif m == 'Jun': return '06'
    elif m == 'Jul': return '07'
    elif m == 'Aug': return '08'
    elif m == 'Sep': return '09'
    elif m == 'Oct': return '10'
    elif m == 'Nov': return '11'
    elif m == 'Dec': return '12'
    else: return 'Month Error'
def to_timestamp(ds):
    # converts a text date/time sting to ISO-8601 formatting: 'Mar 26, 2018 at 9:48 PM' --> '2018-03-26 21:48:00'
    year = int(re.findall(', (20\d\d) at', ds)[0])
    month = int(month_to_num(ds[:3]))
    day = int(re.findall(' (\d+),', ds)[0])
    minute = int(re.findall(' at \d+:(\d+) ', ds)[0])
    hour = int(re.findall(' at (\d+):', ds)[0])

    # update to 24-hour time
    if ds[-2].lower() == 'p' and hour != 12: hour += 12
    elif ds[-2].lower() == 'a' and hour == 12: hour -= 12
    else: pass

    return datetime.datetime(year, month, day, hour, minute)
def to_date(ds):
    # converts a text date string to ISO-8601 formatting: 'Mar 26, 2018' --> '2018-03-26'
    year = int(re.findall(', (20\d\d)', ds)[0])
    month = int(month_to_num(ds[:3]))
    day = int(re.findall(' (\d+),', ds)[0])
    return datetime.datetime(year, month, day)
def qmarks(num):
    # returns a string of question marks in the length requested, ex: (?,?,?,?)
    i = 0
    q = "("
    while i < num:
        q += "?,"
        i += 1
    return q[:-1] + ")"
def stop(text):
    print(text)
    print("")
    try: conn_tbdb.close()
    except: pass

    time_end = datetime.datetime.now()
    time_end = time_end.isoformat(timespec='seconds')
    time_end = str(time_end).replace("T", " ")

    print("***** ***** **** ***** *****")
    print("  End:", time_end)
    print("***** ***** **** ***** *****")
    print("")
    quit()
def remlist(data):
    #returns a list of strings instead of a list of lists/tuples
    l = list()
    t = tuple()
    if data == None or len(data) == 0: return None
    if type(data) != type(l): return data
    if type(data[0]) != type(l) and type(data[0]) != type(t):
        print(type(data), type(data[0]))
        print("NOPE")
        return data
    for d in data: l.append(d[0])
    return l
def db_value(val):
    # if the returned data is a tuple or list, this returns the first value in that set
    # otherwise, it returns the input value untouched
    l = list()
    t = tuple()
    if type(val) == type(t) or type(val) == type(l): return val[0]
    else: return val
def month_to_num(m):
    if m == 'Jan': return '01'
    elif m == 'Feb': return '02'
    elif m == 'Mar': return '03'
    elif m == 'Apr': return '04'
    elif m == 'May': return '05'
    elif m == 'Jun': return '06'
    elif m == 'Jul': return '07'
    elif m == 'Aug': return '08'
    elif m == 'Sep': return '09'
    elif m == 'Oct': return '10'
    elif m == 'Nov': return '11'
    elif m == 'Dec': return '12'
    else: return 'Month Error'
def to_timestamp(ds):
    # converts a text date/time sting to ISO-8601 formatting: 'Mar 26, 2018 at 9:48 PM' --> '2018-03-26 21:48:00'
    if " at " not in ds: ds += " at 0:01 AM"
    year = int(re.findall(', (20\d\d) at', ds)[0])
    month = int(month_to_num(ds[:3]))
    day = int(re.findall(' (\d+),', ds)[0])
    minute = int(re.findall(' at \d+:(\d+) ', ds)[0])
    hour = int(re.findall(' at (\d+):', ds)[0])
    # update to 24-hour time
    if ds[-2].lower() == 'p' and hour != 12: hour += 12
    elif ds[-2].lower() == 'a' and hour == 12: hour -= 12
    else: pass
    return datetime.datetime(year, month, day, hour, minute)
def next_link(url, num): return url + 'page-' + str(num)
def delete_url(url): tbdb.execute('DELETE FROM thread_page WHERE url = ? and html is null', (url,))
def find_last_post(html):
    soup = bs(html) #make soup
    # focus on the specific div
    posts = soup('div', class_="publicControls")
    # carve out info about the last post on the page
    lpinfo = posts[len(posts)-1].find('a', title='Permalink')
    # user-facing number for the last post
    last_post_num = int(lpinfo.text.replace('#','').strip())
    # system id for the last post
    last_post_id = int(re.findall('#post-(\d+)', lpinfo.get('href'))[0])
    return [last_post_num, last_post_id, page]
def first_post_data(html): #returns the post date and user_id for the first post on a page
    soup = bs(html) #make soup
    first_post = soup('ol', class_="messageList")[0]('li')[0] #returns the first post from the <ol> parent of the post <li>s
    data = soup('div', class_="titleBar")[0]
    user_id = int(re.findall('\.(\d+)\/', str(data('a', class_="username")[0]['href']))[0])
    start = to_timestamp(data.span['title'])

    return [str(start)[:10], user_id]
def write_thread(name, page, url, html, last_post):
    # validation
    tbdb.execute('SELECT page, url, last_post_num, last_post_id FROM thread_page WHERE page = ? and url = ?', (page, url))
    val = tbdb.fetchone()

    if val == None:
        tbdb.execute('INSERT INTO thread_page (name, page, url, html, last_post_num, last_post_id) VALUES ' + qmarks(6), (name, page, url, html, last_post[0], last_post[1]))
    else:
        tbdb.execute('DELETE FROM thread_page WHERE name = ? and page = ?', (name, page))
        tbdb.execute('INSERT INTO thread_page (name, page, url, html, last_post_num, last_post_id) VALUES ' + qmarks(6), (name, page, url, html, last_post[0], last_post[1]))
def write_thread_master(name, url, html):
    thread_id = int(re.findall('http.*\.(\d+)\/', url)[0])
    thread_data = first_post_data(html)
    tbdb.execute('INSERT INTO threads (name, id, url, ongoing, start, organizer_id) VALUES ' + qmarks(6), (name, thread_id, url, 'Y', thread_data[0], thread_data[1]))
def remove_pscroll(html):
    # remove ads flags
    html = html.replace("enable_page_level_ads: true", "enable_page_level_ads: false")
    html = html.replace("adsbygoogle", "ads123byN0Pty")

    # delete the <div> with the first-time-user notice
    soup = bs(html)
    try: soup.find('div', class_="PanelScroller Notices").decompose()
    except: html = html #this div was recently removed from the base tb template
    return str(soup)
def add_biffers(thread_name, biffers): #adds new BIF participants to the biffers table
    for b in biffers:
        # validation
        tbdb.execute('SELECT count(*) FROM biffers WHERE thread_name = ? and user_id = ?', (thread_name, b[0]))
        val = db_value(tbdb.fetchone())
        if val == 1: #user is already marked as BIF participant
            print("User is already in this BIF:", b)
            continue

        # write to db
        tbdb.execute('INSERT INTO biffers (thread_name, user_id, username, partner, partner_id, list_order) VALUES ' + qmarks(6), (thread_name, b[0], b[1], b[2], b[3], b[4]))
        print("Added biffer:", b)

        # check if user already exists in the users table
        tbdb.execute('SELECT distinct id FROM users WHERE id = ?', (b[0],))
        val = db_value(tbdb.fetchone())

        if val == None: #need to add this user
            write_users(get_userdata([b[0],b[1]]))
        commit(conn_tbdb)
def find_biffers(thread_name, html): #finds the post with the most users tagged (first page only)
    # find the post and store the users in a variable
    soup = bs(html)
    lis = soup.find('ol', class_="messageList").find_all('li') #collect all <li>s underneath the primary <ol>
    max_users = 8 #the post we're looking for should have dozens of users tagged.  starting at 8 to ignore posts with random user tagging
    biffers = []
    ignore = [-1,3611] #ignore the 'mods' account
    teams = [[]]
    post_id = 0
    for l in lis:
        if 'id="post-' in str(l): #filters out the bulleted lists from the posts
            post_id = int(re.findall('post-(\d+)', l['id'])[0])
            users = l.find('div', class_="messageContent").find_all('a', class_="username")
            num_users = len(users)
            if num_users > max_users:
                biffers.clear() #clear the user list
                listed_order = 1 #reset the counter
                for u in users: #extract info for each biffer
                    info = u['data-user'].split(', ') #returns a list for each biffer: [user_id, username]
                    info[0] = int(info[0])
                    if info[0] in ignore: continue
                    partner = [None,None]
                    for t in teams: #special handling if this biffer has a partner
                        if info[0] in t: #has a partner
                            # find the partner's username
                            for user in users:
                                partner = user['data-user'].split(', ')
                                partner[0] = int(partner[0])
                                if partner[0] in t and partner[0] != info[0]:
                                    # add both users to the list
                                    biffers.append([info[0], info[1], partner[0], partner[1], listed_order])
                                    biffers.append([partner[0], partner[1], info[0], info[1], listed_order])
                                    # add partner to the ignore list so they aren't recorded twice
                                    ignore.append(partner[0])
                                    break #no need to keep searching
                        else: #doesn't have a partner
                            biffers.append([info[0], info[1], None, None, listed_order])
                            break
                    listed_order += 1
                max_users = num_users
        else: continue #ignore <li>s that don't contain a forum post
    add_biffers(thread_name, biffers)
def write_users(ulist):
    # ensure the input is properly formatted as a list of dictionaries
    if type(ulist) != type([]): ulist = [ulist]
    # ulist is a dictionary with attributes: id, username, location, joindate
    for u in ulist:
        # validation
        tbdb.execute('SELECT distinct id, username, joindate FROM users WHERE id = ?', (u['id'],))
        val = tbdb.fetchone()

        if val == None:
            tbdb.execute('INSERT INTO users (id, username, location, joindate) VALUES ' + qmarks(4), (u['id'], u['username'], u['location'], u['joindate']))
            print("Added user", u['username'] + ", id:", u['id'])
        elif None in val:
            if u['location'] != None:
                tbdb.execute('UPDATE users SET username = ?, location = ?, joindate = ? WHERE id = ?', (u['username'], u['location'], u['joindate'], u['id']))
            else:
                tbdb.execute('UPDATE users SET username = ?, joindate = ? WHERE id = ?', (u['username'], u['joindate'], u['id']))
def get_userdata(user):
    # read & soupify the html for this user's page
    url = 'https://www.talkbeer.com/community/members/' + user[1].replace(' ','-').lower().strip() + '.' + str(user[0]) + '/'
    html = s.get(url).text
    soup = bs(html)
    joindate = None # there's no easy way to find the join date

    # some users restrict who can view their profile page
    try:
        jointext = soup.find('div', class_="section infoBlock").find_all('dd')
        for j in jointext:
            if ', 20' in j.text and ':' not in j.text:
                joindate = str(to_date(j.text))[:10]
                break
    except:
        joindate = None

    # only some users share their location
    try: location = soup.find('a', itemprop="address").text
    except: location = None

    # return user data as a dictionary
    return {'id': user[0], 'username': user[1], 'joindate': joindate, 'location': location}

# open & initialize the http session
s = requests.session()
s.headers.update({'User-Agent': 'Mozilla/59.0'}) #talkbeer's forum software doesn't allow scraping, so mask the request as a different user agent
s.verify = False #disable SSL verification
from requests.packages.urllib3.exceptions import InsecureRequestWarning
requests.packages.urllib3.disable_warnings(InsecureRequestWarning) #suppress SSL warning messages

# set credentials for the talkbeets account so we can log in when the time comes
from tbcred import url_login, user_sys, pw_sys
creds = {'login': user_sys, 'password': pw_sys}
login_status = False

# open & initialize the db
conn_tbdb = sqlite3.connect('talkbeer.sqlite')
tbdb = conn_tbdb.cursor()

# if there are ongoing threads, automatically scrape those instead of prompting for a thread to update
tbdb.execute('SELECT distinct name, url FROM threads WHERE ongoing = ? order by start', ('Y',))
toscrape = tbdb.fetchall()

if len(toscrape) == 0: #no ongoing threads
    # retrieve list of all thread nicknames
    tbdb.execute('SELECT distinct name FROM threads order by 1')
    thread_names = remlist(tbdb.fetchall()) #returns a list of strings (instead of a list of tuples)
    new_thread = False

    # show existing threads
    print('Existing threads:')
    for tn in thread_names: print(tn)
    print("")

    # get the thread URL & nickname to scrape
    toscrape = input("Enter an existing thread nickname or a new URL to scrape: ")
    if toscrape == None: stop('') #validate input length
    elif 'http' in toscrape: #user entered a URL
        url = toscrape
        # does this URL already have a nickname?
        tbdb.execute('SELECT distinct name FROM threads WHERE url = ? order by 1', (url,))
        oldname = remlist(tbdb.fetchall()) #returns a list of strings
        if oldname == None: #new thread, so it needs a nickname
            new_thread = True
            name = input("Create a nickname for this thread: ")
            if name == None or len(name) <= 2: stop('Input was either null or too short')
            while name in thread_names:
                name = input('Nickname is already taken.  Choose a different one: ')
        elif len(oldname) > 1: stop('More than one thread matches that URL')
        else: name = oldname[0]
    elif toscrape in thread_names or toscrape.upper() in thread_names: #valid nickname was entered, so grab that URL
        if toscrape.upper() in thread_names: name = toscrape.upper()
        else: name = toscrape
        tbdb.execute('SELECT min(url) FROM thread_page WHERE url is not null AND url <> "" AND name = ?', (name,))
        url = db_value(tbdb.fetchone())
        if url == None: #new thread that hasn't been scraped yet
            tbdb.execute('SELECT min(url) FROM threads WHERE name = ?', (name,))
            url = db_value(tbdb.fetchone())
        elif 'http' not in url: stop('Invalid URL from the db: ' + str(url))
    else: stop('Invalid nickname or URL')
else: #at least one ongoing thread
    new_thread = False
    for ts in toscrape:
        name = ts[0]
        url = ts[1]

# convert to the thread's base URL
if url[-1] == "/": pass
elif re.search('/page-\d+', url): url = url.replace(re.findall('/(page-.+)', url)[0], '')
else: stop("Unable to determine page# from the entered url: " + url)
page = 1

# see if thread data already exists in the db
tbdb.execute("SELECT distinct last_post_num, last_post_id, page, html FROM thread_page WHERE last_post_num = (SELECT max(last_post_num) FROM thread_page WHERE name = ?)", (name,))
last_post_data = tbdb.fetchone()

# log in with the provided credentials
s.post(url_login, data=creds)
login_status = True

if last_post_data == None or None in last_post_data: #data either doesn't exist or is incomplete
    print("Data either doesn't exist or is incomplete.")
    # delete any data that may exist
    delete_url(url)
    current_page = 1
    current_url = url
    html = remove_pscroll(s.get(current_url).text)
    write_thread(name, current_page, current_url, html, find_last_post(html))
    commit(conn_tbdb)
else: #data exists
    print("Found existing data for this thread.")
    current_page = last_post_data[2]
    if current_page == 1: current_url = url
    else: current_url = next_link(url, current_page)
    html = remove_pscroll(s.get(current_url).text)

# read, soup-ify the page
soup = BeautifulSoup(html, 'html.parser')

# does this thread have more than 1 page?
if soup.find('span', class_="pageNavHeader") == None: stop("This is a single-page thread.")

# determine the current & max pages
page_range = soup.find_all('span', class_="pageNavHeader")[0].text.strip()
current_page = int(re.findall('Page (\d+)', page_range)[0])
max_page = int(re.findall('Page \d+ of (\d+)', page_range)[0])

count = 1
while current_page <= max_page:
    delete_url(current_url)
    html = remove_pscroll(s.get(current_url).text)

    # if this is a new thread, create an html entry for page 0
    if new_thread and current_page == 1:
        # add to the master thread table
        new_thread = False
        html0 = html[:html.index('<ol class="messageList"')] + '<ol class="messageList" id="messageList">\n</ol>\n<hr>\n\n</form>\n<i>fin</i>\n</body>\n</html>'
        tbdb.execute('INSERT into thread_page (name, page, html) VALUES (?,?,?)', (name, 0, html0)) #write page 0
        write_thread_master(name, url, html) #write page 1
        commit(conn_tbdb)
        print("Wrote pages 0 & 1")
        find_biffers(name, html)

        """tbdb.execute('SELECt count(distinct user_id) FROM biffers WHERE thread_name = ' + name)
        bcount = db_value(tbdb.fetchone())

        if bcount == None or bcount == 0:
            print("Needs biffers!")
            find_biffers(name, html)"""

    print("Reading page", current_page, "of", max_page)
    write_thread(name, current_page, current_url, html, find_last_post(html))

    if count % 25 == 0: #don't scrape the world
        x = input("Continue? ")
        if x == None or x.lower() not in['y','yes','1']: break

    count += 1
    current_page += 1
    current_url = next_link(url, current_page)

    commit(conn_tbdb)

conn_tbdb.close()

time_end = datetime.datetime.now()
time_end = time_end.isoformat(timespec='seconds')
time_end = str(time_end).replace("T", " ")
print("")
print("***** ***** **** ***** *****")
print("  End:", time_end)
print("***** ***** **** ***** *****")
print("")

from agg_posts import determine_thread

# """