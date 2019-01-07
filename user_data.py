"""
Collects location and join dates for all users from html pages.
    talkbeer.threads schema: [name, page, url, html, last_post_num, last_post_id]
    talkbeer.biffers schema: [thread_name, username, user_id, my_sender, target, partner]
    talkbeer.users   schema: [id, username, location, joindate, url]
    talkbeer.posts   schema: [id, username, message, timestamp, pics_gif, pics_other, other_media, num, thread_name, thread_page, url, user_id]
    talkbeer.posts_raw:      [id, thread_name, soup]
"""

from bs4 import BeautifulSoup
#from datetime import datetime
import sqlite3, re, html, ssl, datetime

time_start = datetime.datetime.now()
time_start = time_start.isoformat(timespec='seconds')
time_start = str(time_start).replace("T", " ")
print("***** ***** **** ***** *****")
print(" Start:", time_start)
print("***** ***** **** ***** *****")
print("")

def pause(): input("====")
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
def pd(items):
    #if 'card' not in str(items): return 0
    for i in items:
        print(i, items[i])
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
    print(text)
    print("")
    try: conn_tbdb.close()
    except: text.strip()
    quit()
def db_value(val):
    # if the returned data is a tuple or list, this returns the first value in that set
    # otherwise, it returns the input value untouched
    l = list()
    t = tuple()
    if type(val) == type(l) or type(val) == type(t): return val[0]
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
def to_date(ds):
    # converts a text date sting to ISO-8601 formatting: 'Mar 26, 2018' --> '2018-03-26'
    year = int(re.findall(', (20\d\d)', ds)[0])
    month = int(month_to_num(ds[:3]))
    day = int(re.findall(' (\d+),', ds)[0])

    return str(datetime.datetime(year, month, day))
def write_users(ulist):
    # ulist is a dictionary with attributes: id, username, location, joindate, url
    for u in ulist:
        # validation
        tbdb.execute('SELECT distinct id, username, joindate, url FROM users WHERE id = ?', (u['id'],))
        val = tbdb.fetchone()

        if val == None:
            tbdb.execute('INSERT INTO users (id, username, location, joindate, url) VALUES ' + qmarks(5), (u['id'], u['username'], u['location'], u['joindate'], u['url']))
        elif None in val:
            if u['location'] != None:
                tbdb.execute('UPDATE users SET username = ?, location = ?, joindate = ?, url = ? WHERE id = ?', (u['username'], u['location'], u['joindate'], u['url'], u['id']))
            else:
                tbdb.execute('UPDATE users SET username = ?, joindate = ?, url = ? WHERE id = ?', (u['username'], u['joindate'], u['url'], u['id']))
        else: print("User", u['username'] + ", id:", u['id'], "already exists.")

ulist = list()

# get the thread name
#name = input("Enter BIF name to aggregate: ")
name = 'SSF14'
filename = name + '.html'

# open & initialize the db
conn_tbdb = sqlite3.connect('talkbeer3.sqlite')
tbdb = conn_tbdb.cursor()

tbdb.execute('SELECT distinct html FROM threads order by page')
pages = tbdb.fetchall()

for p in pages:
    soup = BeautifulSoup(p[0], 'html.parser')
    udata = soup.find_all('div', class_="messageUserInfo")

    for ud in udata:
        user_info = ud.find('h3', class_="userText")
        try: user_part = re.findall('href=\"(.+?)\"', str(user_info))[0]
        except:
            pp(str(ud))
            pp(str(user_info))
            user_part = re.findall('href=\"(.+?)\"', str(user_info))[0]

        user_id = int(re.findall('/.+\.(\d+?)/', user_part)[0])
        user_url = 'https://www.talkbeer.com/community/' + user_part
        username = user_info.find('a', class_="username").text

        fields = ud.find_all('dd') #returns [text-based date, location]
        # some users hide their location
        if len(fields) == 1:
            joindate = to_date(fields[0].text)
            location = None
        elif len(fields) == 2:
            joindate = to_date(fields[0].text)
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
            'url':      user_url
            }
        # add unique users to the list
        if user in ulist: continue
        else: ulist.append(user)

        print("Added", username)

print("")
print(len(ulist), "total users")
print("")

write_users(ulist)
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

#if platform.system() == 'Windows': conn_tbdb = sqlite3.connect("\\\\greendale\Public\Python\\talkbeer\\talkbeer.sqlite")
#elif platform.system() == 'Darwin': conn_tbdb = sqlite3.connect('/Volumes/Public/Python/talkbeer/talkbeer.sqlite')
# """
