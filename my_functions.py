"""tberg's newb-ish functions, all in one place!"""
from bs4 import BeautifulSoup
import datetime
import re


def sort_list_of_dictionaries(unsorted_list, key_to_sort_by, reverse=False) -> list:
    return sorted(unsorted_list, key=lambda k: k[key_to_sort_by], reverse=reverse)


def pause():
    input("[====]")


def make_soup(html_to_soupify) -> BeautifulSoup:
    return BeautifulSoup(html_to_soupify, 'html.parser')


def commit(db):
    db.commit()


def pp(item):
    print("")
    print(item)
    pause()


def pd(items):
    """Print all items in a provided dict, then pause until the user is ready to proceed."""
    for item in items:
        print("{item}: {value}".format(item=item, value=items[item]))
        # logger.debug("{item}: {value}".format(item=item, value=items[item]))
    pause()
    print("")


def pl(items):
    for i in items:
        print(i)
    print("Total items:", len(items))
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


def month_to_num(m) -> str:
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


def to_timestamp(ds) -> datetime.datetime:
    """Converts a text date/time sting to ISO-8601 formatting: 'Mar 26, 2018 at 9:48 PM' --> '2018-03-26 21:48:00'."""
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


def to_date(ds) -> datetime.date:
    """Converts a text date string to ISO-8601 formatting: 'Mar 26, 2018' --> '2018-03-26'."""
    year = int(re.findall(', (20\d\d)', ds)[0])
    month = int(month_to_num(ds[:3]))
    day = int(re.findall(' (\d+),', ds)[0])
    return datetime.date(year, month, day)
