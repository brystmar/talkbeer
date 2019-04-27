from sqlalchemy import Column, Integer, String, Date, TIMESTAMP
from sqlalchemy.ext.declarative import declarative_base, declared_attr

Base = declarative_base()


# Schema: public #
class PublicMixin(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    __table_args__ = {'schema': 'main'}


class Biffers(PublicMixin, Base):
    thread_name = Column(String, primary_key=True)
    username = Column(String)
    user_id = Column(Integer, primary_key=True)
    my_sender = Column(String)
    haul_id = Column(Integer)
    sender = Column(String)
    sender_id = Column(Integer)
    target = Column(String)
    target_id = Column(Integer)
    partner = Column(String)
    partner_id = Column(Integer)
    list_order = Column(Integer)


class Likes(PublicMixin, Base):
    post_id = Column(Integer, primary_key=True)
    user_id = Column(Integer, primary_key=True)
    timestamp = Column(TIMESTAMP)


class Posts(PublicMixin, Base):
    id = Column(Integer, primary_key=True)
    username = Column(String)
    user_id = Column(Integer)
    text = Column(String)
    timestamp = Column(TIMESTAMP)
    num = Column(Integer)
    thread_page = Column(Integer)
    thread_name = Column(String)
    gifs = Column(Integer)
    pics = Column(Integer)
    other_media = Column(Integer)
    hint = Column(Integer)
    url = Column(String)


class Region_Map(PublicMixin, Base):
    state = Column(String, primary_key=True)
    abbrev = Column(String, primary_key=True)
    region = Column(String, primary_key=True)


class Threads(PublicMixin, Base):
    name = Column(String, primary_key=True)
    id = Column(Integer)
    url = Column(String)
    ongoing = Column(String)
    start_date = Column(Date)
    end_date = Column(Date)
    organizer_id = Column(Integer)


class Users(PublicMixin, Base):
    id = Column(Integer, primary_key=True)
    username = Column(String)
    location = Column(String)
    region = Column(String)
    joindate = Column(Date)


# Schema: sys #
class SysMixin(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    __table_args__ = {'schema': 'sys'}


class URLs(SysMixin, Base):
    post = Column(String, primary_key=True)
    user_page = Column(String, primary_key=True)
    gdrive = Column(String, primary_key=True)
    login = Column(String, primary_key=True)


class Output_Options(SysMixin, Base):
    id = Column(Integer, primary_key=True)
    option = Column(String)


# Schema: raw #
class RawMixin(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    __table_args__ = {'schema': 'raw'}


class Errors(RawMixin, Base):
    post_id = Column(Integer)
    url = Column(String, primary_key=True)
    notes = Column(String)


class Posts_Soup(RawMixin, Base):
    id = Column(Integer, primary_key=True)
    thread_name = Column(String)
    soup = Column(String)


class Thread_Page(RawMixin, Base):
    name = Column(String)
    page = Column(String)
    url = Column(String)
    html = Column(String, primary_key=True)
    last_post_num = Column(Integer)
    last_post_id = Column(Integer)
