from sqlalchemy import Column, Integer, String, Date, TIMESTAMP, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base, declared_attr

Base = declarative_base()


# Schema: public
class PublicMixin(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    __table_args__ = {'schema': 'public'}


class Biffers(PublicMixin, Base):
    thread_name = Column(String, ForeignKey('public.threads.name'), primary_key=True)
    username = Column(String)
    user_id = Column(Integer, ForeignKey('public.users.id'), primary_key=True)
    my_sender = Column(String)
    haul_id = Column(Integer, ForeignKey('public.posts.id'))
    sender = Column(String)
    sender_id = Column(Integer, ForeignKey('public.users.id'))
    target = Column(String)
    target_id = Column(Integer, ForeignKey('public.users.id'))
    partner = Column(String)
    partner_id = Column(Integer, ForeignKey('public.users.id'))
    list_order = Column(Integer)

    def __repr__(self):
        return "<Biffer(thread='%s', user='%s', user_id='%s')>" % (self.thread_name, self.username, self.user_id)


class Likes(PublicMixin, Base):
    post_id = Column(Integer, ForeignKey('public.posts.id'), primary_key=True)
    user_id = Column(Integer, ForeignKey('public.users.id'), primary_key=True)
    timestamp = Column(TIMESTAMP)

    def __repr__(self):
        return "<Like(post_id='%s', user_id='%s')>" % (self.post_id, self.user_id)


class Posts(PublicMixin, Base):
    id = Column(Integer, primary_key=True)
    username = Column(String)
    user_id = Column(Integer, ForeignKey('public.users.id'))
    text = Column(String)
    timestamp = Column(TIMESTAMP)
    num = Column(Integer)
    thread_page = Column(Integer)
    thread_name = Column(String, ForeignKey('public.threads.name'))
    gifs = Column(Integer)
    pics = Column(Integer)
    other_media = Column(Integer)
    hint = Column(Integer)
    url = Column(String)

    soup = relationship('Posts_Soup', back_populates='post_info', lazy='dynamic')

    def __repr__(self):
        return "<Post(id='%s', user='%s')>" % (self.id, self.username)


class Region_Map(PublicMixin, Base):
    state = Column(String, primary_key=True)
    abbrev = Column(String, primary_key=True)
    region = Column(String, primary_key=True)

    def __repr__(self):
        return "<Region_Map(state='%s', region='%s')>" % (self.state, self.region)


class Threads(PublicMixin, Base):
    name = Column(String, primary_key=True)
    id = Column(Integer)
    url = Column(String)
    ongoing = Column(String)
    start_date = Column(Date)
    end_date = Column(Date)
    organizer_id = Column(Integer)

    def __repr__(self):
        return "<Thread(name='%s', id='%s'>" % (self.name, self.id)


class Users(PublicMixin, Base):
    id = Column(Integer, primary_key=True)
    username = Column(String)
    location = Column(String)
    region = Column(String)
    joindate = Column(Date)

    def __repr__(self):
        return "<User(name='%s', fullname='%s', nickname='%s')>" % (self.name, self.fullname, self.nickname)


# Schema: sys
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

    def __repr__(self):
        return "<URL>"


class Output_Options(SysMixin, Base):
    id = Column(Integer, primary_key=True)
    option = Column(String)

    def __repr__(self):
        return "<Output_Options(id='%s'>" % self.id


# Schema: raw
class RawMixin(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    __table_args__ = {'schema': 'raw'}


class Posts_Soup(RawMixin, Base):
    id = Column(Integer, ForeignKey('public.posts.id'), primary_key=True)
    thread_name = Column(String)
    soup = Column(String)

    post_info = relationship('Posts', back_populates='soup', lazy='dynamic')

    def __repr__(self):
        return "<Post_Soup(id='%s', thread='%s')>" % (self.id, self.thread_name)


class Thread_Page(RawMixin, Base):
    name = Column(String)
    page = Column(String)
    url = Column(String)
    html = Column(String, primary_key=True)
    last_post_num = Column(Integer)
    last_post_id = Column(Integer)

    def __repr__(self):
        return "<Thread_Page(name='%s', page='%s')>" % (self.name, self.page)
