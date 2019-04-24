CREATE TABLE "biffers" (
	"thread_name"	TEXT,
	"username"	TEXT,
	"user_id"	INTEGER,
	"my_sender"	TEXT,
	"haul_id"	INTEGER,
	"sender"	TEXT,
	"sender_id"	INTEGER,
	"target"	TEXT,
	"target_id"	INTEGER,
	"partner"	TEXT,
	"partner_id"	INTEGER,
	"list_order"	INTEGER,
	PRIMARY KEY("thread_name","user_id")
);

CREATE TABLE errors (
	post_id	INTEGER,
	url	TEXT,
	notes	TEXT
);

CREATE TABLE likes (
	post_id	INTEGER NOT NULL,
	user_id	INTEGER NOT NULL,
	timestamp	timestamp without time zone,
	PRIMARY KEY(post_id,user_id)
);

CREATE TABLE "posts" (
	id	INTEGER NOT NULL UNIQUE,
	username	TEXT,
	user_id	INTEGER,
	text	text,
	timestamp	timestamp without time zone,
	num	INTEGER,
	thread_page	INTEGER,
	thread_name	TEXT,
	gifs	INTEGER,
	pics	INTEGER,
	other_media	INTEGER,
	hint	INTEGER,
	url	TEXT,
	PRIMARY KEY(id)
);

CREATE TABLE region_map (
	state	text,
	abbrev	text,
	region  text
);

CREATE TABLE "threads" (
	name	TEXT,
	id	INTEGER,
	url	TEXT,
	ongoing	char(1),
	start_date	date,
	end_date	date,
	organizer_id	INTEGER,
	PRIMARY KEY(name)
);

CREATE TABLE "urls" (
	post	TEXT,
	user_page	TEXT,
	gdrive	TEXT
);

CREATE TABLE "users" (
	id	INTEGER NOT NULL,
	username	TEXT,
	location	TEXT,
	region	TEXT,
	joindate	date,
	PRIMARY KEY(id)
);


CREATE TABLE "posts_soup" (
	id	INTEGER NOT NULL UNIQUE,
	thread_name	TEXT,
	soup	text,
	PRIMARY KEY(id)
);

CREATE TABLE "thread_page" (
	name	TEXT,
	page	INTEGER,
	url	TEXT,
	html	text,
	last_post_num	INTEGER,
	last_post_id	INTEGER,
	PRIMARY KEY(page,url,name)
);
