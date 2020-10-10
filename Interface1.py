import psycopg2
import os
import sys


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE """ + ratingstablename + """(
    userid integer,
    extra1 char,
    movieid integer,
    extra2 char,
    rating float,
    extra3 char,
    timestamp bigint
    )
    """)

    with open(ratingsfilepath, 'r') as f:
        cur.copy_from(f, ratingstablename, sep=':')

    cur.execute(""" ALTER TABLE """ + ratingstablename + """
            DROP COLUMN IF EXISTS extra1,
            DROP COLUMN IF EXISTS extra2,
            DROP COLUMN IF EXISTS extra3,
            DROP COLUMN IF EXISTS timestamp;
            """)
    cur.close()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection
    cur = conn.cursor()
    n = numberofpartitions
    xy = 5/n
    for i in range(0, n):
        minn = i*xy
        maxx = minn+xy
        tablename = " range_ratings_part" + str(i)
        cur.execute("""CREATE TABLE """ + tablename +
                    """ (userid integer, movieid integer, rating float);""")
        if i == 0:
            cur.execute(""" insert into """ + tablename + """ (userid, movieid, rating) select userid, movieid, rating from """ + ratingstablename + """ where rating >= """ +
                        str(minn) + """ and rating <= """ + str(maxx) + """;""")

        else:
            cur.execute(""" insert into """ + tablename + """ (userid, movieid, rating) select userid, movieid, rating from """ + ratingstablename + """ where rating > """ +
                        str(minn) + """ and rating <= """ + str(maxx) + """;""")
    cur.close()


def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    conn = openconnection
    cur = conn.cursor()
    n = numberofpartitions
    for i in range(n):
        tablename = "round_robin_ratings_part" + str(i)
        cur.execute("""create table """ + tablename +
                    """(userid integer, movieid integer, rating float);""")
        cur.execute("""insert into """ + tablename + """(userid, movieid, rating) select userid, movieid, rating from (select row_number() over() as rownum, userid, movieid, rating from """ +
                    ratingstablename + """ ) as rowidd where mod(rowidd.rownum-1,""" + str(n) + """)=""" + str(i) + """;""")
    cur.close()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute(""" insert into """ + ratingstablename + """ (userid, movieid, rating) values (""" +
                str(userid) + "," + str(itemid) + "," + str(rating) + """);""")
    cur.execute(
        """ select count(*) from  pg_stat_user_tables where relname like 'round_robin_ratings_part%'; """)
    countoftables = int(cur.fetchone()[0])
    # print(countoftables)
    cur.execute("""select count(*) from """ + ratingstablename + """;""")
    countofrows = cur.fetchone()[0]
    # print(countofrows)
    i = (countofrows-1) % (countoftables)
    # print(i, countofrows, countoftables)
    cur.execute(""" insert into round_robin_ratings_part""" + str(i) + """(userid, movieid, rating) values (""" +
                str(userid) + "," + str(itemid) + "," + str(rating) + """);""")

    cur.close()


def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    conn = openconnection
    cur = conn.cursor()
    cur.execute(
        """ select count(*) from  pg_stat_user_tables where relname like 'range_ratings_part%'; """)
    count = int(cur.fetchone()[0])
    cur.execute(""" insert into """ + ratingstablename + """ (userid, movieid, rating) values (""" +
                str(userid) + "," + str(itemid) + "," + str(rating) + """);""")
    rangepart = (5/(count))
    av = int(rating/rangepart)
    if (rating % rangepart == 0) and av != 0:
        av = av-1
    cur.execute(""" insert into range_ratings_part""" + str(av) + """ (userid, movieid, rating) values (""" +
                str(userid) + "," + str(itemid) + "," + str(rating) + """);""")
    cur.close()


def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    conn = openconnection
    cur = conn.cursor()
    cur.execute(
        """ select count(*) from  pg_stat_user_tables where relname like 'round_robin_ratings_part%'; """)
    countofrobin = cur.fetchone()[0]
    cur.execute(
        """ select count(*) from  pg_stat_user_tables where relname like 'range_ratings_part%'; """)
    countofrange = cur.fetchone()[0]
    for i in range(countofrobin):
        cur.execute("""select * from round_robin_ratings_part""" + str(i) + """ where rating >=""" +
                    str(ratingMinValue) + """ and rating<=""" + str(ratingMaxValue) + """;""")
        # print(cur.fetchone())
        robinparts = cur.fetchall()
        tabbb = """round_robin_ratings_part""" + str(i)
        for i in robinparts:
            userid_rob = i[0]
            movieid_rob = i[1]
            rating_rob = i[2]
            op = tabbb + "," + str(userid_rob) + "," + \
                str(movieid_rob) + "," + str(rating_rob)
            txtfile = open(outputPath, "a")
            txtfile.write(op)
            txtfile.write("\n")
    for i in range(countofrange):
        cur.execute("""select * from range_ratings_part""" + str(i) + """ where rating >=""" +
                    str(ratingMinValue) + """ and rating<=""" + str(ratingMaxValue) + """;""")
        # print(cur.fetchone())
        rangeparts = cur.fetchall()

        tabbb = """range_ratings_part""" + str(i)
        for i in rangeparts:
            userid_range = i[0]
            movieid_range = i[1]
            rating_range = i[2]
            op = tabbb + "," + str(userid_range) + "," + \
                str(movieid_range) + "," + str(rating_range)
            txtfile = open(outputPath, "a")
            txtfile.write(op)
            txtfile.write("\n")
    cur.close()


def pointQuery(ratingValue, openconnection, outputPath):
    conn = openconnection
    cur = conn.cursor()
    cur.execute(
        """ select count(*) from  pg_stat_user_tables where relname like 'round_robin_ratings_part%'; """)
    countofrange = cur.fetchone()[0]
    cur.execute(
        """ select count(*) from  pg_stat_user_tables where relname like 'range_ratings_part%'; """)
    countofrobin = cur.fetchone()[0]
    for i in range(countofrobin):
        cur.execute("""select * from round_robin_ratings_part""" + str(i) + """ where rating=""" +
                    str(ratingValue) + """;""")
        # print(cur.fetchone())
        robinparts = cur.fetchall()
        tabbb = """round_robin_ratings_part""" + str(i)
        for i in robinparts:
            userid_rob = i[0]
            movieid_rob = i[1]
            rating_rob = i[2]
            op = tabbb + "," + str(userid_rob) + "," + \
                str(movieid_rob) + "," + str(rating_rob)
            txtfile = open(outputPath, "a")
            txtfile.write(op)
            txtfile.write("\n")
    for i in range(countofrange):
        cur.execute("""select * from range_ratings_part""" + str(i) + """ where rating=""" +
                    str(ratingValue) + """;""")
        # print(cur.fetchone())
        rangeparts = cur.fetchall()

        tabbb = """range_ratings_part""" + str(i)
        for i in rangeparts:
            userid_range = i[0]
            movieid_range = i[1]
            rating_range = i[2]
            op = tabbb + "," + str(userid_range) + "," + \
                str(movieid_range) + "," + str(rating_range)
            txtfile = open(outputPath, "a")
            txtfile.write(op)
            txtfile.write("\n")

    cur.close()


def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute(
        'SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()


def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
