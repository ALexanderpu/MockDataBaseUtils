# convert real database to mock database for software testing
# mysql
import os
import sys
import yaml
import random, string
from datetime import datetime, timedelta
from sqlalchemy import Integer, String, DateTime
from sqlalchemy import create_engine, MetaData, Table, Column, inspect
from sqlalchemy.sql.expression import func, select

from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import subprocess
from random import randrange

# number random generator: same range -> min~max
# string random generator: same range of length and appeared characters
without_decimal_types = ['INTEGER', 'INT', 'SMALLINT', 'TINYINT', 'MEDIUMINT', 'BIGINT']
with_decimal_types = ["DECIMAL", "NUMERIC", "FLOAT", "DOUBLE"]
string_types = ["CHAR", "VARCHAR", "BINARY", "VARBINARY", "BLOB", "TEXT"]
sample_types = ["ENUM"]
# datetime random generator:  in the range
datetime_types = ["TIMESTAMP", "DATETIME"]

def datetime_random_generator(min_range, max_range):
    delta = max_range - min_range
    random_second = randrange(int(delta.total_seconds()))
    return min_range + timedelta(seconds=random_second)

def number_random_generator(data_type, min_range, max_range):
    if data_type in without_decimal_types:
        return random.randint(min_range, max_range)  # endpoints included
    if data_type in with_decimal_types:
        return random.uniform(min_range, max_range)
    if data_type == "BIT":
        return (random.random() >= 0.5)


def str_random_generator(min_length, max_length):
    # endpoints included
    rand_length = max(0, random.randint(min_length, max_length))
    candidate = string.ascii_lowercase+string.digits
    return ''.join(random.choice(candidate) for _ in range(rand_length))


def make_session(connection_string):
    engine = create_engine(connection_string, echo=False, convert_unicode=True)
    Session = sessionmaker(bind=engine)
    return Session(), engine


def quick_mapper(table):
    Base = declarative_base()

    class GenericMapper(Base):
        __table__ = table

    return GenericMapper


# read config file for mysql
def create_schema(config_file):
    with open(config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)
    host = cfg['mysql_src']['host']
    user = cfg['mysql_src']['username']
    passwd = cfg['mysql_src']['password']
    db = cfg['mysql_src']['db']
    port = cfg['mysql_src']['port']
    connect_src = 'mysql+pymysql://' + user + ":" + passwd + "@" + host + ":" + str(port) + "/" + db

    host = cfg['mysql_dest']['host']
    user = cfg['mysql_dest']['username']
    passwd = cfg['mysql_dest']['password']
    port = cfg['mysql_dest']['port']
    connect_dest = 'mysql+pymysql://' + user + ":" + passwd + "@" + host + ":" + str(port)

    if connect_dest and connect_src:
        source, src_engine = make_session(connect_src)
        destination, dest_engine = make_session(connect_dest)
        dest_engine.execute("DROP DATABASE IF EXISTS " + db + ";")
        dest_engine.execute("CREATE DATABASE " + db + ";")  # create db
        dest_engine.execute("USE " + db)  # select new db

        src_metadata = MetaData(bind=src_engine)
        src_metadata.reflect(src_engine)
        src_metadata.create_all(dest_engine)

        source.close()
        destination.close()
        src_engine.dispose()
        dest_engine.dispose()
        return True
    return False


def insert_mock_data(config_file):
    random.seed(datetime.now())
    with open(config_file, 'r') as ymlfile:
        cfg = yaml.load(ymlfile)
    num_records = max(1, int(cfg['mock_database']['records_num']))
    host = cfg['mysql_src']['host']
    user = cfg['mysql_src']['username']
    passwd = cfg['mysql_src']['password']
    db = cfg['mysql_src']['db']
    port = cfg['mysql_src']['port']
    connect_src = 'mysql+pymysql://' + user + ":" + passwd + "@" + host + ":" + str(port) + "/" + db

    host = cfg['mysql_dest']['host']
    user = cfg['mysql_dest']['username']
    passwd = cfg['mysql_dest']['password']
    port = cfg['mysql_dest']['port']
    connect_dest = 'mysql+pymysql://' + user + ":" + passwd + "@" + host + ":" + str(port) + "/" + db
    if connect_dest and connect_src and num_records:
        source, src_engine = make_session(connect_src)
        destination, dest_engine = make_session(connect_dest)

        src_metadata = MetaData(bind=src_engine)
        src_metadata.reflect(src_engine)  # get columns from existing table
        inspector = inspect(src_engine)
        dest_metadata = MetaData(bind=dest_engine)
        for table in src_metadata.sorted_tables:
            print("process table: " + table.name)
            datatypes = []
            attributes = []
            columns_des = inspector.get_columns(table.name)
            num_cols = len(columns_des)
            # get_foreign_keys will return:
            # 'name': a string;
            # 'constrained_columns': a list;
            # 'referred_table': a name string;
            # 'referred_columns': a list
            fk_cols = []
            fk_refer_cols = []
            for fk in inspector.get_foreign_keys(table.name):
                if fk['name']:
                    for ele in range(len(fk['constrained_columns'])):
                        fk_cols.append(fk['constrained_columns'][ele])
                        fk_refer_cols.append((fk['referred_table'], fk['referred_columns'][ele]))

            srcTable = Table(table.name, src_metadata, autoload=True)
            primary_cols = [key.name for key in inspect(srcTable).primary_key]
            nullable = []
            for col_des in columns_des:
                # id & primary key should not be changed
                if col_des['name'] in primary_cols:
                    attributes.append("primary")
                elif col_des['name'] in fk_cols:
                    ind = fk_cols.index(col_des['name'])
                    attributes.append(ind)
                else:
                    attributes.append("none")
                if col_des['nullable']:
                    nullable.append(True)
                else:
                    nullable.append(False)
                datatypes.append(str(col_des['type']).split('(')[0])
            columns = srcTable.columns.keys()
            # columns:    column names               ['id',      'job_id',  'name',    'Desc']
            # datatypes:  used to generate mock data ['INTEGER', 'INTEGER', 'VARCHAR', 'VARCHAR']
            # attributes: used to match foreign keys ['primary', 1,         'none',    'none']
            # nullable:   if column can be null(None)[ False,    False,     False,     True]

            # inspect source database and collect info to generate random mock data
            NewRecord = quick_mapper(srcTable)
            min_ranges = []
            max_ranges = []
            candidate_sets = []

            for c in range(len(columns_des)):
                if datatypes[c] in datetime_types:
                    min_ranges.append(datetime.now())
                    max_ranges.append(datetime(1970, 1, 1))
                else:
                    min_ranges.append(sys.maxsize)
                    max_ranges.append(-sys.maxsize)
                candidate_sets.append(set([]))

            all_records = source.query(srcTable).all()

            for record in all_records:
                for c in range(len(columns_des)):
                    if attributes[c] != "none":
                        continue
                    if datatypes[c] in without_decimal_types or datatypes[c] in with_decimal_types:
                        if record[c] != None:  # it's not None
                            max_ranges[c] = max(max_ranges[c], record[c])
                            min_ranges[c] = min(min_ranges[c], record[c])
                        continue
                    if datatypes[c] in string_types:
                        # have to use api when u want to generate meaningful mock data like name, email, address, url
                        if record[c] != None:  # it's not None
                            max_ranges[c] = max(max_ranges[c], len(record[c]))
                            min_ranges[c] = min(min_ranges[c], len(record[c]))
                        continue
                    if datatypes[c] in sample_types:
                        if record[c] != None:  # it's not None
                            max_ranges[c] = max(max_ranges[c], len(record[c]))
                            min_ranges[c] = min(min_ranges[c], len(record[c]))
                            candidate_sets[c].add(record[c])
                        continue
                    if datatypes[c] in datetime_types:
                        if record[c] != None:  # it's not None
                            max_ranges[c] = max(max_ranges[c], record[c])
                            min_ranges[c] = min(min_ranges[c], record[c])

            # generate mock data using info collected && insert them to mock database
            mock_data = list()
            for record_ind in range(min(len(all_records), num_records)):
                new_record = []
                for c in range(len(columns_des)):
                    if attributes[c] == "primary":
                        new_record.append(all_records[record_ind][c])
                        continue
                    if attributes[c] != "none":
                        # should be the index foreign key
                        refer_foreign_table = fk_refer_cols[attributes[c]][0]
                        refer_foreign_col = fk_refer_cols[attributes[c]][1]
                        dest_foreign_table = Table(refer_foreign_table, dest_metadata, autoload=True)
                        # just randomly pick one
                        fk_columns_id = dest_foreign_table.columns.keys().index(refer_foreign_col)
                        rand_refer_foreign_col = destination.query(dest_foreign_table).order_by(func.rand()).first()
                        # print("has foreign key:" + rand_refer_foreign_col)
                        new_record.append(rand_refer_foreign_col[fk_columns_id])
                        continue
                    if nullable[c]:
                        if random.random() >= 0.5:
                            new_record.append(None)
                            continue
                    if datatypes[c] in without_decimal_types or datatypes[c] in with_decimal_types:
                        if min_ranges[c] > max_ranges[c]:
                            new_record.append(None)
                        else:
                            new_record.append(number_random_generator(datatypes[c], min_ranges[c], max_ranges[c]))
                        continue
                    if datatypes[c] in string_types:
                        if min_ranges[c] > max_ranges[c]:
                            new_record.append(None)
                        else:
                            new_record.append(str_random_generator(min_ranges[c], max_ranges[c]))
                        continue
                    if datatypes[c] in sample_types:
                        new_record.append(random.sample(candidate_sets[c], 1)[0])
                        continue
                    if datatypes[c] in datetime_types:
                        if min_ranges[c] > max_ranges[c]:
                            new_record.append(None)
                        else:
                            new_record.append(datetime_random_generator(min_ranges[c], max_ranges[c]))
                        continue
                    # right now it will not handle the types beyond the lists above so just copy origin data
                    new_record.append(all_records[record_ind][c])
                new_record = tuple(new_record)
                mock_data.append(dict([(str(columns[column]), new_record[column]) for column in range(len(columns))]))
            # commit to the destination db
            for data in mock_data:
                destination.merge(NewRecord(**data))
            print('Committing changes')
            destination.commit()
        # clse the database
        source.close()
        destination.close()
        src_engine.dispose()
        dest_engine.dispose()

def mock_database():
    if create_schema("config.yml"):
        insert_mock_data("config.yml")

mock_database()