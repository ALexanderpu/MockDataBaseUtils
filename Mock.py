# convert real database to mock database for software testing
# mysql
import os
import sys
import yaml
from decimal import *
import random, string
from datetime import datetime, timedelta
from sqlalchemy import create_engine, MetaData, Table, Column, inspect
from sqlalchemy.sql.expression import func, select
from sqlalchemy.orm import mapper, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import subprocess
from random import randrange

# ORM  Object-Relational Mapping
# id (autoincrement)
# number random generator: same range -> min~max
# string random generator: same range of length and appeared characters
without_decimal_types = ["YEAR", 'INTEGER', 'INT', 'SMALLINT', 'TINYINT', 'MEDIUMINT', 'BIGINT']
with_decimal_types = ["DECIMAL", "NUMERIC", "FLOAT", "DOUBLE"]
string_types = ["CHAR", "VARCHAR", "BINARY", "VARBINARY", "BLOB", "TEXT", "TINYBLOB", "TINYTEXT", "MEDIUMBLOB", "MEDIUMTEXT", "LONGBLOB", "LONGTEXT"]
sample_types = ["ENUM"]
datetime_types = ["TIMESTAMP", "DATETIME", "DATE", "TIME"]
# one is date and the other one is time should be converted to datetime


def datetime_random_generator(data_type, min_range, max_range):
    delta = max_range - min_range
    if delta.total_seconds() > 0:
        random_second = randrange(int(delta.total_seconds()))
    else:
        random_second = random.randint(10, 10000)

    result = min_range + timedelta(seconds=random_second)

    if data_type == "DATE":
        return result.date()
    elif data_type == "TIME":
        return result.time()
    else:
        return result



def number_random_generator(data_type, min_range, max_range):
    if data_type in without_decimal_types:
        return random.randint(min_range, max_range)  # endpoints included
    if data_type in with_decimal_types:
        result = random.uniform(float(min_range), float(max_range))
        if data_type == "DECIMAL":
            return Decimal(result)
        else:
            return result
    if data_type == "BIT":
        return random.random() >= 0.5
    # how about "ENUM" "SET"


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
            # table.name = "salaries"
            print("Processing table: " + table.name)

            datatypes = []
            columns_des = inspector.get_columns(table.name)
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

            columns = srcTable.columns.keys()
            primary_cols_index = []
            foreign_cols_index = []
            foreign_fk_cols_index = []
            nullable_cols_index = []
            col_index = -1
            for col_des in columns_des:
                col_index += 1
                # primary keys should be generate unique: using set
                if col_des['name'] in primary_cols:
                    primary_cols_index.append(col_index)

                if col_des['name'] in fk_cols:
                    ind = fk_cols.index(col_des['name'])
                    foreign_fk_cols_index.append(ind)
                    foreign_cols_index.append(col_index)

                if col_des['nullable']:
                    nullable_cols_index.append(col_index)

                datatypes.append(str(col_des['type']).split('(')[0])

            # columns:    column names               ['id',      'job_id',  'name',    'Desc']
            # datatypes:  used to generate mock data ['INTEGER', 'INTEGER', 'VARCHAR', 'VARCHAR']
            # primary_cols_index: [0]
            # foreign_cols_index: [1]
            # nullable_cols_index:   if column can be null(None)[3]

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

            # what if the table is so big?
            num_rows = source.query(srcTable).count()
            if num_rows > 1000*num_records:
                num_rows = min(num_rows, 1000*num_records)
            all_records = source.query(srcTable).limit(num_rows).all()

            for record in all_records:
                for c in range(len(columns_des)):
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
                            if datatypes[c] == "TIME":
                                if type(record[c]) is datetime.time:
                                    cur_record = datetime.combine(datetime.min.date(), record[c])
                                else:
                                    cur_record = datetime.min
                            elif datatypes[c] == "DATE":
                                if type(record[c]) is datetime.date:
                                    cur_record = datetime.combine(record[c], datetime.min.time())
                                else:
                                    cur_record = datetime.min
                            else:
                                # DATETIME, TIMESTAMP
                                if type(record[c]) is datetime:			
                                    cur_record = record[c]
                                else:
                                    cur_record = datetime.min
                            max_ranges[c] = max(max_ranges[c], cur_record)
                            min_ranges[c] = min(min_ranges[c], cur_record)

            # generate mock data using info collected && insert them to mock database

            # 1. generate unique primary keys
            mock_data = list()
            primary_keys_set =set([])
            mock_data_total = min(len(all_records), num_records)
            print('Generating primary columns')
            # set the number of times to generate unique primary keys
            try_times = 10*mock_data_total
            cur_times = 0
            while (len(primary_keys_set) < mock_data_total) and (cur_times <= try_times):
                cur_times += 1
                new_primary_keys = []
                for c in range(len(columns_des)):
                    if c in primary_cols_index:
                        if c in foreign_cols_index:
                            # the primary key is foreign key

                            ind = foreign_cols_index.index(c)
                            refer_ind = foreign_fk_cols_index[ind]
                            refer_foreign_table = fk_refer_cols[refer_ind][0]
                            refer_foreign_col = fk_refer_cols[refer_ind][1]
                            dest_foreign_table = Table(refer_foreign_table, dest_metadata, autoload=True)
                            # just randomly pick one
                            fk_columns_id = dest_foreign_table.columns.keys().index(refer_foreign_col)
                            rand_refer_foreign_col = destination.query(dest_foreign_table).order_by(func.rand()).first()
                            # print("has foreign key:" + rand_refer_foreign_col)
                            new_primary_keys.append(rand_refer_foreign_col[fk_columns_id])
                        else:
                            # the primary key is not the foreign key, random generate

                            if datatypes[c] in without_decimal_types or datatypes[c] in with_decimal_types:
                                new_primary_keys.append(number_random_generator(datatypes[c], min_ranges[c], max_ranges[c]))
                                continue
                            if datatypes[c] in string_types:
                                new_primary_keys.append(str_random_generator(min_ranges[c], max_ranges[c]))
                                continue
                            if datatypes[c] in sample_types:
                                new_primary_keys.append(random.sample(candidate_sets[c], 1)[0])
                                continue
                            if datatypes[c] in datetime_types:
                                new_primary_keys.append(datetime_random_generator(datatypes[c], min_ranges[c], max_ranges[c]))
                                continue

                # tuple is hashable, should convert
                new_primary_keys = tuple(new_primary_keys)
                primary_keys_set.add(new_primary_keys)

            # 2. generate none-primary keys
            primary_keys_list = list(primary_keys_set)
            print("Generated " + str(len(primary_keys_list)))

            print('Generating other columns')

            for record_ind in range(len(primary_keys_list)):
                new_record = []
                for c in range(len(columns_des)):
                    if c in primary_cols_index:
                        list_index = primary_cols_index.index(c)
                        new_record.append(primary_keys_list[record_ind][list_index])
                        continue
                    if c in foreign_cols_index:
                        # foreign key is not hte primary key, which can be repeated
                        ind = foreign_cols_index.index(c)
                        refer_ind = foreign_fk_cols_index[ind]
                        refer_foreign_table = fk_refer_cols[refer_ind][0]
                        refer_foreign_col = fk_refer_cols[refer_ind][1]
                        dest_foreign_table = Table(refer_foreign_table, dest_metadata, autoload=True)
                        # just randomly pick one
                        fk_columns_id = dest_foreign_table.columns.keys().index(refer_foreign_col)
                        rand_refer_foreign_col = destination.query(dest_foreign_table).order_by(func.rand()).first()

                        new_record.append(rand_refer_foreign_col[fk_columns_id])
                        continue
                    if c in nullable_cols_index:
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
                            new_record.append(datetime_random_generator(datatypes[c], min_ranges[c], max_ranges[c]))
                        continue

                    # right now will not handle the types beyond the lists above
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
