import cx_Oracle
import psycopg2

from psycopg2 import OperationalError

dict_oracle = {}
dict_postgres = {}

def create_connection_postgres(db_name, db_user, db_password, db_host, db_port):
    connection = None
    try:
        connection = psycopg2.connect(
            database=db_name,
            user=db_user,
            password=db_password,
            host=db_host,
            port=db_port,
        )
        print("Connection to PostgreSQL DB successful")
    except OperationalError as e:
        print(f"The error '{e}' occurred")
    return connection


connection = create_connection_postgres("vipeng", "postgres", None, "10.216.154.176", "5432")


def create_connection_oracle(db_user, db_password, db_dsn):
    connection2 = None
    try:
        connection2 = cx_Oracle.connect(
            user=db_user,
            password=db_password,
            #host=db_host,
            #port=db_port,
            #SID=db_sid,
            dsn=db_dsn,
        )
        print("Connection to ORACLE DB successful")
    except cx_Oracle.DatabaseError as e:
        print(f"The error '{e}' occurred")
    return connection2

connection2 = create_connection_oracle("symcdba","Kornar1234", "vipeng-us-west-2.cjad7b8nmr8t.us-west-2.rds.amazonaws.com:1521/VIPENG")




def table_count_postgres_fin(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    cursor1 = connection.cursor()
    try:
        cursor.execute(query)
        cursor1.execute("SELECT child.relname AS child FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid   = child.oid JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace where nmsp_child.nspname='fin';")
        table_counts = cursor.fetchall()
        sub_part_tab = cursor1.fetchall()
        tab_fin = [i for i in table_counts if i not in sub_part_tab]
        #print(tab_fin)
        #print(sub_part_tab)
        #dict_postgres['tab_count_fin'] = str(cursor.rowcount)
        dict_postgres['tab_count_fin'] = str(len(tab_fin))
        #print("Table Counts is          :" + str(cursor.rowcount))

    except OperationalError as e:
        print(f"The error '{e}' occurred")


table_count_query_fin = """ select table_name from information_schema.tables where table_schema ='fin'; """

table_count_postgres_fin(connection, table_count_query_fin)








def table_count_postgres_pin(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    cursor1 = connection.cursor()
    try:
        cursor.execute(query)
        cursor1.execute(
            "SELECT child.relname AS child FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid   = child.oid JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace where nmsp_child.nspname='pin';")
        table_counts = cursor.fetchall()
        sub_part_tab = cursor1.fetchall()
        tab_pin = [i for i in table_counts if i not in sub_part_tab]
        #print(tab_pin)
        # print(sub_part_tab)
        # dict_postgres['tab_count_fin'] = str(cursor.rowcount)
        dict_postgres['tab_count_pin'] = str(len(tab_pin))

    except OperationalError as e:
        print(f"The error '{e}' occurred")


table_count_query_pin = """ select table_name from information_schema.tables where table_schema ='pin'; """

table_count_postgres_pin(connection, table_count_query_pin)


def table_count_postgres_ia(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    cursor1 = connection.cursor()
    try:
        cursor.execute(query)
        cursor1.execute("SELECT child.relname AS child FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid   = child.oid JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace where nmsp_child.nspname='ia';")
        table_counts = cursor.fetchall()
        sub_part_tab = cursor1.fetchall()
        tab_ia = [i for i in table_counts if i not in sub_part_tab]
        #print(tab_ia)
        # print(sub_part_tab)
        # dict_postgres['tab_count_fin'] = str(cursor.rowcount)
        dict_postgres['tab_count_ia'] = str(len(tab_ia))
        #print("Table Counts is          :"+str(cursor.rowcount))



    except OperationalError as e:
        print(f"The error '{e}' occurred")


table_count_query_ia = """ select table_name from information_schema.tables where table_schema ='ia'; """

table_count_postgres_ia(connection, table_count_query_ia)


def table_count_oracle_fin(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        table_counts = cursor.fetchall()

        #for table in table_counts:
            #print(table)
        dict_oracle['tab_count_fin'] = str(cursor.rowcount)
        #dict_oracle['table_count'] = str(cursor.rowcount)
        #print("Table Counts is          :" + str(cursor.rowcount))

    except cx_Oracle.DatabaseError as e:
        print(e)


table_count_query_ofin = (" SELECT table_name FROM all_tables WHERE owner='FIN' ")

table_count_oracle_fin(connection2, table_count_query_ofin)


def table_count_oracle_pin(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        table_counts = cursor.fetchall()

        #for table in table_counts:
            #print(table)
        dict_oracle['tab_count_pin'] = str(cursor.rowcount)
        #dict_oracle['table_count'] = str(cursor.rowcount)
        #print("Table Counts is          :" + str(cursor.rowcount))

    except cx_Oracle.DatabaseError as e:
        print(e)


table_count_query_opin = (" SELECT table_name FROM all_tables WHERE owner='PIN' ")

table_count_oracle_pin(connection2, table_count_query_opin)


def table_count_oracle_ia(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        table_counts = cursor.fetchall()

        #for table in table_counts:
            #print(table)
        dict_oracle['tab_count_ia'] = str(cursor.rowcount)
        #dict_oracle['table_count'] = str(cursor.rowcount)
        #print("Table Counts is          :" + str(cursor.rowcount))

    except cx_Oracle.DatabaseError as e:
        print(e)


table_count_query_oia = (" SELECT table_name FROM all_tables WHERE owner='IA' ")

table_count_oracle_ia(connection2, table_count_query_oia)


def trigger_count_postgres_fin(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        trigger_counts = cursor.fetchall()

        # for trigger in trigger_counts:
        # print(trigger)
        #print("Trigger Counts is        :" + str(cursor.rowcount))
        dict_postgres['trigger_count_fin'] = str(cursor.rowcount)


    except OperationalError as e:
        print(f"The error '{e}' occurred")


trigger_count_query_pfin = """ select trigger_name from information_schema.triggers where trigger_schema = 'fin'; """

trigger_count_postgres_fin(connection, trigger_count_query_pfin)


def trigger_count_postgres_pin(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        trigger_counts = cursor.fetchall()

        # for trigger in trigger_counts:
        # print(trigger)
        #print("Trigger Counts is        :" + str(cursor.rowcount))
        dict_postgres['trigger_count_pin'] = str(cursor.rowcount)


    except OperationalError as e:
        print(f"The error '{e}' occurred")


trigger_count_query_ppin = """ select trigger_name from information_schema.triggers where trigger_schema = 'pin'; """

trigger_count_postgres_pin(connection, trigger_count_query_ppin)


def trigger_count_postgres_ia(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        trigger_counts = cursor.fetchall()

        # for trigger in trigger_counts:
        # print(trigger)
        #print("Trigger Counts is        :" + str(cursor.rowcount))
        dict_postgres['trigger_count_ia'] = str(cursor.rowcount)


    except OperationalError as e:
        print(f"The error '{e}' occurred")


trigger_count_query_pia = """ select trigger_name from information_schema.triggers where trigger_schema = 'ia'; """

trigger_count_postgres_ia(connection, trigger_count_query_pia)

def trigger_count_oracle_fin(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        trigger_counts = cursor.fetchall()

        #for trigger in trigger_counts:
            #print(trigger)
        dict_oracle['trigger_count_fin'] = str(cursor.rowcount)
        #print("Trigger Counts is        :" + str(cursor.rowcount))


    except cx_Oracle.DatabaseError as e:
        print(e)


trigger_count_query_ofin = ("select TRIGGER_NAME from ALL_TRIGGERS WHERE owner='FIN'  ")

trigger_count_oracle_fin(connection2, trigger_count_query_ofin)


def trigger_count_oracle_pin(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        trigger_counts = cursor.fetchall()

        #for trigger in trigger_counts:
            #print(trigger)
        dict_oracle['trigger_count_pin'] = str(cursor.rowcount)
        #print("Trigger Counts is        :" + str(cursor.rowcount))


    except cx_Oracle.DatabaseError as e:
        print(e)


trigger_count_query_opin = ("select TRIGGER_NAME from ALL_TRIGGERS WHERE owner='PIN' ")

trigger_count_oracle_pin(connection2, trigger_count_query_opin)



def trigger_count_oracle_ia(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        trigger_counts = cursor.fetchall()

        #for trigger in trigger_counts:
            #print(trigger)
        dict_oracle['trigger_count_ia'] = str(cursor.rowcount)
        #print("Trigger Counts is        :" + str(cursor.rowcount))


    except cx_Oracle.DatabaseError as e:
        print(e)


trigger_count_query_oia = ("select TRIGGER_NAME from ALL_TRIGGERS WHERE owner='IA' ")

trigger_count_oracle_ia(connection2, trigger_count_query_oia)


def index_count_postgres_fin(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    cursor1 = connection.cursor()
    try:
        cursor.execute(query)
        cursor1.execute(
            "SELECT child.relname AS child FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid   = child.oid JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace where nmsp_child.nspname='fin';")
        index_counts = cursor.fetchall()
        sub_part_tab = cursor1.fetchall()
        tab_fin = [i for i in index_counts if i not in sub_part_tab]
        #print(tab_fin)
        # print(sub_part_tab)
        # dict_postgres['tab_count_fin'] = str(cursor.rowcount)
        dict_postgres['index_count_fin'] = str(len(tab_fin))
        # print("Table Counts is          :" + str(cursor.rowcount))

    except OperationalError as e:
        print(f"The error '{e}' occurred")



index_count_query_pfin = """ select indexname from pg_indexes where schemaname  ='fin';  """

index_count_postgres_fin(connection, index_count_query_pfin)



def index_count_postgres_pin(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    cursor1 = connection.cursor()
    try:
        cursor.execute(query)
        cursor1.execute(
            "SELECT child.relname AS child FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid   = child.oid JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace where nmsp_child.nspname='pin';")
        index_counts = cursor.fetchall()
        sub_part_tab = cursor1.fetchall()
        index_pin = [i for i in index_counts if i not in sub_part_tab]
        #print(index_pin)
        # print(sub_part_tab)
        # dict_postgres['tab_count_fin'] = str(cursor.rowcount)
        dict_postgres['index_count_pin'] = str(len(index_pin))
        # print("Table Counts is          :" + str(cursor.rowcount))

    except OperationalError as e:
        print(f"The error '{e}' occurred")


index_count_query_ppin = """ select indexname from pg_indexes where schemaname  ='pin';  """

index_count_postgres_pin(connection, index_count_query_ppin)



def index_count_postgres_ia(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    cursor1 = connection.cursor()
    try:
        cursor.execute(query)
        cursor1.execute("SELECT child.relname AS child FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid   = child.oid JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace where nmsp_child.nspname='ia';")
        index_counts = cursor.fetchall()
        sub_part_tab = cursor1.fetchall()
        index_ia = [i for i in index_counts if i not in sub_part_tab]
        # print(index_pin)
        # print(sub_part_tab)
        # dict_postgres['tab_count_fin'] = str(cursor.rowcount)
        dict_postgres['index_count_ia'] = str(len(index_ia))
        # print("Table Counts is          :" + str(cursor.rowcount))

    except OperationalError as e:
        print(f"The error '{e}' occurred")


index_count_query_pia = """ select indexname from pg_indexes where schemaname  ='ia';  """

index_count_postgres_ia(connection, index_count_query_pia)


def index_count_oracle_fin(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        index_counts = cursor.fetchall()

        #for index in index_counts:
            #print(index)
        #dict_oracle['index_count'] = str(cursor.rowcount)
        #print("Index Counts is          :" + str(cursor.rowcount))
        dict_oracle['index_count_fin'] = str(cursor.rowcount)

    except cx_Oracle.DatabaseError as e:
        print(e)


index_count_query_ofin =  ("select INDEX_NAME from ALL_INDEXES WHERE owner='FIN'")
index_count_oracle_fin(connection2, index_count_query_ofin)


def index_count_oracle_pin(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        index_counts = cursor.fetchall()

        #for index in index_counts:
            #print(index)
        #dict_oracle['index_count'] = str(cursor.rowcount)
        #print("Index Counts is          :" + str(cursor.rowcount))
        dict_oracle['index_count_pin'] = str(cursor.rowcount)

    except cx_Oracle.DatabaseError as e:
        print(e)


index_count_query_opin =  ("select INDEX_NAME from ALL_INDEXES WHERE owner='PIN' ")
index_count_oracle_pin(connection2, index_count_query_opin)


def index_count_oracle_ia(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        index_counts = cursor.fetchall()

        #for index in index_counts:
            #print(index)
        #dict_oracle['index_count'] = str(cursor.rowcount)
        #print("Index Counts is          :" + str(cursor.rowcount))
        dict_oracle['index_count_ia'] = str(cursor.rowcount)

    except cx_Oracle.DatabaseError as e:
        print(e)


index_count_query_oia =  ("select INDEX_NAME from ALL_INDEXES WHERE owner='IA' ")
index_count_oracle_ia(connection2, index_count_query_oia)


def constraint_count_postgres_fin(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    cursor1 = connection.cursor()
    try:
        cursor.execute(query)
        cursor1.execute("SELECT child.relname AS child FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid   = child.oid JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace ; ")
        constraint_counts = cursor.fetchall()
        sub_part_tab = cursor1.fetchall()
        constraint_fin = [i for i in constraint_counts if i not in sub_part_tab]
        #print(constraint_fin)
        # print(sub_part_tab)
        # dict_postgres['tab_count_fin'] = str(cursor.rowcount)
        dict_postgres['constraint_count_fin'] = str(len(constraint_fin))
        # print("Table Counts is          :" + str(cursor.rowcount))

    except OperationalError as e:
        print(f"The error '{e}' occurred")


constraint_count_query_pfin = """ select  constraint_name  from information_schema.table_constraints where  table_schema = 'fin';  """

constraint_count_postgres_fin(connection, constraint_count_query_pfin)


def constraint_count_postgres_pin(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        constraint_counts = cursor.fetchall()

        # for constraint in constraint_counts:
        # print(constraint)
        #print("Constraint Counts is     :" + str(cursor.rowcount))
        dict_postgres['constraint_count_pin'] = str(cursor.rowcount)

    except OperationalError as e:
        print(f"The error '{e}' occurred")


constraint_count_query_ppin = """ select  constraint_name  from information_schema.table_constraints where  table_schema = 'pin';  """

constraint_count_postgres_pin(connection, constraint_count_query_ppin)



def constraint_count_postgres_ia(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        constraint_counts = cursor.fetchall()

        # for constraint in constraint_counts:
        # print(constraint)
        #print("Constraint Counts is     :" + str(cursor.rowcount))
        dict_postgres['constraint_count_ia'] = str(cursor.rowcount)

    except OperationalError as e:
        print(f"The error '{e}' occurred")


constraint_count_query_pia = """ select  constraint_name  from information_schema.table_constraints where  table_schema = 'ia'; """

constraint_count_postgres_ia(connection, constraint_count_query_pia)


def constraint_count_oracle_fin(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        constraint_counts = cursor.fetchall()

        #for constraint in constraint_counts:
            #print(constraint)
        dict_oracle['constraint_count_fin'] = str(cursor.rowcount)
        #print("Constraint Counts is     :" + str(cursor.rowcount))

    except cx_Oracle.DatabaseError as e:
        print(e)


constraint_count_query_ofin = ("select CONSTRAINT_NAME from ALL_CONSTRAINTS WHERE owner='FIN' ")

constraint_count_oracle_fin(connection2, constraint_count_query_ofin)


def constraint_count_oracle_pin(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        constraint_counts = cursor.fetchall()

        #for constraint in constraint_counts:
            #print(constraint)
        dict_oracle['constraint_count_pin'] = str(cursor.rowcount)
        #print("Constraint Counts is     :" + str(cursor.rowcount))

    except cx_Oracle.DatabaseError as e:
        print(e)


constraint_count_query_opin = ("select CONSTRAINT_NAME from ALL_CONSTRAINTS WHERE owner='PIN' ")

constraint_count_oracle_pin(connection2, constraint_count_query_opin)



def constraint_count_oracle_ia(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        constraint_counts = cursor.fetchall()

        #for constraint in constraint_counts:
            #print(constraint)
        dict_oracle['constraint_count_ia'] = str(cursor.rowcount)
        #print("Constraint Counts is     :" + str(cursor.rowcount))

    except cx_Oracle.DatabaseError as e:
        print(e)


constraint_count_query_oia = (" select CONSTRAINT_NAME from ALL_CONSTRAINTS WHERE owner='IA' ")

constraint_count_oracle_ia(connection2, constraint_count_query_oia)


def partition_count_postgres_fin(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    cursor1 = connection.cursor()
    try:
        cursor.execute(query)
        cursor1.execute(" SELECT child.relname AS child FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid   = child.oid JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace ; ")
        partition_counts = cursor.fetchall()
        sub_part_tab = cursor1.fetchall()
        partition_fin = [i for i in partition_counts if i not in sub_part_tab]
        # print(constraint_fin)
        # print(sub_part_tab)
        # dict_postgres['tab_count_fin'] = str(cursor.rowcount)
        dict_postgres['partition_count_fin'] = str(len(partition_fin))
        # print("Table Counts is          :" + str(cursor.rowcount))

    except OperationalError as e:
        print(f"The error '{e}' occurred")




partition_count_query_pfin = """ SELECT parent.relname AS parent FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid  = child.oid JOIN pg_namespace nmsp_parent  ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace where nmsp_parent.nspname='fin';  """

partition_count_postgres_fin(connection, partition_count_query_pfin)


def partition_count_postgres_pin(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        partition_counts = cursor.fetchall()

        # for partition in partition_counts:
        # print(partition)
        #print("Partition Counts is      :" + str(cursor.rowcount))
        dict_postgres['partition_count_pin'] = str(cursor.rowcount)

    except OperationalError as e:
        print(f"The error '{e}' occurred")


partition_count_query_ppin = """ SELECT parent.relname AS parent FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid  = child.oid JOIN pg_namespace nmsp_parent  ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace where nmsp_parent.nspname='pin'; """

partition_count_postgres_pin(connection, partition_count_query_ppin)

def partition_count_postgres_ia(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        partition_counts = cursor.fetchall()

        # for partition in partition_counts:
        # print(partition)
        #print("Partition Counts is      :" + str(cursor.rowcount))
        dict_postgres['partition_count_ia'] = str(cursor.rowcount)

    except OperationalError as e:
        print(f"The error '{e}' occurred")


partition_count_query_pia = """ SELECT parent.relname AS parent FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid  = child.oid JOIN pg_namespace nmsp_parent  ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace where nmsp_parent.nspname='ia'; """

partition_count_postgres_ia(connection, partition_count_query_pia)


def partition_count_oracle_fin(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        partition_counts = cursor.fetchall()

        #for partition in partition_counts:
            #print(partition)
        dict_oracle['partition_count_fin'] = str(cursor.rowcount)
        #print("Partition Counts is      :" + str(cursor.rowcount))

    except cx_Oracle.DatabaseError as e:
        print(e)


partition_count_query_ofin = ("select  PARTITION_NAME from ALL_TAB_PARTITIONS WHERE table_owner='FIN' ")

partition_count_oracle_fin(connection2, partition_count_query_ofin)

def partition_count_oracle_pin(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        partition_counts = cursor.fetchall()

        #for partition in partition_counts:
            #print(partition)
        dict_oracle['partition_count_pin'] = str(cursor.rowcount)
        #print("Partition Counts is      :" + str(cursor.rowcount))

    except cx_Oracle.DatabaseError as e:
        print(e)


partition_count_query_opin = ("select  PARTITION_NAME from ALL_TAB_PARTITIONS WHERE table_owner='PIN' ")

partition_count_oracle_pin(connection2, partition_count_query_opin)

def partition_count_oracle_ia(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        partition_counts = cursor.fetchall()

        #for partition in partition_counts:
            #print(partition)
        dict_oracle['partition_count_ia'] = str(cursor.rowcount)
        #print("Partition Counts is      :" + str(cursor.rowcount))

    except cx_Oracle.DatabaseError as e:
        print(e)


partition_count_query_oia = (" select  PARTITION_NAME from ALL_TAB_PARTITIONS WHERE table_owner='IA' ")

partition_count_oracle_ia(connection2, partition_count_query_oia)


def sub_partition_count_postgres_fin(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        sub_partition_counts = cursor.fetchall()

        # for sub_partition in sub_partition_counts:
        # print(sub_partition)
        #print("Sub-Partition Counts is  :" + str(cursor.rowcount))
        dict_postgres['sub_partition_count_fin'] = str(cursor.rowcount)

    except OperationalError as e:
        print(f"The error '{e}' occurred")


sub_partition_count_query_pfin = """SELECT child.relname AS child FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid   = child.oid JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace where nmsp_child.nspname='fin'; """

sub_partition_count_postgres_fin(connection, sub_partition_count_query_pfin)


def sub_partition_count_postgres_pin(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        sub_partition_counts = cursor.fetchall()

        # for sub_partition in sub_partition_counts:
        # print(sub_partition)
        #print("Sub-Partition Counts is  :" + str(cursor.rowcount))
        dict_postgres['sub_partition_count_pin'] = str(cursor.rowcount)

    except OperationalError as e:
        print(f"The error '{e}' occurred")


sub_partition_count_query_ppin = """SELECT child.relname AS child FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid   = child.oid JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace where nmsp_child.nspname='pin'; """

sub_partition_count_postgres_pin(connection, sub_partition_count_query_ppin)


def sub_partition_count_postgres_ia(connection, query):
    connection.autocommit = True
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        sub_partition_counts = cursor.fetchall()

        # for sub_partition in sub_partition_counts:
        # print(sub_partition)
        #print("Sub-Partition Counts is  :" + str(cursor.rowcount))
        dict_postgres['sub_partition_count_ia'] = str(cursor.rowcount)

    except OperationalError as e:
        print(f"The error '{e}' occurred")


sub_partition_count_query_pia = """SELECT child.relname AS child FROM pg_inherits JOIN pg_class parent ON pg_inherits.inhparent = parent.oid JOIN pg_class child ON pg_inherits.inhrelid   = child.oid JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child ON nmsp_child.oid = child.relnamespace where nmsp_child.nspname='ia'; """

sub_partition_count_postgres_ia(connection, sub_partition_count_query_pia)




def sub_partition_count_oracle_fin(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        sub_partition_counts = cursor.fetchall()

        #for sub_partition in sub_partition_counts:
            #print(sub_partition)

        dict_oracle['sub_partition_count_fin'] = str(cursor.rowcount)
        #print("Sub-Partition Counts is  :" + str(cursor.rowcount))

    except cx_Oracle.DatabaseError as e:
        print(e)

sub_partition_count_query_ofin = (" select SUBPARTITION_NAME FROM ALL_TAB_SUBPARTITIONS WHERE table_owner='FIN'  ")

sub_partition_count_oracle_fin(connection2, sub_partition_count_query_ofin)



def sub_partition_count_oracle_pin(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        sub_partition_counts = cursor.fetchall()

        #for sub_partition in sub_partition_counts:
            #print(sub_partition)

        dict_oracle['sub_partition_count_pin'] = str(cursor.rowcount)
        #print("Sub-Partition Counts is  :" + str(cursor.rowcount))

    except cx_Oracle.DatabaseError as e:
        print(e)

sub_partition_count_query_opin = (" select SUBPARTITION_NAME FROM ALL_TAB_SUBPARTITIONS WHERE table_owner='PIN' ")

sub_partition_count_oracle_pin(connection2, sub_partition_count_query_opin)

def sub_partition_count_oracle_ia(connection2, query):
    connection2.autocommit = True
    cursor = connection2.cursor()
    try:
        cursor.execute(query)
        sub_partition_counts = cursor.fetchall()

        #for sub_partition in sub_partition_counts:
            #print(sub_partition)

        dict_oracle['sub_partition_count_ia'] = str(cursor.rowcount)
        #print("Sub-Partition Counts is  :" + str(cursor.rowcount))

    except cx_Oracle.DatabaseError as e:
        print(e)

sub_partition_count_query_oia = (" select SUBPARTITION_NAME FROM ALL_TAB_SUBPARTITIONS WHERE table_owner='IA' ")

sub_partition_count_oracle_ia(connection2, sub_partition_count_query_oia)



def cmp(dictionary1, dictionary2):
    is_equal = dictionary1 == dictionary2
    if is_equal:
        print('Values match')
    else:
        print('Values does not match')
    common_pairs = {}
    uncommon_pairs = {}
    for key in dictionary1:
        if (key in dictionary2 and dictionary1[key] == dictionary2[key]):
            common_pairs[key] = dictionary1[key]
        else:
            uncommon_pairs[key] = dictionary1[key]
            print(key, dictionary1[key], dictionary2[key])

    #print(common_pairs)
    print("\n")
    #print(uncommon_pairs)
    print("\n")
    print("Below are the Counts of Postgres and oracle = " + "\n")

    print("Postgres FIN Table counts        : ", dict_postgres['tab_count_fin'])
    print("Oracle FIN Table counts          : ", dict_oracle['tab_count_fin']+ "\n")

    print("Postgres PIN Table counts        : ", dict_postgres['tab_count_pin'])
    print("Oracle PIN Table counts          : ", dict_oracle['tab_count_pin']+ "\n")

    print("Postgres IA Table counts         : ", dict_postgres['tab_count_ia'] )
    print("Oracle IA Table counts           : ", dict_oracle['tab_count_ia'] + "\n")

    # print("Postgres FIN Trigger counts      : ", dict_postgres['trigger_count_fin'])
    # print("Oracle FIN Trigger counts         : ", dict_oracle['trigger_count_fin']+ "\n")
    #
    # print("Postgres PIN Trigger counts      : ", dict_postgres['trigger_count_pin'])
    # print("Oracle PIN Trigger counts        : ", dict_oracle['trigger_count_pin']+ "\n")
    #
    #
    # print("Postgres IA Trigger counts       : ", dict_postgres['trigger_count_ia'])
    # print("Oracle IA Trigger counts         : ", dict_oracle['trigger_count_ia'] + "\n")

    print("Postgres  FIN index count s      : ", dict_postgres['index_count_fin'])
    print("Oracle FIN index count s         : ", dict_oracle['index_count_fin'] + "\n")

    print("Postgres  PIN index count s      : ", dict_postgres['index_count_pin'])
    print("Oracle PIN index count s         : ", dict_oracle['index_count_pin']+ "\n")

    print("Postgres  IA index count s       : ", dict_postgres['index_count_ia'])
    print("Oracle IA index count s          :", dict_oracle['index_count_ia'] + "\n")

    # print("Postgres FIN constraint count    : ", dict_postgres['constraint_count_fin'])
    # print("Oracle FIN constraint count      : ", dict_oracle['constraint_count_fin']+ "\n")
    #
    # print("Postgres PIN constraint count    : ", dict_postgres['constraint_count_pin'])
    # print("Oracle PIN constraint count      : ", dict_oracle['constraint_count_pin']+ "\n")
    #
    # print("Postgres IA constraint count     : ", dict_postgres['constraint_count_ia'] )
    # print("Oracle IA constraint count       : ", dict_oracle['constraint_count_ia'] + "\n")

    # print("Postgres FIN partition count     : ", dict_postgres['partition_count_fin'])
    # print("Oracle FIN partition count       : ", dict_oracle['partition_count_fin'] + "\n")
    #
    # print("Postgres PIN partition count     : ", dict_postgres['partition_count_pin'])
    # print("Oracle PIN partition count       : ", dict_oracle['partition_count_pin'] + "\n")
    #
    # print("Postgres IA partition count      : ", dict_postgres['partition_count_ia'] )
    # print("Oracle IA partition count        : ", dict_oracle['partition_count_ia'] + "\n")
    #
    # print("Postgres FIN sub partition count : ", dict_postgres['sub_partition_count_fin'])
    # print("Oracle FIN sub partition count   : ", dict_oracle['sub_partition_count_fin'] + "\n")
    #
    #
    # print("Postgres PIN sub partition count : ", dict_postgres['sub_partition_count_pin'])
    # print("Oracle PIN sub partition count   : ", dict_oracle['sub_partition_count_pin'] + "\n")
    #
    # print("Postgres IA sub partition count  : ", dict_postgres['sub_partition_count_ia'])
    # print("Oracle IA sub partition count    : ", dict_oracle['sub_partition_count_ia'] + "\n")


cmp(dict_oracle,dict_postgres)





#SELECT *	 FROM ALL_TAB_SUBPARTITIONS WHERE table_owner='FIN'

# SELECT nmsp_parent.nspname AS parent_schema, parent.relname      AS parent, nmsp_child.nspname  AS child_schema, child.relname       AS child FROM pg_inherits JOIN pg_class parent            ON pg_inherits.inhparent = parent.oid JOIN pg_class child             ON pg_inherits.inhrelid   = child.oid JOIN pg_namespace nmsp_parent   ON nmsp_parent.oid  = parent.relnamespace JOIN pg_namespace nmsp_child    ON nmsp_child.oid   = child.relnamespace where nmsp_parent.nspname='fin';



#select SUBPARTITION_NAME,PARTITION_NAME FROM ALL_TAB_SUBPARTITIONS WHERE table_owner='PIN'



