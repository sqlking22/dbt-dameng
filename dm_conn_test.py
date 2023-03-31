#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import dmPython


class DMOBJ:
    def __init__(self):
        self.dm_host = 'localhost'
        self.dm_port = 5236
        self.dm_user = 'SYSDBA'
        self.dm_password = 'SYSDBA'

    # 获取dm库连接
    def get_dm_connect(self, dbname):
        try:
            dm_conn = dmPython.connect(user=self.dm_user, password=self.dm_password, server=self.dm_host,
                                       port=self.dm_port)
            dm_cursor = dm_conn.cursor()
            dm_cursor.execute("set schema %s" % dbname)
            return dm_conn
        except (dmPython.Error, Exception) as err:
            print("could not connect to DM8 server", err)

    def get_dm_table_name(self, source_database):
        connection = self.get_dm_connect(source_database)
        cursor = connection.cursor()
        sql = "SELECT distinct TABLE_NAME from dba_tables WHERE owner='{0}'".format(source_database)
        cursor.execute(sql)
        fetchall = cursor.fetchall()
        return fetchall

    def get_dm_metadata(self, conn, dm_schema, dm_table):
        sql = """
        select  distinct
            A.COLUMN_ID as id,
            A.column_name as Field,
            case when A.data_type in('VARCHAR','CHAR') then concat(A.data_type,'(',A.data_length,')') 
                 when A.data_type in('DEC','DECIMAL') then concat(A.data_type,'(',A.DATA_PRECISION,',',A.DATA_SCALE,')') 
                 else A.data_type
            end as Type,
            B.comments as column_comments,
            C.COMMENTS as table_comments
        from ALL_TAB_COLUMNS A
        left join ALL_COL_COMMENTS B on A.COLUMN_NAME=B.column_name 
              and A.Table_Name =B.Table_Name and A.OWNER =B.OWNER
        left join ALL_TAB_COMMENTS C on A.Table_Name=C.TABLE_NAME and A.OWNER =C.OWNER
        where 1=1
          and A.OWNER='{0}'
          and A.Table_Name='{1}'
        ORDER BY id
        """.format(dm_schema, dm_table)
        cursor = conn.cursor()
        cursor.execute(sql)
        result_rows = cursor.fetchall()
        conn.close()
        return result_rows

    def get_dm_columns(self, db_name, table_name):
        dm_column = []
        dm_conn = self.get_dm_connect(db_name)
        dm_column_meta = self.get_dm_metadata(dm_conn, db_name, table_name)
        for row in dm_column_meta:
            # Field 信息
            dm_column.append(row[1])
        dm_conn.close()
        return dm_column

    def get_dm_record_cnt(self, db_name, table_name):
        dm_conn = self.get_dm_connect(db_name)
        cursor = dm_conn.cursor()
        cursor.execute("select count(1) from %s.%s" % (db_name, table_name))
        fetchone = cursor.fetchone()
        dm_conn.close()
        return fetchone[0]


if __name__ == '__main__':
    try:
        dm = DMOBJ()
        tables = dm.get_dm_table_name("DMHR")
        print(tables)
    except Exception as e:
        print("程序运行异常！！！", e)
