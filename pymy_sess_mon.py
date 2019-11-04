import pymysql.cursors
import pandas as pd
import time
import os
import sys
import datetime

mon_gb = sys.argv[1];

if sys.argv[2] == "host":
    hostname = "hostname"

conn = pymysql.connect(host=hostname,
                       port=3306,
                       user=sys.argv[3],
                       password=sys.argv[4],
#                       db='rdw',
                       charset='utf8mb4',
                       cursorclass=pymysql.cursors.DictCursor
                       )

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1200)

s = datetime.datetime.now()
cur_date = s.strftime("%Y%m%d")


try:
    with conn.cursor() as cursor:
        if mon_gb == 'sess':
            sql = "select ID,USER,HOST,DB,COMMAND,TIME,STATE, SUBSTR(INFO,1,100) INFO from information_schema.PROCESSLIST where command <> 'Sleep'  and info not like 'select ID,USER,HOST,DB,COMMAND,TIME,STATE%' "
        elif mon_gb =='lock':
            sql = "select a.requesting_trx_id , a.blocking_trx_id ,b.trx_query,b.trx_isolation_level,b.trx_tables_locked,b.trx_tables_in_use,b.trx_operation_state,c.USER,c.HOST,c.DB,c.COMMAND,c.TIME,c.STATE,c.id  from information_schema.innodb_lock_waits a,information_schema.innodb_trx b,information_schema.PROCESSLIST c where   a.blocking_trx_id = b.trx_id     and b.trx_mysql_thread_id = c.id "

        while 1==1 :
            cursor.execute(sql)
            result = cursor.fetchall()

            df = pd.DataFrame(result)

            s = datetime.datetime.now()
            cur_date_time = s.strftime("%Y-%m-%d %H:%M:%S")
            rows_affected = cursor.rowcount
            with open("c:/mysql_sess_log/" + cur_date + "_" + mon_gb + "_" + hostname + ".log", 'a') as f:
                print("========================================= "+hostname+" =====================================================")
                f.write("========================================= " + hostname + " =====================================================\n")
                if mon_gb =="sess":
                    print("current time : " + cur_date_time + " Active Session : " + str(rows_affected))
                    f.write("current time : " + cur_date_time + " Active Session : " + str(rows_affected)+"\n")
                elif mon_gb == "lock":
                    print("current time : " + cur_date_time + " Active Lock : " + str(rows_affected))

                    f.write("current time : " + cur_date_time + " Active Lock : " + str(rows_affected)+"\n")
            if not df.empty:
                print(df)
                df.to_csv("c:/mysql_sess_log/" + cur_date + "_" + mon_gb + "_" + hostname + ".log", mode='a')
                # f.write(df)

            time.sleep(3)
            os.system('cls')
            # (1, 'test@test.com', 'my-passwd')
finally:
    conn.close()

