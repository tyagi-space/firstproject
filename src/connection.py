import mysql.connector
import csv

mydb = mysql.connector.connect(host='localhost',user='root',password='root',database='tcs')
cur=mydb.cursor()
s='DROP TABLE IF EXISTS student'
cur.execute(s)
cur.execute('CREATE TABLE student(id varchar(6) ,name varchar(15),add_id varchar(6), PRIMARY KEY(id,add_id),attendance_key int,active_ind varchar(1))')
a='DROP TABLE IF EXISTS address'
cur.execute(a)
cur.execute('CREATE TABLE address(add_id varchar(6) PRIMARY KEY,add1 varchar(20),add2 varchar(20),city varchar(15),state varchar(20),country varchar(20),postal_code int,contact_num varchar(12))')
at='DROP TABLE IF EXISTS attendance'
cur.execute(at)
cur.execute('CREATE TABLE attendance(id varchar(6),attendance_key int NOT NULL,attendance_date varchar(11),attendance_yes_no varchar(3))')

file_list = ['Day1.csv','Day2.csv','Day3.csv']
for i in range(0,3):
    with open(r"C:\Users\Kanha Tyagi\Desktop\Tcs_project\connection\\"+file_list[i]) as csv_file:
        reader = csv.reader(csv_file)
        next(csv_file)
        cur.execute('SELECT * FROM student')
        res=cur.fetchall()
        for row in reader: 
            flag=0
            if (res and len(res)>0): 
                for r in res:
                    if row[0]==r[0] and row[2]!=r[2] :
                        upst='UPDATE student SET active_ind="N" WHERE id=%s AND add_id=%s AND active_ind="Y"'
                        upval=(r[0],r[2])
                        cur.execute(upst,upval)
                        flag=0
                    elif row[0]==r[0]:
                        flag=1
            if flag==0:
                w='INSERT INTO student(id,name,add_id,attendance_key,active_ind) VALUES(%s,%s,%s,%s,%s)'
                val=(row[0],row[1],row[2],row[10],'Y')
                cur.execute(w,val)
                mydb.commit()
                x='INSERT INTO address(add_id,add1,add2,city,state,country,postal_code,contact_num) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)'
                val1=(row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9])
                cur.execute(x,val1)
                mydb.commit()  
            y='INSERT INTO attendance(id,attendance_key,attendance_date,attendance_yes_no) VALUES(%s,%s,%s,%s)'
            val2=(row[0],row[10],row[11],row[12])
            cur.execute(y,val2)
            mydb.commit()
            
        