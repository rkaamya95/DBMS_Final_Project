import mysql.connector
import pandas as pd
import numpy as np
from getpass import getpass
from mysql.connector import connect, Error

try:
    with connect(
        host="localhost",
        user=input("Enter username: "),
        password=getpass("Enter password: "),
    ) as connection:
        print(connection)
        """
        Creating a new database project1_db
        """
        create_db_query = "CREATE DATABASE project1_db"
        with connection.cursor() as cursor:
            cursor.execute(create_db_query)
except Error as e:
    print(e)
	

db=mysql.connector.connect(
    host='localhost',
    user='kaamya',
    passwd='kaamya',
    database='project1_db')

mycursor=db.cursor()	


Customer_Detail=pd.read_csv(r'C:/Users/User/Desktop/SJSU Docs/DATA 225 DB System for Analytics/Project Data/Customer_Detail.csv', index_col=False, delimiter=',')
Customer_Detail.fillna(' ', inplace=True)

Customeraccount=pd.read_csv(r'C:/Users/User/Desktop/SJSU Docs/DATA 225 DB System for Analytics/Project Data/Customer_Account.csv', index_col=False, delimiter=',')
Customeraccount.fillna(' ', inplace=True)

Customeraddress=pd.read_csv(r'C:/Users/User/Desktop/SJSU Docs/DATA 225 DB System for Analytics/Project Data/Customer_Address.csv', index_col=False, delimiter=',')
Customeraddress.fillna(' ', inplace=True)

Customercard=pd.read_csv(r'C:/Users/User/Desktop/SJSU Docs/DATA 225 DB System for Analytics/Project Data/Customer_Card.csv', index_col=False, delimiter=',')
Customercard.fillna(' ', inplace=True)

Customerdisp=pd.read_csv(r'C:/Users/User/Desktop/SJSU Docs/DATA 225 DB System for Analytics/Project Data/Customer_Disposition.csv', index_col=False, delimiter=',')
Customerdisp.fillna(' ', inplace=True)

Customerdistrict=pd.read_csv(r'C:/Users/User/Desktop/SJSU Docs/DATA 225 DB System for Analytics/Project Data/Customer_District_data.csv', index_col=False, delimiter=',')
Customerdistrict.fillna(' ', inplace=True)


Customerloan=pd.read_csv(r'C:/Users/User/Desktop/SJSU Docs/DATA 225 DB System for Analytics/Project Data/Customer_Loan.csv', index_col=False, delimiter=',')
Customerloan.fillna(' ', inplace=True)

Customerorder=pd.read_csv(r'C:/Users/User/Desktop/SJSU Docs/DATA 225 DB System for Analytics/Project Data/Customer_Order.csv', index_col=False, delimiter=',')
Customerorder.fillna(' ', inplace=True)

Customertransaction=pd.read_csv(r'C:/Users/User/Desktop/SJSU Docs/DATA 225 DB System for Analytics/Project Data/Customer_Transaction.csv', index_col=False, delimiter=',')
Customertransaction.fillna(' ', inplace=True)


mycursor.execute("""CREATE TABLE project1_db.CustomerDetails (Clientid char(15) primary key, Sex varchar(15),
                 fulldate varchar(10), day varchar(2), month varchar(2), year varchar(4), 
                 age int, social char(15), FirstName varchar(50),  middlename varchar(50),
                    LastName varchar(50), Phone char(15), Email char(100))""")

db_query = """ INSERT INTO project1_db.CustomerDetails
                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) """
for i, row in Customer_Detail.iterrows(): 
    mycursor.execute(db_query, tuple(row))
db.commit();

mycursor.execute('CREATE TABLE project1_db.Customer_Account (AccountID char(20) primary key, DistrictID int, Frequency varchar(50), parseddate char(20), Year int, month int, Day int)')

for i, row in Customeraccount.iterrows():
    sql='INSERT INTO project1_db.Customer_Account VALUES (%s,%s,%s,%s,%s,%s,%s)'
    mycursor.execute(sql,tuple(row))
db.commit()

mycursor.execute('CREATE TABLE project1_db.Customer_Address (Clientid char(15) primary key, DistrictID int, Address_1 char(100), Address_2 char(100), City varchar(30), State varchar(5), Zipcode int)')
for i, row in Customeraddress.iterrows():
    sql='INSERT INTO project1_db.Customer_Address VALUES (%s,%s,%s,%s,%s,%s,%s)'
    mycursor.execute(sql,tuple(row))
    db.commit()
	
mycursor.execute('CREATE TABLE project1_db.Customer_card (card_id char(20) primary key, disp_id char(30), Type varchar(50),year char(6), month char(4), day char(3), Fulldate char(25))')
for i, row in Customercard.iterrows():
    sql='INSERT INTO project1_db.Customer_card VALUES (%s,%s,%s,%s,%s,%s,%s)'
    mycursor.execute(sql,tuple(row))
db.commit()

mycursor.execute('CREATE TABLE project1_db.Customer_Disposition (DispID char(30), Clientid char(15), AccountID char(20), Type varchar(30))')
for i, row in Customerdisp.iterrows():
    sql='INSERT INTO project1_db.Customer_Disposition VALUES (%s,%s,%s,%s)'
    mycursor.execute(sql,tuple(row))
db.commit()

mycursor.execute('CREATE TABLE project1_db.Customer_District (DistrictID int primary key, City varchar(50), State_name varchar(50), State varchar(5), Region varchar(30), Division varchar(75))')
for i, row in Customerdistrict.iterrows():
    sql='INSERT INTO project1_db.Customer_District VALUES (%s,%s,%s,%s,%s,%s)'
    mycursor.execute(sql,tuple(row))
db.commit()

mycursor.execute('CREATE TABLE project1_db.Customer_Loan (LoanID char(20) primary key, AccountID char(20), Amount decimal(10,4), Duration int, Payments int, Status varchar(2), year char(6) , month char (4), day char(4), Fulldate char(20), Location int, Purpose char(50))')
for i, row in Customerloan.iterrows():
    sql='INSERT INTO project1_db.Customer_Loan VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    mycursor.execute(sql,tuple(row))
db.commit()

mycursor.execute('CREATE TABLE project1_db.Customer_order (OrderID int primary key, AccountID char(20), Bank varchar(3), Account_to int, Amount decimal(10,4), K_Symbol varchar(50))')
for i, row in Customerorder.iterrows():
    sql='INSERT INTO project1_db.Customer_order VALUES (%s,%s,%s,%s,%s,%s)'
    mycursor.execute(sql,tuple(row))
db.commit()

mycursor.execute('CREATE TABLE project1_db.Customer_Transaction (SerialNumber int, Trans_id char(50), AccountID char(50), Type varchar(50), operation char(50), Amount decimal(12,4), Balance decimal(12,4), k_symbol char(50), bank char(50), account char(20), year char(6) , month char (4), day char(4),  Fulldate char(20), Fulltime char(50), fulldatewithtime char(75))')
for i, row in Customertransaction.iterrows():
    sql='INSERT INTO project1_db.Customer_Transaction VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    mycursor.execute(sql,tuple(row))
db.commit()

mycursor.execute('CREATE TABLE project1_db.Tran_Category (SerialNumber int, Trans_id char(50), AccountID char(50), Type varchar(50), operation char(50), Amount decimal(12,4), Balance decimal(12,4), k_symbol char(50), bank char(50), account char(20), year char(6) , month char (4), day char(4),Fulldate char(20), Fulltime char(50), fulldatewithtime char(75),transaction_category char (50))')
for i, row in TransactionCategory.iterrows():
    sql='INSERT INTO project1_db.Tran_Category VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    mycursor.execute(sql,tuple(row))
db.commit()
