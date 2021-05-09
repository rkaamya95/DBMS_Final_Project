## Finding out fraud transaction based on the transaction time, location id and transaction time zone ##

SELECT 
    *
FROM
    (
     SELECT 
        td.transaction_details_id AS '1st transaction',
            NULL AS '2nd transaction',
            td.transaction_time AS '1st transaction time',
            NULL AS '2nd transaction time',
            l.State AS '1st transaction state',
            NULL AS '2nd transaction state',
            l.Time_Zone AS '1st transaction time zone',
            NULL AS '2nd transaction time zone',
            'Y' AS 'flag_for_fraud'
    FROM
        transaction_details td
        INNER JOIN location l ON l.location_id = td.location_id
    WHERE
        td.transaction_time BETWEEN '00:00:00' AND '04:00:00'
     UNION 
     SELECT 
        td1.transaction_details_id AS '1st transaction',
            td2.transaction_details_id AS '2nd transaction',
            td1.transaction_time AS '1st transaction time',
            td2.transaction_time AS '2nd transaction time',
            l1.State AS '1st transaction state',
            l2.State AS '2nd transaction state',
            l1.Time_Zone AS '1st transaction time zone',
            l2.Time_Zone AS '2nd transaction time zone',
             td.min_diff,
            CASE
                WHEN TIMESTAMPDIFF(hour,td2.transaction_time,td1.transaction_time) < td.min_diff THEN 'Y'
                ELSE 'N'
            END AS 'flag_for_fraud'
    FROM
        transaction_details td1
    INNER JOIN transaction_details td2 ON td1.transaction_details_id = td2.transaction_details_id + 1
        AND td1.transaction_date = td2.transaction_date
    INNER JOIN location l1 ON l1.location_id = td1.location_id
    INNER JOIN location l2 ON l2.location_id = td2.location_id
    INNER JOIN time_difference td ON td.time_zone_1 = l1.time_zone
        AND td.time_zone_2 = l2.time_zone
    WHERE
        l1.time_zone != l2.time_zone) AS fraud_calculation
WHERE
    flag_for_fraud = 'Y';
	
	
#### Finding out fraud transaction based transaction category type ##


select * from (
SELECT ctran.* ,
case when length(k_symbol) < 5 and ctran.Type = 'Debit' and operation = 'Cash Withdrawal' and amount between 2101 and 3568 then 'Gift Card'
when length(k_symbol) < 5 and ctran.Type = 'Debit' and operation = 'Cash Withdrawal' and amount between  1016 and 1980 then 'Gift Card'
     when length(k_symbol) < 5 and ctran.Type = 'Debit' and operation = 'Cash Withdrawal' and amount between  2 and 9 then 'Online Purchase'
     when length(k_symbol) < 5 and ctran.Type = 'Debit' and operation = 'Cash Withdrawal' and amount between  101 and 108 then 'Online Purchase'
     when length(k_symbol) < 5 and ctran.Type = 'Debit' and operation = 'Cash Withdrawal' and amount between  10012 and 14012 then 'Jewellery'
     when length(k_symbol) < 5 and ctran.Type = 'Debit' and operation = 'Cash Withdrawal' and amount between  710 and 795 then 'Electronics'
     else ' ' end  as tran_cat
     FROM project1_db.customer_transaction ctran where length(k_symbol) < 5 and ctran.Type = 'Debit' and operation = 'Cash Withdrawal' ) b
     where length(tran_cat) > 2;

##Read data, create table and write data using python## 
TransactionCategory=pd.read_csv(r'C:/Users/User/Desktop/SJSU Docs/DATA 225 DB System for Analytics/Project Data/Transaction_Category.csv', index_col=False, delimiter=',')
TransactionCategory.fillna(' ', inplace=True)

mycursor.execute('CREATE TABLE project1_db.Tran_Category (SerialNumber int, Trans_id char(50), AccountID char(50), Type varchar(50), operation char(50), Amount decimal(12,4), Balance decimal(12,4), k_symbol char(50), bank char(50), account char(20), year char(6) , month char (4), day char(4),Fulldate char(20), Fulltime char(50), fulldatewithtime char(75),transaction_category char (50))')
for i, row in TransactionCategory.iterrows():
    sql='INSERT INTO project1_db.Tran_Category VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
    mycursor.execute(sql,tuple(row))
db.commit()


ALTER TABLE tran_category ADD is_fraud CHAR(50);

UPDATE TRAN_CATEGORY
SET IS_FRAUD = (
CASE WHEN TRANS_ID IN (
SELECT TRANS_ID FROM transaction_category WHERE FULLTIME BETWEEN '00:00:00' AND '05:00:00')
THEN 'YES'
ELSE 'NO'
END);

			       
#### Finding out the list of customers exceeding the payee limit per day ##

select a.from_accountID,c.FirstName,c.LastName,b.clientid,count(payee_name) as payee_count,a.Date_payee_added,if(count(payee_name) > 3,"Y","N") as Fraud_Flag from payee_records a 
inner join customer_disposition b on b.accountid=a.from_accountID 
inner join customer_details c on c.clientid=b.clientid where b.type = 'Owner'
group by a.from_accountID,a.Date_payee_added;
			       

#### Finding out the most vulnerable age group that are prone to Frauds ##
			       
select t.Age_group,t.FraudFlag, COUNT(t.FraudFlag) as Number_of_Frauds from (select a.AccountID,a.Amount,c.Age_group, if(Amount<10,"Y","N") as FraudFlag from customer_transaction a
inner  join  customer_disposition b  on a.AccountID=b.AccountID 
inner join customerdetails c on b.Clientid=c.Clientid) as t
where t.FraudFlag<>'N' and t.Age_group is not null
group by t.Age_group
			       
