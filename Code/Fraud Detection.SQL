# categorization logic - Mocking up data to add category to transactions like 
#['Gift Card', 'Online Purchase', 'Jewellery', 'Electronics', 'Remittance', 'OtherDebits'
# Query 1
alter table Data225_final_project.customer_transaction modify tran_cat char(40);
commit;
# Query 2 
## Adding transaction category in the Customer_transaction tables to get the better fraud detection. by categories.
UPDATE Data225_final_project.customer_transaction ctran
SET 
    tran_cat = (case when length(k_symbol) < 5 and ctran.Type = 'Debit' and operation = 'Cash Withdrawal' and amount between 2101 and 3568 then 'Gift Card'
	 when length(k_symbol) < 5 and ctran.Type = 'Debit' and operation = 'Cash Withdrawal' and amount between  300 and 1980 then 'Gift Card'
     when length(k_symbol) < 5 and ctran.Type = 'Debit' and operation = 'Cash Withdrawal' and amount between  2 and 9 then 'Online Purchase'
     when length(k_symbol) < 5 and ctran.Type = 'Debit' and operation = 'Cash Withdrawal' and amount between  101 and 108 then 'Online Purchase'
     when length(k_symbol) < 5 and ctran.Type = 'Debit' and operation = 'Cash Withdrawal' and amount between  10012 and 50012 then 'Jewellery'
     when length(k_symbol) < 5 and ctran.Type = 'Debit' and operation = 'Cash Withdrawal' and amount between  710 and 2000 then 'Electronics'
      when length(k_symbol) < 5 and ctran.Type = 'Debit' and operation = 'Cash Withdrawal' and amount > 50000 then 'Remittance'
     when ctran.Type = 'Credit' then 'Credit'
     when ctran.Type = 'Debit' then 'OtherDebits'
     else ' ' end);

######################******
#Query #3. - Adding categories to more transaction using Random function
UPDATE Data225_final_project.customer_transaction ctran
SET tran_cat = 
		ELT(0.5 + RAND() * 6, 'Gift Card', 'Online Purchase', 'Jewellery', 'Electronics', 'Remittance', 'OtherDebits')
        where ctran.Type = 'Debit' 
        
select tran_cat,Type, count(*) from  Data225_final_project.customer_transaction group by tran_cat,Type
commit;
#################################################################

#Query 4 Calculating Quartiles, MOnth Average
CREATE TABLE Data225_final_project.customer_transaction_QR AS (
select QR1.AccountID, QR1.year, QR1.month, QR1.Amount, QR1.ntile_grp, QR1.total, QR1.monthly_avg,
max(QR1.Amount) OVER (PARTITION BY QR1.AccountID, QR1.year, QR1.month, QR1.ntile_grp ) AS NTILE_MAX
FROM 
(SELECT ct.AccountID, ct.year, ct.month, ct.Amount,
NTILE(4) OVER (PARTITION BY ct.AccountID, ct.year, ct.month ORDER BY ct.Amount) ntile_grp,
           SUM(ct.Amount)  OVER (PARTITION BY AccountID, ct.year, ct.month) AS total,
           avg(ct.Amount)  OVER (PARTITION BY AccountID, ct.year, ct.month) AS monthly_avg
FROM Data225_final_project.customer_transaction ct) QR1 ) 

#Query 5  Calculating Minimum and Maximum with Q1, Q3 anfd IQR

create table Data225_final_project.customer_transaction_QR1 as (
select QR1.AccountID, QR1.year, QR1.month, QR1.total, QR1.monthly_avg, q1, q3,
q3-q1 as IQR, q1 - 1.5*(q3-q1) as low_q,  q3 + 1.5*(q3-q1) as high_q
from (
select distinct 
QR1.AccountID, QR1.year, QR1.month, QR1.total, QR1.monthly_avg, NTILE_MAX as q1
 from Data225_final_project.customer_transaction_QR QR1
 where QR1.ntile_grp  = 1 ) QR1
inner join (
select distinct 
QR3.AccountID, QR3.year, QR3.month, QR3.total, QR3.monthly_avg, NTILE_MAX as q3
 from Data225_final_project.customer_transaction_QR QR3
 where QR3.ntile_grp  = 3 ) QR3
 on QR1.AccountID = QR3.AccountID
 and QR1.year  = QR3.year 
 and QR1.month = QR3.month )
 
 #Query 6 , Finally flagging transactions as Fraud  which are outliers and falls outside the Minimum and Maximum
 # create fraud detection table. ************
 Create table Data225_final_project.customer_transaction_fraud as (
 #select tran_cat, count(*) from (
 select ctran.* , total, monthly_avg, q1, q3, IQR, low_q, high_q, 
 case when (Amount < low_q or Amount > high_q) and ctran.Type = 'Debit' then '1'
 else '0' end as fraud_flag
 from Data225_final_project.customer_transaction ctran
 inner join Data225_final_project.customer_transaction_QR1 QR1
 on ctran.AccountID = QR1.AccountID
 and ctran.year  = QR1.year 
 and ctran.month = QR1.month
 ) 
 
 Commit;

 select count(*) from Data225_final_project.customer_transaction_fraud
