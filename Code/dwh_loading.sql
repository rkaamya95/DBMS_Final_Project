
## 1. INsert statement for customer dimension.
INSERT into DATA225_DWH.DIM_CUSTOMER (CUSTOMER_ID,
GENDER, RELATIONSHIP_START_DATE,CUSTOMER_AGE,
SSN,
FIRST_NAME,MIDDLE_NAME,LAST_NAME,
PRIMARY_PHONE,PRIMARY_EMAIL,
CURRENT_FLG,
BEGIN_EFF_DATE,
END_EFF_DATE, CHECKSUM_NUM)  
SELECT `CustomerDetails`.`Clientid`,
	CASE WHEN Sex like 'M%' then 'M'
		 WHEN Sex like 'F%' then 'F'
         else 'O' end Sex,
    
   # `CustomerDetails`.`Sex`,
    `CustomerDetails`.`fulldate`,
    `CustomerDetails`.`age`,
    replace(`CustomerDetails`.`social`, '#','') social,
    `CustomerDetails`.`FirstName`,
    `CustomerDetails`.`middlename`,
    `CustomerDetails`.`LastName`,
    `CustomerDetails`.`Phone`,
    `CustomerDetails`.`Email`,
    'Y' CURRENT_FLG,
    fulldate as BEGIN_EFF_DATE,
    null as END_EFF_DATE,
    MD5(CONCAT(Clientid, Sex, fulldate,age, social,FirstName, middlename,LastName,Phone,Email))  as CHECKSUM_NUM
FROM `Data225_final_project`.`CustomerDetails`;


#2. Account begins
UPDATE Data225_final_project.Customer_Account 
SET 
    parseddate = REPLACE(parseddate, '-', '/');
    
#select * from DATA225_DWH.dim_account;

INSERT into DATA225_DWH.DIM_ACCOUNT
(`ACCOUNT_ID`, `LOAN_ID`, `ACCOUNT_OPEN_DATE`, `FREQUENCY`, `ACCOUNT_TYPE`,
 `ACCOUNT_STATUS`, `ACCOUNT_PURPOSE`, `ACCOUNT_LOCATION`, `CURRENT_FLG`, `BEGIN_EFF_DATE`, `END_EFF_DATE`, `CHECKSUM_NUM`)

SELECT `Customer_Account`.`AccountID`,
		`Customer_Loan`.`LoanID`,
        STR_TO_DATE(`Customer_Account`.`parseddate`, '%d/%m/%Y') parseddate1,
        #parseddate,
    `Customer_Account`.`Frequency`,
     'Loan' as ACCOUNT_TYPE,
     `Customer_Loan`.`Status`,
     `Customer_Loan`.`Purpose`,
     `Customer_Loan`.`Location`,
     'Y' CURRENT_FLG,
    STR_TO_DATE(`Customer_Account`.`parseddate`, '%d/%m/%Y') as BEGIN_EFF_DATE,
    null as END_EFF_DATE,
    MD5(CONCAT(Customer_Account.AccountID, parseddate,Frequency)) 
    as CHECKSUM_NUM
FROM `Data225_final_project`.`Customer_Account`
left outer join `Data225_final_project`.`Customer_Loan`
on `Customer_Account`.`AccountID` = `Customer_Loan`.`AccountID` 

commit;

#### Relationship table begins. ###
select * from data225_dwh.CUSTOMER_RLTD_ACCOUNT 
INSERT into data225_dwh.CUSTOMER_RLTD_ACCOUNT (customer_key, account_key, role_type,
CURRENT_FLG, BEGIN_EFF_DATE, END_EFF_DATE, CHECKSUM_NUM
)
select customer_key,account_key, CD.type,   
'Y' as CURRENT_FLG,              
    ACCOUNT_OPEN_DATE as BEGIN_EFF_DATE,
    null as END_EFF_DATE,
    MD5(CONCAT(Clientid, AccountID,cd.type)) 
 from Data225_final_project.Customer_Disposition CD
inner join DATA225_DWH.DIM_ACCOUNT DA on  CD.AccountID = DA.ACCOUNT_ID
inner join DATA225_DWH.DIM_CUSTOMER DC on CD.Clientid =  DC.CUSTOMER_ID

commit;

#### 4. Time dimension - LOADD VIA ONE TIME FILE UPLOAD
SELECT * FROM DATA225_DWH.DIM_TIME

## 5. LOADING FACT TRANSACTION TABLES
	INSERT into data225_dwh.FACT_TRANSACTION_DETAIL (
	DIM_TIME_KEY,TRAN_SERIAL_NUM, TRANS_ID,
    ACCOUNT_KEY, TRANS_TYPE, TRAN_AMOUNT, ACC_RUNNING_BALANCE, TRANS_DATE, TRANS_TIME,
    FRAUD_FLAG, TRAN_CAT, TIME_ZONEID
)
	SELECT 
    DIM_TIME_KEY,SERIALNUMBER,TRANS_ID,account_key,TYPE,
    AMOUNT,BALANCE,FULLDATE,FULLTIME,FRAUD_FLAG,TRAN_CAT,TIME_ZONE_ID
    FROM 	Data225_final_project.customer_transaction_fraud TF
    INNER JOIN DATA225_DWH.DIM_TIME ON RUN_DATE = FULLDATE
    inner join DATA225_DWH.DIM_ACCOUNT DA on  TF.AccountID = DA.ACCOUNT_ID
    
    
    
    
    