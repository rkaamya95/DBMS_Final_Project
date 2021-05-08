select json_object(
  'transaction serial num', SerialNumber
 , 'Trans_id',p.Trans_id 
 ,'AccountID',p.`AccountID`
 , 'tran_type', type
 , 'operation', operation
 , 'transaction amount', amount
 , 'running balance', balance
 , 'Date and Time',  fulldatewithtime
 , 'transaction category', tran_cat
 , 'Fraud_detection', fraud_flag
 ,'child_objects',json_array(
                     (select GROUP_CONCAT(
                                 json_object('clientid',clientid,'firstname',`firstname`, 'lastname',lastname,'primary phone', phone, 'primary email', email)
                             )   
                      from ( select accountid, cus.clientid	, firstname,	 lastname	, phone,	 email
 from Data225_final_project.customer_disposition cd 
inner join Data225_final_project.CustomerDetails cus on cus.clientid = cd.clientid) c
                      where p.AccountID = c.AccountID))
                   ) fraud_message
  from Data225_final_project.customer_transaction_fraud p where fraud_flag = 1