# multistream_dlt_pipeline

Easily manage and consume from diverse streaming platforms across multiple clouds through a single ETL pipeline.

1. Modify the JSON with requried credential and locaiton 
2. Attach the notebooks to your DLT pipeline
3. Execute and Monitor

# Schemas for different payloads

1. Admissions 

| # | Field Name    | Field Data type | Field Definition                                                                                       |
|---|---------------|-----------------|--------------------------------------------------------------------------------------------------------|
| 1 | admissions_id | string          | Auto generated GUID which uniquely identifies each admission record.                                   |
| 2 | hospital_id   | integer         | An integer which uniquely identifies a hospital. This refers to the hospital_id of the hospital table. |
| 3 | patient_id    | integer         | An integer which uniquely identifies a patient. This refers to the patient_id of the patient table.    |
| 4 | Timestamp     | timestamp       | A timestamp that specifies when the admission event was recorded in the source system.    

2. Vaccination

| # | Field Name       | Field Data type | Field Definition                                                                                       |
|---|------------------|-----------------|--------------------------------------------------------------------------------------------------------|
| 1 | hospital_id      | integer         | An integer which uniquely identifies a hospital. This refers to the hospital_id of the hospital table. |
| 3 | patient_id       | integer         | An integer which uniquely identifies a patient. This refers to the patient_id of the patient table.    |
| 4 | vaccination_type | string          | Type of vaccination given to the patient                                                               |
| 5 | Timestamp        | timestamp       | A timestamp that specifies when vaccination was given to the patient                                   |

3. Testing Records

| # | Field Name        | Field Data type | Field Definition                                                                                       |
|---|-------------------|-----------------|--------------------------------------------------------------------------------------------------------|
| 1 | testing_record_id | string          | A auto generated GUID which uniquely identifies each admission record.                                 |
| 2 | hospital_id       | integer         | An integer which uniquely identifies a hospital. This refers to the hospital_id of the hospital table. |
| 3 | patient_id        | integer         | An integer which uniquely identifies a patient. This refers to the patient_id of the patient table.    |
| 4 | is_positive       | string          | Did the laboratory test provide a positive (Y) or a negative (N) result?                               |
| 5 | Timestamp         | timestamp       | A timestamp that specifies when the laboratory test event was recorded in the source system.           |

4. Claims

| # | Field Name             | Field Data type | Field Definition                                                       |
|---|------------------------|-----------------|------------------------------------------------------------------------|
| 1 | admissions_id          | string          | A auto generated GUID which uniquely identifies each admission record. |
| 2 | claim_id               | string          | A string which uniquely identifies each claim that has been filed.     |
| 3 | claim_timestamp        | timestamp       | Stores timestamp at which the claim was submitted.                     |
| 4 | patient_payable_amount | double          | Amount payable by patient                                              |
| 5 | payer_covered_amount   | double          | Amount covered by Insurance                                            |
| 6 | total_invoiced_amount  | double          | Total amount invoiced                                                  |



