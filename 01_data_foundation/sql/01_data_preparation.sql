-- Create a back up of the Original loaded records
CREATE TABLE customermind_ai.raw_customer_profiles_og AS 
SELECT * FROM customermind_ai.raw_customer_profiles;


-- Use to generate dummy records
CREATE OR REPLACE TABLE `customermind_ai.raw_customer_profiles_1B`
-- 🚨 CRITICAL: You MUST cluster a 1 Billion row table to prevent massive query costs
CLUSTER BY ID
AS
WITH multiplier AS (
  -- This creates an array of 500,000 numbers. 
  -- 2,000 original rows * 5,000 = 10,000,000 rows.
  SELECT step FROM UNNEST(GENERATE_ARRAY(1, 5000)) AS step
)
SELECT 
  -- Create a brand new, unique Customer ID
  CAST((m.step * 100000) + c.ID AS INT64) AS ID,
  
  -- Add +/- 5% random variance to Income 
  CAST(c.Income * (0.95 + (RAND() * 0.1)) AS FLOAT64) AS Income,

  -- Add +/- 3 days random variance to Recency
  CAST(c.Recency + CAST(ROUND((RAND() * 6) - 3) AS INT64) AS INT64) AS Recency,

  -- Copy the rest of the original data over identically
  c.* EXCEPT(ID, Income, Recency)

FROM `customermind_ai.raw_customer_profiles` c
CROSS JOIN multiplier m;

-- Inser the newly generated records into the raw table

INSERT INTO customermind_ai.raw_customer_profiles(ID
,Income
,Recency
,Year_Birth
,Education
,Marital_Status
,Kidhome
,Teenhome
,Dt_Customer
,MntWines
,MntFruits
,MntMeatProducts
,MntFishProducts
,MntSweetProducts
,MntGoldProds
,NumDealsPurchases
,NumWebPurchases
,NumCatalogPurchases
,NumStorePurchases
,NumWebVisitsMonth
,AcceptedCmp3
,AcceptedCmp4
,AcceptedCmp5
,AcceptedCmp1
,AcceptedCmp2
,Complain
,Z_CostContact
,Z_Revenue
,Response)
SELECT ID
,CAST(Income AS INT) AS Income
,Recency
,Year_Birth
,Education
,Marital_Status
,Kidhome
,Teenhome
,Dt_Customer
,MntWines
,MntFruits
,MntMeatProducts
,MntFishProducts
,MntSweetProducts
,MntGoldProds
,NumDealsPurchases
,NumWebPurchases
,NumCatalogPurchases
,NumStorePurchases
,NumWebVisitsMonth
,AcceptedCmp3
,AcceptedCmp4
,AcceptedCmp5
,AcceptedCmp1
,AcceptedCmp2
,Complain
,Z_CostContact
,Z_Revenue
,Response FROM customermind_ai.raw_customer_profiles_1B;

-- Create a clean version 

CREATE OR REPLACE TABLE `customermind_ai.raw_customer_profiles_clean`
CLUSTER BY ID
AS
SELECT 
ABS(FARM_FINGERPRINT(GENERATE_UUID())) AS ID
,CAST(Income AS INT) AS Income
,Recency
,Year_Birth
,Education
,Marital_Status
,Kidhome
,Teenhome
,Dt_Customer
,MntWines
,MntFruits
,MntMeatProducts
,MntFishProducts
,MntSweetProducts
,MntGoldProds
,NumDealsPurchases
,NumWebPurchases
,NumCatalogPurchases
,NumStorePurchases
,NumWebVisitsMonth
,AcceptedCmp3
,AcceptedCmp4
,AcceptedCmp5
,AcceptedCmp1
,AcceptedCmp2
,Complain
,Z_CostContact
,Z_Revenue
,Response FROM customermind_ai.raw_customer_profiles