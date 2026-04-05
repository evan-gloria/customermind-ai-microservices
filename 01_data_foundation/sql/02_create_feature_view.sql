CREATE OR REPLACE VIEW `customermind_ai.v_customer_features` AS
SELECT 
  ID as CustomerID,
  (2024 - Year_Birth) AS Age,
  Income,
  Recency,
  (MntWines + MntFruits + MntMeatProducts + MntFishProducts + MntSweetProducts + MntGoldProds) AS Total_Spend,
  (NumWebPurchases + NumCatalogPurchases + NumStorePurchases) AS Total_Purchases,
  NumWebVisitsMonth,
  AcceptedCmp1 + AcceptedCmp2 + AcceptedCmp3 + AcceptedCmp4 + AcceptedCmp5 AS Total_Campaigns_Accepted
FROM `customermind_ai.raw_customer_profiles_clean`
WHERE Income IS NOT NULL;