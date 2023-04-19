REGISTER file:/usr/lib/pig/piggybank.jar;
DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVLoader;

-- Load data
Records = LOAD '$INPUT' using CSVLoader() AS(period_begin,period_end,period_duration,region_type,region_type_id,
table_id,is_seasonally_adjusted,region,city,state,state_code,property_type,property_type_id,median_sale_price,
median_sale_price_mom,median_sale_price_yoy,median_list_price,median_list_price_mom,median_list_price_yoy,median_ppsf,
median_ppsf_mom,median_ppsf_yoy,median_list_ppsf,median_list_ppsf_mom,median_list_ppsf_yoy,homes_sold,homes_sold_mom,
homes_sold_yoy,pending_sales,pending_sales_mom,pending_sales_yoy,new_listings,new_listings_mom,new_listings_yoy,inventory,
inventory_mom,inventory_yoy,months_of_supply,months_of_supply_mom,months_of_supply_yoy,median_dom,median_dom_mom,
median_dom_yoy,avg_sale_to_list,avg_sale_to_list_mom,avg_sale_to_list_yoy,sold_above_list,sold_above_list_mom,
sold_above_list_yoy,price_drops,price_drops_mom,price_drops_yoy,off_market_in_two_weeks,off_market_in_two_weeks_mom,
off_market_in_two_weeks_yoy,parent_metro_region,parent_metro_region_metro_code,last_updated);

-- Filter the data: (1) invalid (2) not single family home or not in 2021
Filtered = FILTER Records BY (median_sale_price != '');
Filtered = FILTER Filtered BY (property_type_id == '6' AND SUBSTRING(period_end, 0, 4) == '2021');

-- Group by state
state_sales = GROUP Filtered BY state_code;

-- Compute average median sale price for each state
state_median_sale_price = FOREACH state_sales GENERATE group AS state, AVG(Filtered.median_sale_price) AS new_price;

-- Sort by median sale price decreasing and get the top 10
top_10_states = ORDER state_median_sale_price BY new_price DESC;
top_10_states = LIMIT top_10_states 10;

STORE top_10_states INTO '$OUTPUT';