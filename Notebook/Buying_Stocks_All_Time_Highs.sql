/*
 Title: Buying stocks at high price or any other price


 Purpose: 
   Analyze S&P stock market data from 1998-01-01 through 2020-08-26 
   and evaluate the performance of purchases after 1yr, 3yr, and 5yr.

   Segment the data:
     - Purchases when the stock is trading at an all-time high
     - Purchases on any day

   Based on a JP Morgan report suggesting that buying at all-time highs 
   outperforms buying on other days.

 Goal:
   Understand whether it makes more sense to purchase on days when the 
   market is at an all-time high versus waiting for a pull-back.
   Prevailing advice = avoid buying at all-time highs.
*/


/*
 Step 1: Create raw data table
*/
create table sp_stock_date (
    trade_date date,
    close_price decimal(8,2)
);


/*
 Step 2: Create parameters table (to avoid duplicating logic)
*/
drop table if exists data_params;

create table data_params as (
    select
        max(s.trade_date) - interval '1 year' as max_trade_dt_1yr_out,
        max(s.trade_date) - interval '3 year' as max_trade_dt_3yr_out,
        max(s.trade_date) - interval '5 year' as max_trade_dt_5yr_out
    from sp_stock_date s
);


/*
 Step 3: Capture the running high price up to each date
*/
drop table if exists trades_w_high_price_to_date;

create table trades_w_high_price_to_date as (
    select
        s.trade_date,
        s.close_price,
        max(s.close_price) over (
            order by s.trade_date
            rows between unbounded preceding and current row
        ) as max_close_price_to_date
    from sp_stock_date s
    order by s.trade_date
);


/*
 Step 4: Capture previous day’s high
*/
drop table if exists trades_w_prev_day_high_price;

create table trades_w_prev_day_high_price as (
    select
        t.trade_date,
        t.close_price,
        t.max_close_price_to_date,
        lag(t.max_close_price_to_date) over (order by t.trade_date) as prev_day_max_close_price_to_date
    from trades_w_high_price_to_date t
    order by t.trade_date
);


/*
 Step 5: Flag trades as new high vs not new high
*/
drop table if exists trades_w_prev_day_high_price_n_flag;

create table trades_w_prev_day_high_price_n_flag as (
    select
        t.trade_date,
        t.close_price,
        t.max_close_price_to_date,
        t.prev_day_max_close_price_to_date,
        case 
            when (t.max_close_price_to_date = t.prev_day_max_close_price_to_date) 
                then 'n' 
            else 'y'
        end as is_new_high_yn
    from trades_w_prev_day_high_price t
    order by t.trade_date
);


/*
 Step 6: Add 1yr, 3yr, 5yr forward trade dates
*/
drop table if exists trades_w_dates_yrs_out;

create table trades_w_dates_yrs_out as (
    select
        t.trade_date,
        t.close_price,
        t.max_close_price_to_date,
        t.prev_day_max_close_price_to_date,
        t.is_new_high_yn,
        (t.trade_date + interval '1 year') as trade_dt_plus_1yr,
        (t.trade_date + interval '3 year') as trade_dt_plus_3yr,
        (t.trade_date + interval '5 year') as trade_dt_plus_5yr,
        row_number() over(order by t.trade_date) as row_number
    from trades_w_prev_day_high_price_n_flag t
);


/*
 Step 7: Find row_numbers for 1yr-out dates
*/
drop table if exists trades_w_yr1_sale_date_row_num;

create table trades_w_yr1_sale_date_row_num as (
    select
        t.trade_date,
        t.close_price,
        t.max_close_price_to_date,
        t.prev_day_max_close_price_to_date,
        t.is_new_high_yn,
        t.trade_dt_plus_1yr,
        t.trade_dt_plus_3yr,
        t.trade_dt_plus_5yr,
        t.row_number,
        max(yr1.row_number) as yr1_row_number
    from trades_w_dates_yrs_out t
    left join trades_w_dates_yrs_out yr1
        on t.trade_dt_plus_1yr >= yr1.trade_date
       and (t.trade_dt_plus_1yr - interval '10 days') <= yr1.trade_date
    group by
        t.trade_date, t.close_price, t.max_close_price_to_date,
        t.prev_day_max_close_price_to_date, t.is_new_high_yn,
        t.trade_dt_plus_1yr, t.trade_dt_plus_3yr, t.trade_dt_plus_5yr,
        t.row_number
);


/*
 Step 8: Find row_numbers for 3yr-out dates
*/
drop table if exists trades_w_yr3_sale_date_row_num;

create table trades_w_yr3_sale_date_row_num as (
    select
        t.trade_date,
        t.close_price,
        t.max_close_price_to_date,
        t.prev_day_max_close_price_to_date,
        t.is_new_high_yn,
        t.trade_dt_plus_1yr,
        t.trade_dt_plus_3yr,
        t.trade_dt_plus_5yr,
        t.row_number,
        t.yr1_row_number,
        max(yr3.row_number) as yr3_row_number
    from trades_w_yr1_sale_date_row_num t
    left join trades_w_yr1_sale_date_row_num yr3
        on t.trade_dt_plus_3yr >= yr3.trade_date
       and (t.trade_dt_plus_3yr - interval '10 days') <= yr3.trade_date
    group by
        t.trade_date, t.close_price, t.max_close_price_to_date,
        t.prev_day_max_close_price_to_date, t.is_new_high_yn,
        t.trade_dt_plus_1yr, t.trade_dt_plus_3yr, t.trade_dt_plus_5yr,
        t.row_number, t.yr1_row_number
);


/*
 Step 9: Find row_numbers for 5yr-out dates
*/
drop table if exists trades_w_yr1_3_5_sale_date_row_num;

create table trades_w_yr1_3_5_sale_date_row_num as (
    select
        t.trade_date,
        t.close_price,
        t.max_close_price_to_date,
        t.prev_day_max_close_price_to_date,
        t.is_new_high_yn,
        t.trade_dt_plus_1yr,
        t.trade_dt_plus_3yr,
        t.trade_dt_plus_5yr,
        t.row_number,
        t.yr1_row_number,
        t.yr3_row_number,
        max(yr5.row_number) as yr5_row_number
    from trades_w_yr3_sale_date_row_num t
    left join trades_w_yr3_sale_date_row_num yr5
        on t.trade_dt_plus_5yr >= yr5.trade_date
       and (t.trade_dt_plus_5yr - interval '10 days') <= yr5.trade_date
    group by
        t.trade_date, t.close_price, t.max_close_price_to_date,
        t.prev_day_max_close_price_to_date, t.is_new_high_yn,
        t.trade_dt_plus_1yr, t.trade_dt_plus_3yr, t.trade_dt_plus_5yr,
        t.row_number, t.yr1_row_number, t.yr3_row_number
);


/*
 Step 10: Calculate returns (1yr, 3yr, 5yr) and clean invalid cases
*/
drop table if exists trades_w_return_perf;

create table trades_w_return_perf as (
    select
        t.trade_date,
        t.close_price,
        t.max_close_price_to_date,
        t.prev_day_max_close_price_to_date,
        t.is_new_high_yn,
        t.trade_dt_plus_1yr,
        t.trade_dt_plus_3yr,
        t.trade_dt_plus_5yr,
        t.row_number,
        t.yr1_row_number,
        t.yr3_row_number,
        t.yr5_row_number,

        -- yr1 results
        case when t.trade_date > (select d.max_trade_dt_1yr_out from data_params d)
            then null else yr1.close_price end as yr1_close_price,
        case when t.trade_date > (select d.max_trade_dt_1yr_out from data_params d)
            then null else (yr1.close_price - t.close_price) end as yr1_gain_dollars,
        case when t.trade_date > (select d.max_trade_dt_1yr_out from data_params d)
            then null else round((yr1.close_price - t.close_price) / t.close_price , 4) end as yr1_gain_perc,

        -- yr3 results
        case when t.trade_date > (select d.max_trade_dt_3yr_out from data_params d)
            then null else yr3.close_price end as yr3_close_price,
        case when t.trade_date > (select d.max_trade_dt_3yr_out from data_params d)
            then null else (yr3.close_price - t.close_price) end as yr3_gain_dollars,
        case when t.trade_date > (select d.max_trade_dt_3yr_out from data_params d)
            then null else round((yr3.close_price - t.close_price) / t.close_price , 4) end as yr3_gain_perc,

        -- yr5 results
        case when t.trade_date > (select d.max_trade_dt_5yr_out from data_params d)
            then null else yr5.close_price end as yr5_close_price,
        case when t.trade_date > (select d.max_trade_dt_5yr_out from data_params d)
            then null else (yr5.close_price - t.close_price) end as yr5_gain_dollars,
        case when t.trade_date > (select d.max_trade_dt_5yr_out from data_params d)
            then null else round((yr5.close_price - t.close_price) / t.close_price , 4) end as yr5_gain_perc
    from trades_w_yr1_3_5_sale_date_row_num t
    left join trades_w_yr1_3_5_sale_date_row_num yr1 on t.yr1_row_number = yr1.row_number
    left join trades_w_yr1_3_5_sale_date_row_num yr3 on t.yr3_row_number = yr3.row_number
    left join trades_w_yr1_3_5_sale_date_row_num yr5 on t.yr5_row_number = yr5.row_number
);


/*
 Step 11: Average performance - all trades vs new high trades
*/
select
    'all_trades' as segment,
    avg(t.yr1_gain_perc) as yr1_gain_perc,
    avg(t.yr3_gain_perc) as yr3_gain_perc,
    avg(t.yr5_gain_perc) as yr5_gain_perc
from trades_w_return_perf t

union all

select
    'new_high_trades' as segment,
    avg(t.yr1_gain_perc) as yr1_gain_perc,
    avg(t.yr3_gain_perc) as yr3_gain_perc,
    avg(t.yr5_gain_perc) as yr5_gain_perc
from trades_w_return_perf t
where t.is_new_high_yn = 'y';


/*
 Step 12: Summarize trade performance by year
*/
drop table if exists yearly_trade_performance;

create table yearly_trade_performance as (
    select 
        extract('year' from t.trade_date) as trade_year,
        count(1) as all_trades_ct,
        sum(case when t.is_new_high_yn = 'y' then 1 else 0 end) as new_high_trades_ct,

        -- year 1
        avg(t.yr1_gain_perc) as all_trades_yr1_gain_perc,
        avg(case when t.is_new_high_yn = 'y' then t.yr1_gain_perc end) as new_high_trades_yr1_gain_perc,

        -- year 3
        avg(t.yr3_gain_perc) as yr3_gain_perc,
        avg(case when t.is_new_high_yn = 'y' then t.yr3_gain_perc end) as new_high_trades_yr3_gain_perc,

        -- year 5
        avg(t.yr5_gain_perc) as yr5_gain_perc,
        avg(case when t.is_new_high_yn = 'y' then t.yr5_gain_perc end) as new_high_trades_yr5_gain_perc
    from trades_w_return_perf t
    group by trade_year
    order by trade_year
);


/*
 Step 13: Median performance (yr1, yr3, yr5)
*/
-- Year 1
drop table if exists yr1_gain_perc_medians;

create table yr1_gain_perc_medians as (
    select 'all_trades' as segment, max(p.yr1_gain_perc) as yr1_median_gain
    from (
        select t.trade_date, t.is_new_high_yn, t.yr1_gain_perc,
               percent_rank() over(order by t.yr1_gain_perc) as yr1_percentile
        from trades_w_return_perf t
    ) p 
    where p.yr1_percentile <= 0.50
    group by segment

    union all

    select 'new_high_trades' as segment, max(p.yr1_gain_perc) as yr1_median_gain
    from (
        select t.trade_date, t.is_new_high_yn, t.yr1_gain_perc,
               percent_rank() over(order by t.yr1_gain_perc) as yr1_percentile
        from trades_w_return_perf t
        where t.is_new_high_yn = 'y'
    ) p
    where p.yr1_percentile <= 0.50
    group by segment
);


-- Year 3
drop table if exists yr3_gain_perc_medians;

create table yr3_gain_perc_medians as (
    select 'all_trades' as segment, max(p.yr3_gain_perc) as yr3_median_gain
    from (
        select t.trade_date, t.is_new_high_yn, t.yr3_gain_perc,
               percent_rank() over(order by t.yr3_gain_perc) as yr3_percentile
        from trades_w_return_perf t
    ) p 
    where p.yr3_percentile <= 0.50
    group by segment

    union all

    select 'new_high_trades' as segment, max(p.yr3_gain_perc) as yr3_median_gain
    from (
        select t.trade_date, t.is_new_high_yn, t.yr3_gain_perc,
               percent_rank() over(order by t.yr3_gain_perc) as yr3_percentile
        from trades_w_return_perf t
        where t.is_new_high_yn = 'y'
    ) p
    where p.yr3_percentile <= 0.50
    group by segment
);


-- Year 5
drop table if exists yr5_gain_perc_medians;

create table yr5_gain_perc_medians as (
    select 'all_trades' as segment, max(p.yr5_gain_perc) as yr5_median_gain
    from (
        select t.trade_date, t.is_new_high_yn, t.yr5_gain_perc,
               percent_rank() over(order by t.yr5_gain_perc) as yr5_percentile
        from trades_w_return_perf t
    ) p 
    where p.yr5_percentile <= 0.50
    group by segment

    union all

    select 'new_high_trades' as segment, max(p.yr5_gain_perc) as yr5_median_gain
    from (
        select t.trade_date, t.is_new_high_yn, t.yr5_gain_perc,
               percent_rank() over(order by t.yr5_gain_perc) as yr5_percentile
        from trades_w_return_perf t
        where t.is_new_high_yn = 'y'
    ) p
    where p.yr5_percentile <= 0.50
    group by segment
);


/*
 Step 14: Combine medians (yr1, yr3, yr5)
*/
select
    yr1.segment,
    yr1.yr1_median_gain,
    yr3.yr3_median_gain,
    yr5.yr5_median_gain
from yr1_gain_perc_medians yr1
join yr3_gain_perc_medians yr3 on yr1.segment = yr3.segment
join yr5_gain_perc_medians yr5 on yr1.segment = yr5.segment;
