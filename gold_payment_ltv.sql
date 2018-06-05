/*
＜＜ゴールド会員動向：paynent＞＞

ゴールド会員の課金タイミングは、
１、ゴールド会員になったその瞬間に課金が発生する。
２、月初に課金が発生する。
３、一回、フリーに戻ったあとに再度ゴールド会員になる際にも課金が発生する。
（3/29にゴールド会員になれば、４月に入ってすぐにまた課金が発生する）



ver.1 20180516 ゴールド会員テーブルのfreeユーザーの除外したものをローンチ
ver.2 20180604 古橋さんMTG後の修正：ゴールド会員はwithdraw_dtの次の日からフリーになる。UU推移に変更あり。
*/


with gold_users_table as (  
  select
    dt,
    cast(register_dt as date) as register_dt,
    cast(
      if(withdraw_dt = 'NULL', 
        '2000-04-01',
        withdraw_dt) 
      as date) as withdraw_dt, 
    user_id
  from
     `patriot-999.stg_dokusho.satori_stg_user_gold_status_daily` 
  where
    _partitiontime >= "2018-03-30 00:00:00" and _partitiontime < '2018-05-31 00:00:00'
), split_table as (
  select distinct
    dt,
    register_dt,
    withdraw_dt,
    user_id,
    if(register_dt >= withdraw_dt,
      if(dt <> withdraw_dt,
        if(register_dt > withdraw_dt,
          'gold','free'),'gold'),
          if(dt = withdraw_dt,
        'gold',
      'free')) as status
  from
    gold_users_table
), none_free_table as (
  select
    dt,
    date_trunc(dt, month) as month_dt,
    register_dt,
    withdraw_dt,
    user_id
  from
    split_table
  where
    status like 'gold'
), gold_pay_table as (  
  select distinct
    user_id,
    if   
      (date_trunc(register_dt, month) =　date_trunc(dt, month),　　
      register_dt,
      date_trunc(date_add(register_dt, interval date_diff(dt, register_dt, month) month), month))
      as date_pay,
    case
      when user_id is not null then '300'
      else 'else' end payment
  from 
    none_free_table
), gold_pay_table2 as (　　
  select 
    date_pay,
    user_id,
    cast(payment as int64) as payment,
    case
      when user_id is not null then 'pay_gold'
      else 'else' end as pay_flag
  from
    gold_pay_table
), purchase_table as (  
  select
    user_id,
    dt as date_pay,
    sum(payment_amount) as payment,
    case
      when user_id is not null then 'pay_purchase'
      else 'else' end as pay_flag
  from
    `patriot-999.stg_dokusho.satori_stg_user_purchase_daily` 
  where
        _partitiontime >= "2018-03-30 00:00:00" and _partitiontime < '2018-05-09 00:00:00'
  group by
    dt, user_id
), purchase_gold_union_table as (  
  select
    l.user_id,
    l.date_pay,
    l.payment,
    l.pay_flag
  from
    gold_pay_table2 as l
  union all
  select
    r.user_id,
    r.date_pay,
    r.payment,
    r.pay_flag
  from
    purchase_table as r
), gold_premier_extration_table as (
  select
    l.*,
    r.user_id as gold_day
  from
    purchase_gold_union_table as l
  left join 
    none_free_table as r
  on l.user_id = r.user_id and l.date_pay = r.dt
), gold_free_table as (  
  select
    *,
    case
      when pay_flag  = 'pay_gold' then 'gold'
      when pay_flag  = 'pay_purchase' and gold_day is not null then 'gold-premier'
      else 'free' end user_flag
  from
    gold_premier_extration_table
), gold_free_table2 as (
  select
    date_pay,
    user_id,
    user_flag,
    sum(payment) as payment
  from
    gold_free_table
  group by 
    date_pay,
    user_id,
    user_flag 
), gold_premier_table as (
  select
    *
  from
    gold_free_table2
  where
    user_flag like 'gold_premier'
), add_register_table as (  
  select
    l.*,
    r.register_dt as register_dt
  from
    gold_premier_table as l
  left join
    none_free_table as r
  on l.user_id = r.user_id and l.date_pay = r.dt
), pay_day_table as (
  select
    date_pay,
    register_dt,
    sum(payment) as payment
  from
    add_register_table
  group by
    date_pay, register_dt
), pay_month_table as (
  select
    date_trunc(date_pay, month) as month_pay,
    date_trunc(register_dt, month) as month_register,
    payment
  from
    pay_day_table
), pay_month_slinking_table as (
  select
    month_pay,
    month_register,
    sum(payment) as payment
  from
    pay_month_table
  group by
    month_pay, month_register
), lag_table as (
  select
    *,
    if(month_pay = month_register,
      payment,
      lag(payment) over (partition by month_pay order by month_register)) as lag_payment
  from
    pay_month_slinking_table
)
  select
    *
  from
　　　lag_table