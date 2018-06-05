/*
＜＜デジタル総合：課金(payment)＞＞

まず、ゴールド会員の正しい値を取得する。（freeユーザー存在する場合があるため）
その後に、課金テーブルとクラスターテーブルを結合している。

ver.1 20180515 ゴールド会員テーブルのfreeユーザーを除外
ver.2 20180516 ゴールド会員テーブルの修正版をローンチ
ver.3 20180531 古橋さんMTG後の修正。（ゴールド会員が退会ボタンを押した次の日からフリーになる）
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
), payment_user_split_table as (
  select
    *,
    case
      when dt = register_dt then 300
      when dt = month_dt then 300
    else 0 end as payment
  from
    none_free_table
), gold_payment_table as (
  select
    dt as date_pay,
    user_id,
    sum(payment) as payment,
    case
      when user_id is not null then 'pay_gold'
      else 'else' end pay_flag
  from
    payment_user_split_table
  where
    payment != 0
  group by
    date_pay,
    user_id,
    pay_flag
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
        _partitiontime >= "2018-03-30 00:00:00" AND _partitiontime < '2018-05-31 00:00:00'
  group by
    dt, user_id
), purchase_gold_union_table as (  
  select
    l.user_id,
    l.date_pay,
    l.payment,
    l.pay_flag
  from
    gold_payment_table as l
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
), cluster_table as ( 
  select
    *
  from
    `patriot-999.stg_dokusho.satori_stg_user_cluster_daily` 
  where
      _partitiontime >= "2018-03-30 00:00:00" and _partitiontime < '2018-05-31 00:00:00'
), add_cluster_table as (　
  select
    l.*,
    r.user_cluster
  from
    gold_free_table as l
  left join
    cluster_table as r
  on l.user_id = r.user_id and l.date_pay = r.dt
), adjust_table as (　
  select
   　date_pay,
    user_flag,
    user_cluster,
    sum(payment) as sum_payment,
    count(user_id) as pay_uu
  from
    add_cluster_table
  group by
    date_pay,
    user_flag,
    user_cluster
  order by date_pay, user_flag
)
  select
    *
  from
     adjust_table