/*
＜＜ゴールド会員動向：UU＞＞

LTV用のゴールド会員UUを抽出している。
LTVなので、日付単位を月に設定し直している。

ver.1 20180516 ゴールド会員テーブルのfreeユーザーを除外したもの
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
), gold_uu_table as (
  select
    count(user_id) as gold_uu,
    date_trunc(dt, month) as month_dt,
    date_trunc(register_dt, month) as month_register_dt
  from
    none_free_table
  group by
    dt,
    register_dt
), gold_transition_table as (
  select
    month_dt,
    month_register_dt,
    sum(gold_uu) as gold_uu
  from
    gold_uu_table
  group by
    month_dt,
    month_register_dt
), add_lag_table as (
  select
    *,
    if(month_dt = month_register_dt,
      gold_uu,
      lag(gold_uu) over (partition by month_register_dt order by month_dt)) as lag_gold_uu
  from
    gold_transition_table
  order by month_register_dt, month_dt
)
  select
    *
  from
    add_lag_table