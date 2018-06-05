/*
＜＜プレミアメニュー動向に使用するクエリ＞＞


ver.1 
ver.2 20180531　古橋さんMTG後の修正。ゴールド会員は退会ボタンを押した次の日からフリー。


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
    register_dt,
    withdraw_dt,
    user_id
  from
    split_table
  where
    status like 'gold'
), purchase_table as (  
  select
    *,
    date_diff(dt, cast(release_date as date), day) as diff_release,
    case
      when date_trunc(dt, month) = date_trunc(cast(release_date as date), month) then 'new_release'
      else 'after_release' end as release
  from
    `patriot-999.stg_dokusho.satori_stg_user_purchase_daily` 
  where
    _partitiontime >= "2018-03-30 00:00:00" AND _partitiontime < current_timestamp()
), purchase_gold_table as (  
  select
    l.*,
    r.user_id as gold_user_id
  from
    purchase_table  as l
  left join
    none_free_table as r
  on l.user_id = r.user_id and l.dt = r.dt
), flag_table as (  
  select
    *,
    case
      when gold_user_id is not null and payment_amount > 0 then 'gold-premier'
      when gold_user_id is not null then 'gold'
      when gold_user_id is null then 'free'
    else 'else' end as user_flag
  from
    purchase_gold_table
), cluster_table as ( 
  select
    *
  from
    `patriot-999.stg_dokusho.satori_stg_user_cluster_daily`
  where
    _partitiontime >= "2018-03-30 00:00:00" AND _partitiontime < current_timestamp()
), add_cluster_table as (  
  select
    l.*,
    r.user_cluster
  from
    flag_table as l
  left join
    cluster_table as r
  on l.user_id = r.user_id and l.dt = r.dt
 ), pripare_table as (
   select
     *,
     case
       when diff_release >= 0 and diff_release < 7 then 'in_7days'
       when diff_release >= 7 and diff_release < 30 then 'in_30days'
       when diff_release >= 30 and diff_release < 60 then 'in_60days'
       when diff_release >= 60 then 'over_60days'
     else 'else' end as relese_period
   from
     add_cluster_table
)
  select
    *
  from
    pripare_table