/*
＜＜ゴールド会員動向：新規会員獲得進捗＞＞


ver.1 20180516 ゴールド会員テーブルのfreeユーザーを除外したものローンチ
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
), cluster_table as (  
  select
    dt,
    user_id,
    user_cluster
  from
    `patriot-999.stg_dokusho.satori_stg_user_cluster_daily`
  where
    _partitiontime >= "2018-03-30 00:00:00" and _partitiontime < '2018-05-31 00:00:00'
), add_cluster_table as (
  select
    l.*,
    r.user_cluster
  from
    none_free_table as l
  left join 
    cluster_table as r
  on l.user_id = r.user_id and l.dt = r.dt
), trance_table as (
  select
    register_dt,
    user_id,
    case
      when user_cluster is null then '0_NO_LOGIN_GOLD'
      when user_cluster = '1_LIGHT' then '1_LIGHT'
      when user_cluster = '2_MIDDLE' then '2_MIDDLE'
      when user_cluster = '3_DEMOTE_HEAVY' then '3_DEMOTE_HEAVY'
      when user_cluster = '4_HEAVY' then '4_HEAVY'
      else 'else' end as user_cluster
  from
    add_cluster_table  
), table as (
  select
    register_dt,
    user_id,
    min(user_cluster) as user_cluster
  from
    trance_table
  group by
    user_id,
    register_dt
  order by 
    user_id,
    register_dt
)
  select
    register_dt,
    user_cluster,
    count(user_id) as new_gold_uu
  from
    table
  group by
    register_dt,
    user_cluster