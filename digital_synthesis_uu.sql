/*
＜＜デジタル総合：UU＞＞

actionテーブルとゴールド会員テーブルを結合する。
そのあとに、クラスターテーブルと、スペンドテーブルを結合する。


ver.1 20180515 ゴールド会員テーブルのfreeユーザーを除外
ver.2 20180516 ゴールド会員テーブルの修正版をローンチ
ver.3 20180531 古橋さんMTG後の修正。（ゴールド会員は退会ボタンを押した時点でフリーになる）
*/



with action_table as (
select
  dt,
  action_type,
  user_id,
  sum(action_count) as count_action
from
  `patriot-999.stg_dokusho.satori_stg_user_action_daily` 
where
  _partitiontime >= "2018-03-30 00:00:00" AND _partitiontime < '2018-05-31 00:00:00'
group by dt, action_type, user_id
), gold_users_table as (  
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
), gold_action_table as (
  select 
   l.*,
   r.user_id as gold
  from
    action_table as l
  left outer join
    none_free_table as r
  on l.user_id = r.user_id and l.dt = r.dt
), free_table as (
 select
   dt,
   user_id,
   action_type,
   count_action,
   case
     when gold is null then 'free'
     when gold is not null then 'gold'
     else 'else' end as gold
 from
   gold_action_table
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
    case
      when r.user_cluster is null then '0_NO-LOGIN_GOLD'
      when r.user_cluster = '1_LIGHT' then '1_LIGHT'
      when r.user_cluster = '2_MIDDLE' then '2_MIDDLE'
      when r.user_cluster = '3_DEMOTE_HEAVY' then '3_DEMOTE_HEAVY'
      when r.user_cluster = '4_HEAVY' then '4_HEAVY'
      else 'else' end as user_cluster
  from
    free_table as l
  left join
    cluster_table as r
  on l.user_id = r.user_id and l.dt = r.dt
), spend_table as (
  select
    *
  from
    `patriot-999.stg_dokusho.satori_stg_user_spend_daily`
  where
    _partitiontime >= "2018-03-30 00:00:00" and _partitiontime < '2018-05-31 00:00:00'
), add_spend_table as (
  select
    l.*,
    sum(l.count_action) as action_count,
    sum(r.spend) as spend,
    sum(r.spend_coin) as spend_coin
  from
    add_cluster_table as l
  left join
    spend_table as r
  on l.user_id = r.user_id and l.dt = r.dt
  group by
    l.dt,
    l.user_id,
    l.action_type,
    l.gold,
    l.user_cluster,
    l.count_action
), get_ready_table as (
  select
    dt,
    action_type,
    gold as user_flag,
    user_cluster,
    count(distinct user_id) as action_uu,
    count(user_id) as action_uu_trial,
    sum(count_action) as sum_count_action,
    sum(spend) as spned,
    sum(spend_coin) as spend_coin
  from
    add_spend_table
  group by
    dt,
    action_type,
    gold,
    user_cluster
)
  select
    *
  from
    get_ready_table