/*
＜＜ゴールド会員動向の、累積登録UUを出すために使用しているクエリ＞＞

--ゴールド課金ユーザーテーブル（重複削除）
--register_dtの更新されたユーザーは、最新のものを取得（新規登録ユーザーとしてカウントする）

withdraw_dtから次月に出ているログは、ゴールド会員ではない!!


ver.1 20180516 ゴルド会委員テーブルのfreeユーザーを除外したものをローンチ
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
    register_dt,
    withdraw_dt,
    user_id
  from
    split_table
  where
    status like 'gold'
)
  select
    dt,
    register_dt,
    count(user_id) as accumulation_gold_uu
  from
    none_free_table
  group by
    dt,
    register_dt


