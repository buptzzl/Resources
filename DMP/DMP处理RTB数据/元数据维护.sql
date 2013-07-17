--放数
cd /dmp/tmp
hadoop fs -put D_CAMP2.csv /hive/warehouse/d_camp/
hadoop fs -put D_IDEA2.csv /hive/warehouse/d_idea/
hadoop fs -put D_INTR_CONS2.csv /hive/warehouse/d_intr_cons/
hadoop fs -put D_SITE2.csv /hive/warehouse/d_site/
hadoop fs -put ip_dstc.dat /hive/warehouse/p_ip_dstc_map/  -- IP 地域ID映射
hadoop fs -put P_DV_ID_CD_MAP2.csv /hive/warehouse/p_dv_id_cd_map/ -- 网站编码
hadoop fs -put P_INTR_CONS_MAP2.csv /hive/warehouse/p_intr_cons_map/
hadoop fs -put V_D_INTR_CONS2.csv /hive/warehouse/v_d_intr_cons/
hadoop fs -put V_P_DM_IC_MAP2.csv /hive/warehouse/v_p_dm_ic_map/  
 
-- 数据整理
drop table p_media_map;
create table p_media_map
as select site.plat_type, hash(site.url) as hash_url, ic.ic_type
    from d_site site join p_intr_cons_map ic on site.site_type = ic.orig_ic_type and site.plat_type = ic.plat_type and ic.class_id = 1;
	
drop table p_trade_map;    
create table p_trade_map
as     
select camp.plat_type, camp.camp_id, ic.ic_type
    from d_camp camp join p_intr_cons_map ic on camp.biz_type = ic.orig_ic_type and camp.plat_type = ic.plat_type and ic.class_id = 1;

-- 验证	
select * from d_camp limit 10;
select * from d_idea limit 10;
select * from d_intr_cons limit 10;
select * from d_site limit 10;
select * from p_ip_dstc_map limit 10;
select * from p_dv_id_cd_map limit 10;
select * from p_intr_cons_map limit 10;
select * from v_d_intr_cons limit 10;
select * from v_p_dm_ic_map limit 10;
select * from p_media_map limit 10;
select * from p_trade_map limit 10;