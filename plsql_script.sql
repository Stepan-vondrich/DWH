CREATE OR REPLACE PACKAGE BODY FEST_MIG7 IS
  /******************************************************************************
     NAME:       FEST_MIG7
     PURPOSE:    Package for holding definitions of PL/SQL exceptions

     REVISIONS:
     Ver        Date        Author           Description
     ---------  ----------  ---------------  ------------------------------------
     1.0        11.12.2018   S.Vondrich       Package created
  ******************************************************************************/
  PROCEDURE WHITELIST(mig_wave IN NUMBER) IS
  BEGIN
    --WHITELIST

--  execute immediate 'truncate TABLE dm_com_lnk.fest_whitelist';

    UPDATE dm_com_lnk.fest_whitelist a SET a.mig_status_flg = 'N' WHERE a.mig_load_num = mig_wave;
    INSERT INTO dm_com_lnk.fest_whitelist
      (subscriber_id, sub_type, mig_status_flg, MIG_LOAD_NUM)
      SELECT c.val subscriber_id
            ,p.prod_num sub_type
            ,'Y' mig_status_flg
      ,mig_wave MIG_LOAD_NUM
        FROM prod_repo.prod p
        JOIN prod_repo.prod_char_val c
          ON p.ident = c.prod_ident
         AND p.prod_num = 'SUBI000'
         AND p.status <> 'inactive' --IN ('active', 'suspended')
         AND c.prod_spec_char_ident = 'subscriber_id'
       WHERE NOT EXISTS (SELECT v.val
                FROM prod_repo.prod_char_val v
               WHERE p.ident = v.prod_ident
                 AND v.prod_spec_char_ident = 'oss_provisioning')
         AND c.val NOT IN (SELECT /*+use_hash(a b) use_hash(b x) use_hash(a d)*/DISTINCT x.val -- open order validace
                             FROM ocv_repo.busn_inter_item a
                             JOIN ocv_repo.prod b
                               ON a.prod_ident = b.ident
                             JOIN ocv_repo.prod_char_val x
                               ON b.ident = x.prod_ident
                             JOIN ocv_repo.busn_inter d
                               ON a.busn_inter_ident = d.ident
                            WHERE x.prod_spec_char_ident = 'subscriber_id'
                              AND a.inter_status NOT IN ('completed', 'cancelled')
                              AND x.val IS NOT NULL); 

  UPDATE dm_com_lnk.fest_whitelist a
     SET a.mig_status_flg = 'N'
   WHERE a.subscriber_id IN
    (SELECT v1.val
       FROM prod_repo.prod_char_val v1
       JOIN prod_repo.prod p2
         ON p2.parent_ident = v1.prod_ident
       JOIN prod_repo.prod p3
         ON p3.parent_ident = p2.ident
       JOIN prod_repo.prod_char_val v2
         ON v2.prod_ident = p3.ident
        AND v2.prod_spec_char_ident = 'mmo_profile'
        AND p2.status = 'active'
      WHERE v1.prod_spec_char_ident = 'subscriber_id'
        AND DECODE(SUBSTR(v2.val, 1, 4), 'DSLA', 'ADSL', 'DSLV', 'VDSL', NULL) IS NOT NULL
        group by v1.val having count(*) > 1);
        
-- kontrola na konzistetní počet subů které se migrují
MERGE INTO dm_com_lnk.FEST_MIGRATION_PROGRESS A
    USING FEST_WHITELIST B ON (A.SUBSCRIBER_ID = B.SUBSCRIBER_ID)
WHEN NOT MATCHED THEN
    INSERT (A.SUBSCRIBER_ID, A.FEST_CFS_FLG, A.FEST_CFS_CHAR_FLG, A.FEST_CFS_SERVICE_POINT_FLG, A.FEST_CUSTOMER_FLG, A.CUSTOMER_ID)
    VALUES (B.SUBSCRIBER_ID,'N','N','N','N', '');

-- kontrola a vyřazení objednávek s open_order
  UPDATE dm_com_lnk.fest_whitelist a
     SET a.mig_status_flg = 'N'
   WHERE a.subscriber_id IN
       (SELECT DISTINCT cv.VAL subscriber_id
          FROM OCV_REPO.BUSN_INTER bi
          JOIN OCV_REPO.BUSN_INTER_ITEM i
            ON (bi.ident = i.BUSN_INTER_IDENT)
          JOIN ocv_repo.prod p
            ON (i.PROD_IDENT = p.ident)
          JOIN OCV_REPO.PROD_CHAR_VAL cv
            ON (p.ident = cv.prod_ident AND cv.PROD_SPEC_CHAR_IDENT = 'subscriber_id' AND
                bi.INTER_STATUS NOT IN ('completed', 'cancelled')))
     AND a.mig_load_num = mig_wave
;

-- kontrola na již zmigrované subi
 UPDATE dm_com_lnk.fest_whitelist a
     SET a.mig_status_flg = 'N'
   WHERE a.subscriber_id IN (select t.subscriber_id from FEST_MIGRATION_PROGRESS t where t.fest_cfs_flg = 'Y' OR t.fest_cfs_char_flg = 'Y' OR t.fest_cfs_service_point_flg = 'Y' OR t.FEST_CUSTOMER_FLG = 'Y');


  COMMIT;
/*
  EXCEPTION
    WHEN OTHERS THEN
    dbms_output.put_line(SQLERRM);
    ROLLBACK;
*/
  END WHITELIST;

  PROCEDURE MIGRATION(mig_wave IN NUMBER) IS
  BEGIN
    --
    -- Q2COM01
    --

--  execute immediate 'truncate TABLE dm_com_lnk.fest_cfs';
--  execute immediate 'truncate TABLE dm_com_lnk.fest_cfs_char';
--  execute immediate 'truncate TABLE dm_com_lnk.fest_cfs_service_point';
--  execute immediate 'truncate TABLE dm_com_lnk.fest_customer';
--  delete from fest_customer_names2@psbl01;
--  execute immediate 'truncate TABLE dm_com_lnk.FEST_S_ORG_EXT';
--  execute immediate 'truncate TABLE dm_com_lnk.FEST_S_CONTACT';
--  execute immediate 'truncate TABLE dm_com_lnk.fest_whitelist_check';
--  execute immediate 'truncate TABLE dm_com_lnk.fest_surrogate';


--  ALTER session SET global_names = FALSE;

    --prompt FWA fest_cfs
    INSERT INTO fest_cfs
      (NAME, service_instance_id, status, customer_id, product_code, service_description, mig_load_num)
      WITH fest_order AS
       (SELECT p.*
              ,p.ident prod_ident
              ,c.val subscriber_id
              ,r.REF_PARTY_IDENT cust_ident
              ,(SELECT s.name FROM upc_repo.prod_spec s WHERE s.prod_num = p.prod_num) service_description
          FROM prod_repo.prod p
          JOIN prod_repo.prod_char_val c
            ON p.ident = c.prod_ident
              --     AND p.prod_num = 'SUBI000'
              --     AND p.status IN ('active', 'suspended')
           AND c.prod_spec_char_ident = 'subscriber_id'
          JOIN PROD_REPO.FRGN_INVOLVE_ROLE r
            ON r.prod_ident = p.ident
           AND r.INVOLVE_ROLE = 'customer'
         WHERE NOT EXISTS (SELECT v.val
                  FROM prod_repo.prod_char_val v
                 WHERE p.ident = v.prod_ident
                   AND v.prod_spec_char_ident = 'oss_provisioning')
           AND c.val IN (SELECT f.subscriber_id FROM fest_whitelist f WHERE f.mig_status_flg = 'Y') -- whitelist
           --AND p.prod_num = 'SUBI000' --možná také
           AND p.status <> 'inactive')
      -- fest_cfs INTERNET_CHANNEL
      SELECT 'INTERNET_CHANNEL' NAME
            ,o.subscriber_id service_instance_id
            ,case o.status when 'suspended' then 'active' else o.status end status
            ,o.cust_ident customer_id
            ,o.prod_num product_code
            ,o.service_description
      ,mig_wave mig_load_num
        FROM fest_order o;

    --prompt FWA fest_cfs_char
    INSERT INTO fest_cfs_char
      (service_instance_id, NAME, VALUE, MIG_LOAD_NUM)
      WITH fest_order AS
       (SELECT p.*
              ,p.ident           prod_ident
              ,c.val             subscriber_id
              ,r.REF_PARTY_IDENT cust_ident
              ,x.mmo
              ,x.tarif
          FROM prod_repo.prod p
          JOIN prod_repo.prod_char_val c
            ON p.ident = c.prod_ident
           AND p.prod_num = 'SUBI000'
           AND p.status IN ('active', 'suspended')
           AND c.prod_spec_char_ident = 'subscriber_id'
          JOIN PROD_REPO.FRGN_INVOLVE_ROLE r
            ON r.prod_ident = p.ident
           AND r.INVOLVE_ROLE = 'customer'
          JOIN (SELECT v1.val
                     ,v2.val tarif
                     ,DECODE(SUBSTR(v2.val, 1, 4), 'DSLA', 'ADSL', 'DSLV', 'VDSL', NULL) mmo
                 FROM prod_repo.prod_char_val v1
                 JOIN prod_repo.prod p2
                   ON p2.parent_ident = v1.prod_ident
                 JOIN prod_repo.prod p3
                   ON p3.parent_ident = p2.ident
                 JOIN prod_repo.prod_char_val v2
                   ON v2.prod_ident = p3.ident
                  AND v2.prod_spec_char_ident = 'mmo_profile'
                  AND p2.status = 'active'
                WHERE v1.prod_spec_char_ident = 'subscriber_id'
                  AND DECODE(SUBSTR(v2.val, 1, 4), 'DSLA', 'ADSL', 'DSLV', 'VDSL', NULL) IS NOT NULL -- nevím jestli bych tu podmínku neměl vyhodit
               ) x
            ON x.val = c.val
         WHERE NOT EXISTS (SELECT v.val
                  FROM prod_repo.prod_char_val v
                 WHERE p.ident = v.prod_ident
                   AND v.prod_spec_char_ident = 'oss_provisioning')
           AND c.val IN (SELECT f.subscriber_id FROM fest_whitelist f WHERE f.mig_status_flg = 'Y')
           AND p.prod_num = 'SUBI000'
           AND p.status <> 'inactive')
      SELECT o.subscriber_id service_instance_id
            ,'Medium' NAME
            ,'Metallic' VALUE
      ,mig_wave MIG_LOAD_NUM
        FROM fest_order o
      UNION ALL
      SELECT o.subscriber_id service_instance_id
            ,'Service Provider' NAME
            ,'TMCZ' VALUE
      ,mig_wave MIG_LOAD_NUM
        FROM fest_order o
      UNION ALL
      SELECT o.subscriber_id service_instance_id
            ,'Channel Owner' NAME
            ,'CETIN' VALUE
      ,mig_wave MIG_LOAD_NUM
        FROM fest_order o
      UNION ALL
      SELECT o.subscriber_id service_instance_id
            ,'Technology Type' NAME
            ,o.mmo VALUE -- specifikovat
      ,mig_wave MIG_LOAD_NUM
        FROM fest_order o
      UNION ALL
      SELECT o.subscriber_id service_instance_id
            ,'Service Model' NAME
            ,'WS - MMO' VALUE
      ,mig_wave MIG_LOAD_NUM
        FROM fest_order o
      UNION ALL
      SELECT o.subscriber_id service_instance_id
            ,'Class of Service' NAME
            ,o.tarif VALUE
      ,mig_wave MIG_LOAD_NUM
        FROM fest_order o
       ORDER BY service_instance_id
               ,NAME;

    --prompt FWA fest_cfs_service_point
    INSERT INTO fest_cfs_service_point
      (service_instance_id, service_point_id, service_type, access_number,mig_load_num)
      WITH fest_order AS
       (SELECT p.*
              ,p.ident prod_ident
              ,c.val subscriber_id
              ,r.REF_PARTY_IDENT cust_ident
              ,i.ident item_id
              ,b.ident order_id
              ,b.INTER_DATE_COMPL
              ,'CZ-' || i.ident installation_address
          FROM prod_repo.prod p
          JOIN prod_repo.prod_char_val c
            ON p.ident = c.prod_ident
              --     AND p.prod_num = 'SUBI000'
              --     AND p.status IN ('active', 'suspended')
           AND c.prod_spec_char_ident = 'subscriber_id'
          JOIN PROD_REPO.FRGN_INVOLVE_ROLE r
            ON r.prod_ident = p.ident
           AND r.INVOLVE_ROLE = 'customer'
          JOIN com_repo.busn_inter_item i
            ON p.ident = i.prod_ident
           AND i.actn = 'add'
          JOIN com_repo.busn_inter b
            ON b.ident = i.BUSN_INTER_IDENT
        --   AND b.obj_id = 'CustomerProductOrder'
        --   AND b.inter_status IN ('completed')
         WHERE NOT EXISTS (SELECT v.val
                  FROM prod_repo.prod_char_val v
                 WHERE p.ident = v.prod_ident
                   AND v.prod_spec_char_ident = 'oss_provisioning')
           AND c.val IN (SELECT f.subscriber_id FROM fest_whitelist f WHERE f.mig_status_flg = 'Y')
           AND p.status <> 'inactive')
      -- fest_cfs_service_point
      SELECT subscriber_id service_instance_id
            ,(SELECT 'CZ-' || c.val
                FROM prod_repo.prod_char_val c
               WHERE c.prod_ident = o.prod_ident
                 AND c.prod_spec_char_ident = 'installation_address') service_point_id
            ,'Rented Metallic' service_type
            ,(SELECT 'CZ-' || c.val
                FROM prod_repo.prod_char_val c
               WHERE c.prod_ident = o.prod_ident
                 AND c.prod_spec_char_ident = 'installation_address') access_number
      ,mig_wave MIG_LOAD_NUM
        FROM fest_order o;

    --fest_customer
    INSERT INTO dm_com_lnk.fest_customer
      (customer_id, customer_code, account_type, mig_load_num)
      WITH fest_order AS
       (SELECT p.*
              ,p.ident            prod_ident
              ,c.val              subscriber_id
              ,r.REF_PARTY_IDENT  cust_ident
              ,r.REF_BUSN_IDENT   cust_number
              , -- tady jsem mel chybu pri migraci 28
               i.ident            item_id
              ,b.ident            order_id
              ,b.INTER_DATE_COMPL
          FROM prod_repo.prod p
          JOIN prod_repo.prod_char_val c
            ON p.ident = c.prod_ident
              --     AND p.prod_num = 'SUBI000'
              --     AND p.status IN ('active', 'suspended')
           AND c.prod_spec_char_ident = 'subscriber_id'
          JOIN PROD_REPO.FRGN_INVOLVE_ROLE r
            ON r.prod_ident = p.ident
        --   AND r.INVOLVE_ROLE = 'customer'
          JOIN com_repo.busn_inter_item i
            ON p.ident = i.prod_ident
           AND i.actn = 'add'
          JOIN com_repo.busn_inter b
            ON b.ident = i.BUSN_INTER_IDENT
        --   AND b.obj_id = 'CustomerProductOrder'
        --   AND b.inter_status IN ('completed')
         WHERE NOT EXISTS (SELECT v.val
                  FROM prod_repo.prod_char_val v
                 WHERE p.ident = v.prod_ident
                   AND v.prod_spec_char_ident = 'oss_provisioning')
           AND c.val IN (SELECT f.subscriber_id FROM fest_whitelist f WHERE f.mig_status_flg = 'Y')
           AND p.status <> 'inactive')
      SELECT DISTINCT o.cust_ident customer_id
                     ,o.cust_number customer_code
                     ,decode(substr(t.ACCNT_TYPE_CD, 1, 3), 'B2B', 'Business', 'B2C', 'Residential', '') account_type
           ,mig_wave MIG_LOAD_NUM
        FROM fest_order o
        JOIN s_org_ext t
          ON o.cust_ident = t.integration_id
        LEFT OUTER JOIN com_repo.order_ext_att e
          ON o.order_id = e.order_ident
         AND e.att_name = 'customerType';
     COMMIT;
/*
  EXCEPTION
    WHEN OTHERS THEN
    dbms_output.put_line(SQLERRM);
    ROLLBACK;
*/
  END MIGRATION;

  PROCEDURE WHITELIST_VALIDATE(mig_wave IN NUMBER) IS
  BEGIN

    --fest_customer_names
  execute immediate 'truncate table fest_error';
  execute immediate 'truncate table fest_whitelist_check';
  execute immediate 'truncate table FEST_SURROGATE';

MERGE INTO dm_com_lnk.FEST_CUSTOMER_NAMES2 A
USING FEST_CUSTOMER_NAMES2@psbl01 B ON (A.CUSTOMER_ID = B.CUSTOMER_ID AND A.MIG_LOAD_NUM = MIG_WAVE)
WHEN NOT MATCHED THEN
  INSERT (CUSTOMER_ID, FIRST_NAME, FAMILY_NAME, ACCOUNT_TYPE, CUSTOMER_CODE, MIG_STATUS_FLG, MIG_LOAD_NUM, CREATED, NAME)
  VALUES (B.CUSTOMER_ID, B.FIRST_NAME, B.FAMILY_NAME, B.ACCOUNT_TYPE, B.CUSTOMER_CODE, B.MIG_STATUS_FLG, B.MIG_LOAD_NUM, B.CREATED, B.NAME);
  
    INSERT INTO dm_com_lnk.FEST_ERROR
      (SUBSCRIBER_ID, table_name, "COLUMNS", "MIG_WAVE")
      SELECT f.service_instance_id
            ,'fest_cfs' table_name
            ,CASE
               WHEN f.status IS NULL THEN
                'status, '
             END || CASE
               WHEN f.customer_id IS NULL THEN
                'customer_id, '
             END "COLUMNS"
            ,mig_wave "MIG_WAVE"
        FROM dm_com_lnk.fest_cfs f
       WHERE (f.status IS NULL OR f.customer_id IS NULL)
         AND f.MIG_LOAD_NUM = mig_wave
          

      UNION

      SELECT a.service_instance_id
            ,'fest_cfs_char' table_name
            ,CASE
               WHEN a.name = 'Technology Type' AND a.value IS NULL THEN
                'Technology Type, '
             END || CASE
               WHEN a.name = 'Class of Service' AND a.value IS NULL THEN
                'Class of Service, '
             END "COLUMNS"
            ,mig_wave "MIG_WAVE"
        FROM dm_com_lnk.fest_cfs_char a
       WHERE a.name = 'Technology Type'
         AND (a.value IS NULL OR a.name = 'Class of Service')
         AND a.MIG_LOAD_NUM = mig_wave
         
      UNION

      SELECT d.service_instance_id
            ,'FEST_CUSTOMER_NAMES2' table_name
            ,CASE
               WHEN c.NAME IS NULL THEN
                'NAME, '
             END "COLUMNS"
            ,mig_wave "MIG_WAVE"
        FROM dm_com_lnk.FEST_CUSTOMER_NAMES2 c
        JOIN dm_com_lnk.fest_cfs d
          ON d.customer_id = c.customer_id
       WHERE (c.FIRST_NAME IS NULL OR c.FAMILY_NAME IS NULL OR c.NAME IS NULL)
         AND c.MIG_LOAD_NUM = mig_wave
         AND C.ACCOUNT_TYPE = 'Business'

      UNION

      SELECT d.service_instance_id
            ,'FEST_CUSTOMER_NAMES2' table_name
            ,CASE
               WHEN c.FIRST_NAME IS NULL THEN
                'FIRST_NAME, '
             END || CASE
               WHEN c.FAMILY_NAME IS NULL THEN
                'FAMILY_NAME, '
             END "COLUMNS"
            ,mig_wave "MIG_WAVE"
        FROM dm_com_lnk.FEST_CUSTOMER_NAMES2 c
        JOIN dm_com_lnk.fest_cfs d
          ON d.customer_id = c.customer_id
       WHERE (c.FIRST_NAME IS NULL OR c.FAMILY_NAME IS NULL)
         AND c.MIG_LOAD_NUM = mig_wave
         AND C.ACCOUNT_TYPE = 'Residential'

      UNION

      SELECT e.service_instance_id
            ,'fest_cfs_service_point' table_name
            ,CASE
               WHEN e.service_point_id = 'CZ-' THEN
                'service_point_id, '
             END || CASE
               WHEN e.access_number = 'CZ-' THEN
                'access_number, '
             END COLUMNS
            ,mig_wave "MIG_WAVE"
        FROM dm_com_lnk.FEST_CFS_SERVICE_POINT e
       WHERE (e.service_point_id = 'CZ-' OR e.access_number = 'CZ-')
         AND e.MIG_LOAD_NUM = mig_wave
       
       UNION
       
      SELECT e.service_instance_id
            ,'fest_cfs_service_point' table_name
            ,CASE
               WHEN e.service_point_id IS NULL THEN
                'service_point_id, '
             END || CASE
               WHEN e.access_number IS NULL THEN
                'access_number, '
             END COLUMNS
            ,mig_wave "MIG_WAVE"
        FROM dm_com_lnk.FEST_CFS_SERVICE_POINT e
       WHERE (e.service_point_id IS NULL OR e.access_number IS NULL)
         AND e.MIG_LOAD_NUM = mig_wave;

--  UPDATE dm_com_lnk.fest_whitelist a
--    SET a.mig_status_flg = 'N'
--    WHERE a.subscriber_id IN ( SELECT SUBSCRIBER_ID FROM FEST_ERROR) and a.mig_load_num = mig_wave;

commit;

-- druha kontrola
INSERT INTO fest_whitelist_check(subscriber_id_check, order_id_check, sub_type_check, mig_status_flg_check, mig_load_num_check, created_check) select subscriber_id, order_id, sub_type, mig_status_flg, mig_load_num, created from fest_whitelist abc where abc.mig_load_num = mig_wave;
insert into DM_COM_LNK.FEST_SURROGATE
select distinct (a.subscriber_id),
                 y.ref_party_ident
        from prod_repo.prod w
                    JOIN prod_repo.prod_char_val x
                      ON w.ident = x.prod_ident
                     AND x.prod_spec_char_ident = 'subscriber_id'
  JOIN PROD_REPO.FRGN_INVOLVE_ROLE y
    ON y.prod_ident = w.ident
   AND w.status = 'active'
   AND y.INVOLVE_ROLE = 'customer'
  RIGHT JOIN dm_com_lnk.fest_whitelist a
       ON a.subscriber_id = x.val
       WHERE w.status <> 'inactive'
         AND w.prod_num LIKE 'SUB%';

UPDATE (select * from dm_com_lnk.fest_whitelist_check a
      left join DM_COM_LNK.FEST_SURROGATE b
                  on a.subscriber_id_check = b.subscriber_id_surrogate) c
      SET c.customer_id_check = c.customer_id_surrogate;

UPDATE dm_com_lnk.fest_whitelist_check abc SET abc.MIG_STATUS_FLG_CHECK = 'X' WHERE abc.subscriber_id_check in
(SELECT a.service_instance_id subscriber_id
--       a.fest_cfs,
--       b.FEST_CFS_CHAR,
--       c.fest_cfs_service_point,
--       d.FEST_CUSTOMER_NAMES2
  FROM (SELECT a.service_instance_id, COUNT(*) fest_cfs
          FROM FEST_CFS a
         WHERE a.mig_load_num = mig_wave
         GROUP BY a.service_instance_id) a
  JOIN (SELECT a.service_instance_id, COUNT(*) FEST_CFS_CHAR
          FROM FEST_CFS_CHAR a
         WHERE a.mig_load_num = mig_wave
         GROUP BY a.service_instance_id) b
    ON a.service_instance_id = b.service_instance_id
  JOIN (SELECT a.service_instance_id, COUNT(*) fest_cfs_service_point
          FROM fest_cfs_service_point a
         WHERE a.mig_load_num = mig_wave
         GROUP BY a.service_instance_id) c
    ON a.service_instance_id = c.service_instance_id
  JOIN (SELECT a.subscriber_id_surrogate service_instance_id, COUNT(*) FEST_CUSTOMER_NAMES2
          FROM (SELECT b.subscriber_id_surrogate, a.*
                  FROM FEST_CUSTOMER_NAMES2 a
                  JOIN fest_surrogate b
                    ON a.customer_id = b.customer_id_surrogate) a
         WHERE a.mig_load_num = mig_wave
         GROUP BY a.subscriber_id_surrogate) d
    ON a.service_instance_id = d.service_instance_id
 WHERE a.fest_cfs <> 1
    OR b.FEST_CFS_CHAR <> 6
    OR c.fest_cfs_service_point <> 1
    OR d.FEST_CUSTOMER_NAMES2 <> 1);

commit;
END WHITELIST_VALIDATE;

PROCEDURE FLAG_MIGRATED(mig_wave IN NUMBER) IS
BEGIN

merge into dm_com_lnk.FEST_MIGRATION_PROGRESS va
 using
 (select * from dm_com_lnk.fest_surrogate v
           join dm_com_lnk.FEST_MIGRATION_PROGRESS s
           on v.subscriber_id_surrogate = s.subscriber_id and s.CUSTOMER_ID is null) a
           on (va.subscriber_id = a.subscriber_id_surrogate)
WHEN MATCHED THEN
   update set
          va.customer_id = a.customer_id_surrogate;
   
   UPDATE dm_com_lnk.FEST_MIGRATION_PROGRESS a
     SET a.FEST_CFS_FLG = 'Y'
   WHERE a.subscriber_id IN (select T.SERVICE_INSTANCE_ID from fest_mig_data.FEST_CFS@PMIG02 t where t.MIG_LOAD_NUM = mig_wave and t.mig_status_flg  = 'S');
   
   UPDATE dm_com_lnk.FEST_MIGRATION_PROGRESS a
     SET a.FEST_CFS_CHAR_FLG = 'Y'
   WHERE a.subscriber_id IN (select T.SERVICE_INSTANCE_ID from fest_mig_data.FEST_CFS_CHAR@PMIG02 t where t.MIG_LOAD_NUM = mig_wave and t.mig_status_flg = 'S');
   
   UPDATE dm_com_lnk.FEST_MIGRATION_PROGRESS a
     SET a.FEST_CFS_SERVICE_POINT_FLG = 'Y'
   WHERE a.subscriber_id IN (select T.SERVICE_INSTANCE_ID from fest_mig_data.FEST_CFS_SERVICE_POINT@PMIG02 t where t.MIG_LOAD_NUM = mig_wave and t.mig_status_flg = 'S');
   
   UPDATE dm_com_lnk.FEST_MIGRATION_PROGRESS a
     SET a.FEST_CUSTOMER_FLG = 'Y'
   WHERE a.customer_id IN (select T.CUSTOMER_ID from fest_mig_data.FEST_CUSTOMER@PMIG02 t where t.MIG_LOAD_NUM = mig_wave and t.mig_status_flg = 'S');

END FLAG_MIGRATED;
END;
