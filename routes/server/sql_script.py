def gen_sql():
    sql = """
        WITH tmp AS (
            SELECT serial_number,dcm_date,dcm_date_time,test_start_date_time,batch_date_time,batch_date,startdt,pass_fail,
            dcm_moba_code,dcm_line_id,pn_capacity,test_type,contextid,head_id,radius_id,dcm_eval_code,
            hiest_rro_frq_1,hiest_rro_frq_2,hiest_rro_frq_3,hiest_rro_frq_4,hiest_rro_frq_5,
            hiest_rro_mag_1,hiest_rro_mag_2,hiest_rro_mag_3,hiest_rro_mag_4,hiest_rro_mag_5,
            CASE WHEN hiest_rro_mag_1>hiest_rro_mag_2 and hiest_rro_mag_1>hiest_rro_mag_3 and hiest_rro_mag_1>hiest_rro_mag_4 and hiest_rro_mag_1>hiest_rro_mag_5 THEN hiest_rro_mag_1
                WHEN hiest_rro_mag_2>hiest_rro_mag_1 and hiest_rro_mag_2>hiest_rro_mag_3 and hiest_rro_mag_2>hiest_rro_mag_4 and hiest_rro_mag_2>hiest_rro_mag_5 THEN hiest_rro_mag_2
                WHEN hiest_rro_mag_3>hiest_rro_mag_1 and hiest_rro_mag_3>hiest_rro_mag_2 and hiest_rro_mag_3>hiest_rro_mag_4 and hiest_rro_mag_3>hiest_rro_mag_5 THEN hiest_rro_mag_3
                WHEN hiest_rro_mag_4>hiest_rro_mag_1 and hiest_rro_mag_4>hiest_rro_mag_2 and hiest_rro_mag_4>hiest_rro_mag_3 and hiest_rro_mag_4>hiest_rro_mag_5 THEN hiest_rro_mag_4
                WHEN hiest_rro_mag_5 >hiest_rro_mag_1 and hiest_rro_mag_5>hiest_rro_mag_2 and hiest_rro_mag_5>hiest_rro_mag_3 and hiest_rro_mag_5>hiest_rro_mag_4 THEN hiest_rro_mag_5
                END AS Maxmag_20xTo30x,failcount_PE1,groups,tester_id,testerID,groupMBA
        FROM(
                SELECT xsm.serial_number,
                    xsm.test_start_date_time,
                    xsm.dcm_date_time,
                    xsm.dcm_date,
                    xsm.batch_date_time,
                    xsm.batch_date,
                    xsm.startdt,
                    xsm.pass_fail,
                    xsm.tester_id,
                    xsm.dcm_moba_code,
                    xsm.dcm_line_id,
                    xsm.pn_capacity,
                    xrd.test_type,
                    xrd.contextid,
                    xrd.head_id,
                    xrd.radius_id,
                    xrd.dcm_eval_code,
                    xrd.hiest_rro_frq_1,
                    xrd.hiest_rro_frq_2,
                    xrd.hiest_rro_frq_3,
                    xrd.hiest_rro_frq_4,
                    xrd.hiest_rro_frq_5,
                    xrd.hiest_rro_mag_1/(84000.0*4) AS hiest_rro_mag_1,
                    xrd.hiest_rro_mag_2/(84000.0*4) AS hiest_rro_mag_2,
                    xrd.hiest_rro_mag_3/(84000.0*4) AS hiest_rro_mag_3,
                    xrd.hiest_rro_mag_4/(84000.0*4) AS hiest_rro_mag_4,
                    xrd.hiest_rro_mag_5/(84000.0*4) AS hiest_rro_mag_5,
                    case when xrd.hiest_rro_frq_1 between 24 and 29 and xrd.hiest_rro_mag_1 > 700000 then 1 else 0 end as failcount_PE1,
                    case when xsm.serial_number in ('WX12A70D05NS','WX12A70D0KD3','WXE2A709S70E','WXE2A709S85A','WXE2A709S907','WXE2A709S99Y','WXE2A709S9J5','WXE2A709SC04','WXE2A709SD3X'
                                                        ,'WXE2A709SFY6','WXE2A709SKLR','WXE2A709SNCZ','WXE2A709SNN8','WXE2A709SPFH','WXE2A709ST87','WXE2A709SVCK','WXE2A70RYTFV','WXE2A80E0AA8'
                                                        ,'WXE2A80E0FJ2','WXE2A80E0J0U','WXK2A80575LD','WXL2A801HCYU','WXL2A801HDRL','WXL2A801HF6Z','WXL2A801HRLD','WXL2A801HSUS','WXL2A801HUT2'
                                                        ,'WXL2A801HV7E','WXL2A801HV7K','WXL2A801HZSN') then 'OQA glist'
                            else 'passer' end as groups,
                    case when substring(xsm.tester_id,1,2) = 'EB' then 'Neptune' when substring(xsm.tester_id,1,2) = 'PB' then 'Optimus' end as testerID,
                    case when xsm.dcm_moba_code in ('6','S') then 'Nidec' when xsm.dcm_moba_code = '9' then 'PMDM' else 'NA' end as groupMBA
                FROM (
                    (SELECT serial_number,
                            product_name,
                            test_start_date_time,
                            dcm_date_time,
                            dcm_date,
                            batch_date_time,
                            batch_date,
                            startdt,
                            test_type,
                            tester_id,
                            pass_fail,
                            family_id,
                            dcm_moba_code,
                            dcm_line_id,
                            pn_capacity
                    FROM xmms.vwb_epbe_summary
                    WHERE product IN ('palmer72')
                    AND   site IN ('wdb4')
                    --AND   startdt BETWEEN '20201110' AND '20201122'
                    AND startdt BETWEEN DATE_FORMAT(DATE_ADD('month',-3,CURRENT_DATE),'%Y%m%d') AND DATE_FORMAT(CURRENT_DATE,'%Y%m%d')
                    AND   pass_fail = 'P'
                    --AND test_type = 764
                    --AND batch_date_time in (SELECT distinct MAX(batch_date_time) over(partition by serial_number) from xmms.vwb_epbe_summary WHERE product IN ('palmer72') and serial_number  in('WXD2A40DL48F','WXE2A709SC04'))                                
                    --AND serial_number in('WXN2A80178HN','WX92E60ED3WN','WXE2A70RY51E')
                    ) AS xsm
                INNER join
                    (SELECT serial_number,
                            startdt,
                            test_start_date_time,
                            test_type,
                            contextid,
                            test_phase_id,
                            head_id,
                            radius_id,
                            dcm_eval_code,
                            site,
                            (case when hiest_rro_frq_1 between 20 and 30 THEN hiest_rro_frq_1 ELSE 0 END) AS hiest_rro_frq_1,
                            (case when hiest_rro_frq_2 between 20 and 30 THEN hiest_rro_frq_2 ELSE 0 END) AS hiest_rro_frq_2,
                            (case when hiest_rro_frq_3 between 20 and 30 THEN hiest_rro_frq_3 ELSE 0 END) AS hiest_rro_frq_3,
                            (case when hiest_rro_frq_4 between 20 and 30 THEN hiest_rro_frq_4 ELSE 0 END) AS hiest_rro_frq_4,
                            (case when hiest_rro_frq_5 between 20 and 30 THEN hiest_rro_frq_5 ELSE 0 END) AS hiest_rro_frq_5,
                            (case when hiest_rro_frq_1 between 20 and 30 THEN hiest_rro_mag_1 ELSE 0 END) AS hiest_rro_mag_1,
                            (case when hiest_rro_frq_2 between 20 and 30 THEN hiest_rro_mag_2 ELSE 0 END) AS hiest_rro_mag_2,
                            (case when hiest_rro_frq_3 between 20 and 30 THEN hiest_rro_mag_3 ELSE 0 END) AS hiest_rro_mag_3,
                            (case when hiest_rro_frq_4 between 20 and 30 THEN hiest_rro_mag_4 ELSE 0 END) AS hiest_rro_mag_4,
                            (case when hiest_rro_frq_5 between 20 and 30 THEN hiest_rro_mag_5 ELSE 0 END) AS hiest_rro_mag_5
                    FROM xmms.vwb_epbe_radius
                    WHERE product IN ('palmer72')
                    --AND   startdt BETWEEN '20201110' AND '20201122'
                    AND startdt BETWEEN DATE_FORMAT(DATE_ADD('month',-3,CURRENT_DATE),'%Y%m%d') AND DATE_FORMAT(CURRENT_DATE,'%Y%m%d')
                    AND   test_type IN (764,762)
                    AND   contextid IN (55,54)
                    AND hiest_rro_frq_1 is not null
                    --AND serial_number in('WXN2A80178HN','WX92E60ED3WN','WXE2A70RY51E')
                    ) AS xrd
                ON xsm.serial_number = trim(xrd.serial_number)
                AND xsm.test_start_date_time = xrd.test_start_date_time
                AND xsm.test_type = xrd.test_type
                )
            UNION ALL
                SELECT xsm.serial_number,
                    xsm.test_start_date_time,
                    xsm.dcm_date_time,
                    xsm.dcm_date,
                    xsm.batch_date_time,
                    xsm.batch_date,
                    xsm.startdt,
                    xsm.pass_fail,
                    xsm.tester_id,
                    xsm.dcm_moba_code,
                    xsm.dcm_line_id,
                    xsm.pn_capacity,
                    xrd.test_type,
                    xrd.contextid,
                    xrd.head_id,
                    xrd.radius_id,
                    xrd.dcm_eval_code,
                    xrd.hiest_rro_frq_1,
                    xrd.hiest_rro_frq_2,
                    xrd.hiest_rro_frq_3,
                    xrd.hiest_rro_frq_4,
                    xrd.hiest_rro_frq_5,
                    xrd.hiest_rro_mag_1/(84000.0*4) AS hiest_rro_mag_1,
                    xrd.hiest_rro_mag_2/(84000.0*4) AS hiest_rro_mag_2,
                    xrd.hiest_rro_mag_3/(84000.0*4) AS hiest_rro_mag_3,
                    xrd.hiest_rro_mag_4/(84000.0*4) AS hiest_rro_mag_4,
                    xrd.hiest_rro_mag_5/(84000.0*4) AS hiest_rro_mag_5,
                    case when xrd.hiest_rro_frq_1 between 24 and 29 and xrd.hiest_rro_mag_1 > 700000 then 1 else 0 end as failcount_PE1,
                    case when xsm.serial_number in ('WX12A70D05NS','WX12A70D0KD3','WXE2A709S70E','WXE2A709S85A','WXE2A709S907','WXE2A709S99Y','WXE2A709S9J5','WXE2A709SC04','WXE2A709SD3X'
                                                        ,'WXE2A709SFY6','WXE2A709SKLR','WXE2A709SNCZ','WXE2A709SNN8','WXE2A709SPFH','WXE2A709ST87','WXE2A709SVCK','WXE2A70RYTFV','WXE2A80E0AA8'
                                                        ,'WXE2A80E0FJ2','WXE2A80E0J0U','WXK2A80575LD','WXL2A801HCYU','WXL2A801HDRL','WXL2A801HF6Z','WXL2A801HRLD','WXL2A801HSUS','WXL2A801HUT2'
                                                        ,'WXL2A801HV7E','WXL2A801HV7K','WXL2A801HZSN') then 'OQA glist'
                            else 'passer' end as groups,
                    case when substring(xsm.tester_id,1,2) = 'EB' then 'Neptune' when substring(xsm.tester_id,1,2) = 'PB' then 'Optimus' end as testerID,
                    case when xsm.dcm_moba_code in ('6','S') then 'Nidec' when xsm.dcm_moba_code = '9' then 'PMDM' else 'NA' end as groupMBA
                FROM (
                    (SELECT serial_number,
                            product_name,
                            test_start_date_time,
                            dcm_date_time,
                            dcm_date,
                            batch_date_time,
                            batch_date,
                            startdt,
                            test_type,
                            tester_id,
                            pass_fail,
                            family_id,
                            dcm_moba_code,
                            dcm_line_id,
                            pn_capacity
                    FROM xmms.vwb_xpbe_summary
                    WHERE product IN ('palmer72')
                    AND   site IN ('wdb4')
                    --AND   startdt BETWEEN '20201110' AND '20201122'
                    AND startdt BETWEEN DATE_FORMAT(DATE_ADD('month',-3,CURRENT_DATE),'%Y%m%d') AND DATE_FORMAT(CURRENT_DATE,'%Y%m%d')
                    AND   pass_fail = 'P'
                    --AND test_type = 764
                    --AND batch_date_time in (SELECT distinct MAX(batch_date_time) over(partition by serial_number) from xmms.vwb_epbe_summary WHERE product IN ('palmer72') and serial_number  in('WXD2A40DL48F','WXE2A709SC04'))                                
                    --AND serial_number in('WXN2A80178HN','WX92E60ED3WN','WXE2A70RY51E')
                    ) AS xsm
                INNER join
                    (SELECT serial_number,
                            startdt,
                            test_start_date_time,
                            test_type,
                            contextid,
                            test_phase_id,
                            head_id,
                            radius_id,
                            dcm_eval_code,
                            site,
                            (case when hiest_rro_frq_1 between 20 and 30 THEN hiest_rro_frq_1 ELSE 0 END) AS hiest_rro_frq_1,
                            (case when hiest_rro_frq_2 between 20 and 30 THEN hiest_rro_frq_2 ELSE 0 END) AS hiest_rro_frq_2,
                            (case when hiest_rro_frq_3 between 20 and 30 THEN hiest_rro_frq_3 ELSE 0 END) AS hiest_rro_frq_3,
                            (case when hiest_rro_frq_4 between 20 and 30 THEN hiest_rro_frq_4 ELSE 0 END) AS hiest_rro_frq_4,
                            (case when hiest_rro_frq_5 between 20 and 30 THEN hiest_rro_frq_5 ELSE 0 END) AS hiest_rro_frq_5,
                            (case when hiest_rro_frq_1 between 20 and 30 THEN hiest_rro_mag_1 ELSE 0 END) AS hiest_rro_mag_1,
                            (case when hiest_rro_frq_2 between 20 and 30 THEN hiest_rro_mag_2 ELSE 0 END) AS hiest_rro_mag_2,
                            (case when hiest_rro_frq_3 between 20 and 30 THEN hiest_rro_mag_3 ELSE 0 END) AS hiest_rro_mag_3,
                            (case when hiest_rro_frq_4 between 20 and 30 THEN hiest_rro_mag_4 ELSE 0 END) AS hiest_rro_mag_4,
                            (case when hiest_rro_frq_5 between 20 and 30 THEN hiest_rro_mag_5 ELSE 0 END) AS hiest_rro_mag_5
                    FROM xmms.vwb_xpbe_radius
                    WHERE product IN ('palmer72')
                    --AND   startdt BETWEEN '20201110' AND '20201122'
                    AND startdt BETWEEN DATE_FORMAT(DATE_ADD('month',-3,CURRENT_DATE),'%Y%m%d') AND DATE_FORMAT(CURRENT_DATE,'%Y%m%d')
                    AND   test_type IN (764,762)
                    AND   contextid IN (55,54)
                    AND hiest_rro_frq_1 is not null
                    --AND serial_number in('WXN2A80178HN','WX92E60ED3WN','WXE2A70RY51E')
                    ) AS xrd
                ON xsm.serial_number = trim(xrd.serial_number)
                AND xsm.test_start_date_time = xrd.test_start_date_time
                AND xsm.test_type = xrd.test_type
                )
        )

        )
        select dcm_date,dcm_line_id||'_'||groupMBA||'_'||pn_capacity AS CRNO_MBA_CAP
        ,count(*) as Total_Qty
        --,sum(failcount_PElimit) as failcount_limit1
        --,sum(failcount_PElimit) *100.0/count(*) as failrate_limit1
        --,sum(failcount_FEALimit) as failcount_limit2
        --,sum(failcount_FEALimit) *100.0/count(*) as failrate_limit2
        --,sum(failcount_FEA2Limit) as failcount_limit3
        --,sum(failcount_FEA2Limit) *100.0/count(*) as failrate_limit3
        --,sum(case when failcount_PElimit = 1 or failcount_FEALimit = 1 then 1 else 0 end) as failcount_limit3
        --,sum(case when failcount_PElimit = 1 or failcount_FEALimit = 1 then 1 else 0 end) *100.0/count(*) as failrate_limit3
        
        ,ROUND(AVG(failcount_Q1Limit)*100,2) as Q1_FR
        --,AVG(failcount_Q2Limit)*100 as failcount_Q2
        --,AVG(failcount_Q3Limit)*100 as failcount_Q3
        --,AVG(failcount_Q4Limit)*100 as failcount_Q4
        
        
        from (
                SELECT serial_number,dcm_eval_code, dcm_date, dcm_date_time, test_start_date_time,batch_date_time,batch_date,startdt,pass_fail,dcm_moba_code,dcm_line_id,pn_capacity,
                    Maxmag_20xTo30x_BI,Maxmag_20xTo30x_POSBI,DeltaMaxMag,failcount_PElimit,
                    max(case when Maxmag_20xTo30x_POSBI >= 1.10 or DeltaMaxMag > 0.34 then 1 else 0 end) as failcount_FEALimit,
                    max(case when Maxmag_20xTo30x_POSBI >= 1.10 then 1 else 0 end) as failcount_FEA2Limit,
                    
                    max(case when Maxmag_20xTo30x_POSBI >= 1.10 and DeltaMaxMag > 0.34 then 1 else 0 end) as failcount_Q1Limit,
                    max(case when Maxmag_20xTo30x_POSBI >= 1.10 and DeltaMaxMag <= 0.34 then 1 else 0 end) as failcount_Q2Limit,
                    max(case when Maxmag_20xTo30x_POSBI < 1.10 and DeltaMaxMag <= 0.34 then 1 else 0 end) as failcount_Q3Limit,
                    max(case when Maxmag_20xTo30x_POSBI < 1.10 and DeltaMaxMag > 0.34 then 1 else 0 end) as failcount_Q4Limit,
                    row_number() over(partition by serial_number,dcm_date order by batch_date asc) AS Flg_bt_date,
                    groupMBA, groups,tester_id,testerID
                FROM (
                        SELECT BI.serial_number,
                                POS_BI.dcm_eval_code,
                                POS_BI.dcm_date,
                                POS_BI.dcm_date_time,
                                BI.test_start_date_time,
                                BI.batch_date_time,
                                BI.batch_date,
                                POS_BI.startdt,
                                BI.pass_fail,
                                BI.dcm_moba_code,
                                BI.dcm_line_id,
                                BI.pn_capacity,
                                BI.Maxmag_20xTo30x_BI,
                                POS_BI.Maxmag_20xTo30x_POSBI,
                                (POS_BI.Maxmag_20xTo30x_POSBI-BI.Maxmag_20xTo30x_BI) AS DeltaMaxMag,
                                POS_BI.failcount_PElimit,
                                BI.groupMBA,BI.groups,BI.tester_id,BI.testerID

                            FROM(
                                    (SELECT serial_number,dcm_eval_code,
                                        test_start_date_time,
                                        DATE_FORMAT(cast(dcm_date as timestamp),'%Y%m%d') AS dcm_date,
                                        dcm_date_time, 
                                        batch_date_time,
                                        DATE_FORMAT(cast(batch_date as timestamp),'%Y%m%d') AS batch_date,
                                        startdt,
                                        pass_fail,
                                        dcm_moba_code,
                                        dcm_line_id,
                                        pn_capacity,
                                        test_type,
                                        MAX(Maxmag_20xTo30x) AS Maxmag_20xTo30x_BI,
                                        groups,tester_id,testerID,groupMBA
                                        From tmp
                                        where test_type = 762
                                    group by serial_number,dcm_eval_code, dcm_date, dcm_date_time, 
                                        test_start_date_time,
                                        batch_date_time,
                                        DATE_FORMAT(cast(batch_date as timestamp),'%Y%m%d'),
                                        startdt,
                                        pass_fail,
                                        dcm_moba_code,
                                        dcm_line_id,
                                        pn_capacity,
                                        test_type,groups,tester_id,testerID,groupMBA
                                    ) AS BI
                    
                                INNER JOIN
                
                                    (SELECT serial_number,
                                        dcm_eval_code, 
                                        DATE_FORMAT(cast(dcm_date as timestamp),'%Y%m%d') AS dcm_date,
                                        dcm_date_time, 
                                        test_start_date_time,
                                        batch_date_time,
                                        DATE_FORMAT(cast(batch_date as timestamp),'%Y%m%d') AS batch_date,
                                        startdt,
                                        pass_fail,
                                        dcm_moba_code,
                                        dcm_line_id,
                                        pn_capacity,
                                        test_type,
                                        MAX(Maxmag_20xTo30x) AS Maxmag_20xTo30x_POSBI,
                                        MAX(failcount_PE1) as failcount_PElimit,
                                        groups,tester_id,testerID,groupMBA
                                        From tmp
                                        where test_type = 764
                                    group by serial_number,dcm_eval_code, dcm_date, dcm_date_time, 
                                        test_start_date_time,
                                        batch_date_time,
                                        DATE_FORMAT(cast(batch_date as timestamp),'%Y%m%d'),
                                        startdt,
                                        pass_fail,
                                        dcm_moba_code,
                                        dcm_line_id,
                                        pn_capacity,
                                        test_type,groups,tester_id,testerID,groupMBA
                                    ) AS  POS_BI
                                    ON BI.serial_number = POS_BI.serial_number
                                    and BI.batch_date = POS_BI.batch_date
                        ) 
                    ) AS SubA
                Group by serial_number,dcm_eval_code, dcm_date, dcm_date_time, test_start_date_time,batch_date_time,batch_date,startdt,pass_fail,dcm_moba_code,dcm_line_id,pn_capacity,
                Maxmag_20xTo30x_BI,Maxmag_20xTo30x_POSBI,DeltaMaxMag,failcount_PElimit,groupMBA, groups,tester_id,testerID
        )
        where  dcm_date BETWEEN DATE_FORMAT(DATE_ADD('day',-7,CURRENT_DATE),'%Y%m%d') AND DATE_FORMAT(CURRENT_DATE,'%Y%m%d')
            AND Flg_bt_date = 1
        Group by dcm_date,dcm_line_id||'_'||groupMBA||'_'||pn_capacity
        HAVING COUNT(*) >= 500
        Order by dcm_date,dcm_line_id||'_'||groupMBA||'_'||pn_capacity
    """
    return sql