INSERT INTO app_mgmt.sys_config_dataset_info (src_nm, dataset_nm, file_qty, file_range_min, pre_proc_flg, serv_now_priorty_cd, sla_runtm_second, serv_now_group_nm, err_notfcn_email_nm, notfcn_email_nm, virt_env_cd, proc_stage_cd, catlg_flg, trgt_dw_list, cmput_whse_nm, manl_upld_s3_uri_txt, manl_upld_flg) VALUES
('fin_user', 'demo_1', 1, 30, 'N', 'P4', 0, 'kortex_nga_aws.global.l2', 'Non-Production_Kortex_AWS_Alerts@kellogg.com', '', 'preproc', 'Y', 'Y', 'Snowflake', 'keu_it_small', '', 'N');

INSERT INTO app_mgmt.sys_config_pre_proc_info (src_nm, dataset_nm, file_patrn_txt, pre_proc_methd_val, file_qty, fmt_type_cd) VALUES
('fin_user', 'demo_1', '', '', 0, '');

INSERT INTO app_mgmt.sys_config_table_info (src_nm, domn_nm, dataset_nm, redshift_table_nm, src_table_nm, data_clasfctn_nm, file_qty, fmt_type_cd, delmtr_cd, file_patrn_txt, dprct_methd_cd, load_enbl_flg, file_hdr_cnt, pre_proc_flg, pre_proc_cd, src_encod_cd, src_chrset_cd, src_newln_chr_cd, proc_stage_cd, catlg_flg, dprct_selct_critra_txt, bypas_file_order_seq_check_ind, land_spctrm_flg, wrkflw_nm, copy_by_field_nm_not_posn_ind, bypas_file_hdr_config_posn_check_ind, bypas_file_hdr_config_check_inf) VALUES
('fin_user', 'fin_user', 'demo_1', 'demo_2', 'demo_2', 'confd', 0, 'csv', ',', '', 'okrdra', 'Y', 1, 'N', '', '', '', '', 'load', 'Y', '', 'N', '', '', '', '', '');

INSERT INTO app_mgmt.sys_config_table_field_info (src_nm, src_table_nm, field_nm, field_posn_nbr, datatype_nm, datatype_size_val, datatype_scale_val, key_ind, check_table, field_desc, dprct_ind, partitn_ind, sort_key_ind, dist_key_ind, proc_stage_cd, catlg_flg, dblqt_repl_flg, delta_key_ind) VALUES
('fin_user', 'demo_2', 'Model', 1, 'VARCHAR(255)', 9, 9, '', '', '', '', '', '', '', '', '', '', ''),
('fin_user', 'demo_2', 'Select Version', 2, 'VARCHAR(255)', 10, 10, '', '', '', '', '', '', '', '', '', '', ''),
('fin_user', 'demo_2', 'Report_Count', 3, 'NUMBER(38,0)', 2, 2, '', '', '', '', '', '', '', '', '', '', ''),
('fin_user', 'demo_2', 'Comparison Version', 4, 'VARCHAR(255)', 10, 10, '', '', '', '', '', '', '', '', '', '', ''),
('fin_user', 'demo_2', 'Report_Date', 5, 'TIMESTAMP_NTZ', 19, 19, 'X', '', '', '', '', '', '', '', '', '', ''),
('fin_user', 'demo_2', 'Select Version Code', 6, 'VARCHAR(255)', 10, 10, '', '', '', '', '', '', '', '', '', '', ''),
('fin_user', 'demo_2', 'FX Version Code', 7, 'VARCHAR(255)', 22, 22, '', '', '', '', '', '', '', '', '', '', ''),
('fin_user', 'demo_2', 'Comparison Version Code', 8, 'VARCHAR(255)', 9, 9, '', '', '', '', '', '', '', '', '', '', ''),
('fin_user', 'demo_2', 'Type of Version', 9, 'VARCHAR(255)', 15, 15, '', '', '', '', '', '', '', '', '', '', ''),
('fin_user', 'demo_2', 'Flag', 10, 'VARCHAR(255)', 5, 5, '', '', '', '', '', '', '', '', '', '', '');