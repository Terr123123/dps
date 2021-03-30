DELETE FROM edw.dict_map where src_sys_cd='sp';
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm,dict_list) VALUES
    ('fp_job_lvl','理财师职位等级','list','A','分总','分公司总经理,城市总经理'),
    ('fp_job_lvl','理财师职位等级','list','B','常务副总','常务副总'),
    ('fp_job_lvl','理财师职位等级','list','C','团队副总','副总经理（带团队）'),
    ('fp_job_lvl','理财师职位等级','list','D','理财经理','高级理财经理,理财经理,理财经理(实习)'),
    ('fp_job_lvl','理财师职位等级','list','E','核心人力','其他');
    
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('emp_stat','员工状态','key','正式','正式'),
    ('emp_stat','员工状态','key','试用','试用'),
    ('emp_stat','员工状态','key','实习','实习'),
    ('emp_stat','员工状态','key','离职','离职'),
    ('emp_stat','员工状态','key','退休','退休');
    
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('left_flag','员工离职状态','key','Y','离职'),
    ('left_flag','员工离职状态','key','N','在职');
         
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('emp_cat','员工类别','key','前台','前台'),
    ('emp_cat','员工类别','key','后台','后台');
    
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm,dict_rng_beg,dict_rng_end) VALUES
    ('conf_stat_map','总审状态映射','rng','1','待提交','PRIFUND','0'),
    ('conf_stat_map','总审状态映射','rng','2','待初审','PRIFUND','1'),
    ('conf_stat_map','总审状态映射','rng','3','待总审','PRIFUND','2'),
    ('conf_stat_map','总审状态映射','rng','4','总审通过','PRIFUND','3'),
    ('conf_stat_map','总审状态映射','rng','5','初审退回','PRIFUND','4'),
    ('conf_stat_map','总审状态映射','rng','6','总审退回','PRIFUND','5'),
    ('conf_stat_map','总审状态映射','rng','7','交易作废','PRIFUND','6'),
    ('conf_stat_map','总审状态映射','rng','8','已回访','PRIFUND','7'),
    ('conf_stat_map','总审状态映射','rng','9','财务审核通过','PRIFUND','8'),
    ('conf_stat_map','总审状态映射','rng','10','财务审核退回','PRIFUND','9'),
    ('conf_stat_map','总审状态映射','rng','11','待回访','PRIFUND','10'),
    ('conf_stat_map','总审状态映射','rng','12','回访不通过','PRIFUND','11'),
    ('conf_stat_map','总审状态映射','rng','13','待提交','AFUND','0'),
    ('conf_stat_map','总审状态映射','rng','14','待初审','AFUND','1'),
    ('conf_stat_map','总审状态映射','rng','15','待总审','AFUND','2'),
    ('conf_stat_map','总审状态映射','rng','16','总审通过','AFUND','3'),
    ('conf_stat_map','总审状态映射','rng','17','初审退回','AFUND','4'),
    ('conf_stat_map','总审状态映射','rng','18','总审退回','AFUND','5'),
    ('conf_stat_map','总审状态映射','rng','19','交易作废','AFUND','6'),
    ('conf_stat_map','总审状态映射','rng','20','确认成功','AFUND','7'),
    ('conf_stat_map','总审状态映射','rng','21','确认失败','AFUND','8'),
    ('conf_stat_map','总审状态映射','rng','25','待总审','INS','1'),
    ('conf_stat_map','总审状态映射','rng','26','总审通过','INS','2'),
    ('conf_stat_map','总审状态映射','rng','27','总审退回','INS','3'),
    ('conf_stat_map','总审状态映射','rng','28','待提交','XT','0'),
    ('conf_stat_map','总审状态映射','rng','29','待初审','XT','1'),
    ('conf_stat_map','总审状态映射','rng','30','待总审','XT','2'),
    ('conf_stat_map','总审状态映射','rng','31','总审通过','XT','3'),
    ('conf_stat_map','总审状态映射','rng','32','初审退回','XT','4'),
    ('conf_stat_map','总审状态映射','rng','33','总审退回','XT','5'),
    ('conf_stat_map','总审状态映射','rng','34','交易作废','XT','6'),
    ('conf_stat_map','总审状态映射','rng','35','已回访','XT','7'),
    ('conf_stat_map','总审状态映射','rng','36','财务审核通过','XT','8'),
    ('conf_stat_map','总审状态映射','rng','37','财务审核退回','XT','9'),
    ('conf_stat_map','总审状态映射','rng','38','待回访','XT','10'),
    ('conf_stat_map','总审状态映射','rng','39','回访不通过','XT','11'),
    ('conf_stat_map','总审状态映射','rng','40','待总审','BX','1'),
    ('conf_stat_map','总审状态映射','rng','41','总审通过','BX','2'),
    ('conf_stat_map','总审状态映射','rng','42','总审退回','BX','3'),
    ('conf_stat_map','总审状态映射','rng','43','待管理人审核','PRIFUND','12'),
    ('conf_stat_map','总审状态映射','rng','44','管理人审核通过','PRIFUND','13'),
    ('conf_stat_map','总审状态映射','rng','45','管理人审核不通过','PRIFUND','14'),
    ('conf_stat_map','总审状态映射','rng','46','待管理人审核','XT','12'),
    ('conf_stat_map','总审状态映射','rng','47','管理人审核通过','XT','13'),
    ('conf_stat_map','总审状态映射','rng','48','管理人审核不通过','XT','14');

INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm,dict_rng_beg,dict_rng_end) VALUES
    ('req_stat_map','申请状态映射','rng','1','未确认','PRIFUND','1'),
    ('req_stat_map','申请状态映射','rng','2','已上送','PRIFUND','2'),
    ('req_stat_map','申请状态映射','rng','3','确认接受','PRIFUND','3'),
    ('req_stat_map','申请状态映射','rng','4','确认成功','PRIFUND','4'),
    ('req_stat_map','申请状态映射','rng','5','确认失败','PRIFUND','5'),
    ('req_stat_map','申请状态映射','rng','6','已撤销','PRIFUND','6'),
    ('req_stat_map','申请状态映射','rng','7','未确认','XT','1'),
    ('req_stat_map','申请状态映射','rng','8','已上送','XT','2'),
    ('req_stat_map','申请状态映射','rng','9','确认接受','XT','3'),
    ('req_stat_map','申请状态映射','rng','10','确认成功','XT','4'),
    ('req_stat_map','申请状态映射','rng','11','确认失败','XT','5'),
    ('req_stat_map','申请状态映射','rng','12','已撤销','XT','6'),
    ('req_stat_map','申请状态映射','rng','13','待确认','FUND','0'),
    ('req_stat_map','申请状态映射','rng','14','正在处理','FUND','1'),
    ('req_stat_map','申请状态映射','rng','15','已处理','FUND','2'),
    ('req_stat_map','申请状态映射','rng','16','确认中','FUND','3');

INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('is_agcy','是否机构','key','Y','机构'),
    ('is_agcy','是否机构','key','N','个人');
    
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('mgr_group','所属板块','key','WN','WN'),
    ('mgr_group','所属板块','key','HS','HS'),
    ('mgr_group','所属板块','key','其他板块','其他板块'),
    ('mgr_group','所属板块','key','多元板块','多元板块');
    
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('ech_cd','梯队编号','key','1','第一梯队'),
    ('ech_cd','梯队编号','key','2','第二梯队'),
    ('ech_cd','梯队编号','key','3','第三梯队'),
    ('ech_cd','梯队编号','key','5','筹备梯队'),
    ('ech_cd','梯队编号','key','X','未分配梯队');
    
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('camp_bform','活动形式','key','01','新客户活动'),
    ('camp_bform','活动形式','key','02','产品活动'),
    ('camp_bform','活动形式','key','03','老客户活动');
    
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('camp_sform','活动类型','key','11','财富下午茶'),
    ('camp_sform','活动类型','key','12','投策会'),
    ('camp_sform','活动类型','key','13','大型峰会'),
    ('camp_sform','活动类型','key','14','异业合作沙龙活动'),
    ('camp_sform','活动类型','key','21','产品沙龙'),
    ('camp_sform','活动类型','key','22','V直播下午茶'),
    ('camp_sform','活动类型','key','23','视频联动产品路演'),
    ('camp_sform','活动类型','key','24','区域联动产品路演'),
    ('camp_sform','活动类型','key','25','产品路演'),
    ('camp_sform','活动类型','key','26','项目考察'),
    ('camp_sform','活动类型','key','31','增值沙龙'),
    ('camp_sform','活动类型','key','32','名医养生沙龙'),
    ('camp_sform','活动类型','key','33','大客户周边休闲游'),
    ('camp_sform','活动类型','key','34','答谢会'),
    ('camp_sform','活动类型','key','98','老客户公益活动'),
    ('camp_sform','活动类型','key','99','新客户公益活动');
    
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('change_type','客户等级变化类型','key','01','升级'),
    ('change_type','客户等级变化类型','key','02','降级'),
    ('change_type','客户等级变化类型','key','00','入会');
    
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('cash_flag','到期日计算方式','key','1','确认日期'),
    ('cash_flag','到期日计算方式','key','2','投资起始日期'),
    ('cash_flag','到期日计算方式','key','3','成立日期'),
    ('cash_flag','到期日计算方式','key','4','多次清算');
    
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('track_app_cd','APP埋点系统编号','key','app002','掌上海银APP'),
    ('track_app_cd','APP埋点系统编号','key','app003','海银基金APP'),
    ('track_app_cd','APP埋点系统编号','key','app004','海银基金官网'),
    ('track_app_cd','APP埋点系统编号','key','app010','积分商城');
    
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('org_type','机构类型','key','01','国有'),
    ('org_type','机构类型','key','02','合作'),
    ('org_type','机构类型','key','03','合资'),
    ('org_type','机构类型','key','04','独资'),
    ('org_type','机构类型','key','05','集体'),
    ('org_type','机构类型','key','06','私营'),
    ('org_type','机构类型','key','07','个人工商户'),
    ('org_type','机构类型','key','08','其他'),
    ('org_type','机构类型','key','09','家族企业');
    
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('crcy_type','币种','key','036','澳大利亚元'),
    ('crcy_type','币种','key','124','加拿大元'),
    ('crcy_type','币种','key','156','人民币'),
    ('crcy_type','币种','key','344','港币'),
    ('crcy_type','币种','key','392','日元'),
    ('crcy_type','币种','key','410','韩元'),
    ('crcy_type','币种','key','458','马币'),
    ('crcy_type','币种','key','764','泰铢'),
    ('crcy_type','币种','key','826','英镑'),
    ('crcy_type','币种','key','840','美元'),
    ('crcy_type','币种','key','978','欧元');    
    
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm,dict_rng_beg,dict_rng_end) VALUES
    ('trans_chnl','交易渠道','rng','1','海银基金代销','FUND','fsfund'),
    ('trans_chnl','交易渠道','rng','2','柜台','FUND','counter'),
    ('trans_chnl','交易渠道','rng','3','网上交易','FUND','etrading'),
    ('trans_chnl','交易渠道','rng','4','智能投顾','FUND','iimobile'),
    ('trans_chnl','交易渠道','rng','5','官网','FUND','ec'),
    ('trans_chnl','交易渠道','rng','6','移动端','FUND','wechat'),
    ('trans_chnl','交易渠道','rng','7','手机端','FUND','mobile'),
    ('trans_chnl','交易渠道','rng','8','智投柜台','FUND','iicounter'),
    ('trans_chnl','交易渠道','rng','9','组合交易','FUND','ffmobile'),
    ('trans_chnl','交易渠道','rng','10','柜面','PRIFUND','001'),
    ('trans_chnl','交易渠道','rng','11','网上自助','PRIFUND','002'),
    ('trans_chnl','交易渠道','rng','12','微信','PRIFUND','003'),
    ('trans_chnl','交易渠道','rng','13','手机APP','PRIFUND','004'),
    ('trans_chnl','交易渠道','rng','14','海银会','PRIFUND','005'),
    ('trans_chnl','交易渠道','rng','15','邮件','PRIFUND','006'),
    ('trans_chnl','交易渠道','rng','16','海银资产微信端','PRIFUND','007'),
    ('trans_chnl','交易渠道','rng','17','短信','PRIFUND','008'),
    ('trans_chnl','交易渠道','rng','18','柜面','XT','001'),
    ('trans_chnl','交易渠道','rng','19','网上自助','XT','002'),
    ('trans_chnl','交易渠道','rng','20','微信','XT','003'),
    ('trans_chnl','交易渠道','rng','21','手机APP','XT','004'),
    ('trans_chnl','交易渠道','rng','22','海银会','XT','005'),
    ('trans_chnl','交易渠道','rng','23','邮件','XT','006'),
    ('trans_chnl','交易渠道','rng','24','海银资产微信端','XT','007'),
    ('trans_chnl','交易渠道','rng','25','短信','XT','008');
    

INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm,dict_rng_beg,dict_rng_end) VALUES
    ('sign_mthd','签署方式','rng','1','纸质合同','PRIFUND','01'),
    ('sign_mthd','签署方式','rng','2','电子合同','PRIFUND','02'),
    ('sign_mthd','签署方式','rng','3','纸质合同','XT','01'),
    ('sign_mthd','签署方式','rng','4','电子合同','XT','02');
    
INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('core_prod_goal','核心产品业绩达成率合格指标','key','2020','0.8'),
    ('new_busi_goal','新型业务业绩达成率合格指标','key','2020','0.1');

INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm) VALUES
    ('share_chnl','分享渠道','key','headline','财经早报'),
    ('share_chnl','分享渠道','key','micromarketing','智能名片'),
    ('share_chnl','分享渠道','key','article','财经资讯'),
    ('share_chnl','分享渠道','key','poster','精品海报'),
    ('share_chnl','分享渠道','key','teach','投教'),
    ('share_chnl','分享渠道','key','live','直播'),
    ('share_chnl','分享渠道','key','owner_content','转发助手'),
    ('share_chnl','分享渠道','key','mp','小程序');

INSERT INTO edw.dict_map(grp_key,grp_nm,dict_type, dict_key,dict_nm,filed1,filed2) VALUES
  ('src_sys_cd','源系统编号','key', 'application_hycrm', '客户关系（新）', 'crm', 'CRM'),
  ('src_sys_cd','源系统编号','key', 'application_hyb_trans', '私募基金交易系统', 'pf', 'PRIFUND'),
  ('src_sys_cd','源系统编号','key', 'application_ylgq_oms', '公募基金系统', 'hyf', 'FUND'),
  ('src_sys_cd','源系统编号','key', 'application_forefund', '海外基金', 'of', 'AFUND'),
  ('src_sys_cd','源系统编号','key', 'application_portal3', '海银企业门户2', 'portal', 'portal'),
  ('src_sys_cd','源系统编号','key', 'application_cams', '佣金核算系统', 'cms', 'cms'),
  ('src_sys_cd','源系统编号','key', 'application_hyzc_oms', '海银资产运营系统', 'hyzc', ''),
  ('src_sys_cd','源系统编号','key', 'application_wnf_oms', '五牛基金运营系统', 'wnf', ''),
  ('src_sys_cd','源系统编号','key', 'application_hyh', '海银会', 'hyh', 'HYH'),
  ('src_sys_cd','源系统编号','key', 'application_ph', '普惠', 'ph', 'PH'),
  ('src_sys_cd','源系统编号','key', 'application_yqd', '优其鼎', 'yqd', 'YQD'),
  ('src_sys_cd','源系统编号','key', 'application_ft', '家族信托', 'ft', '');