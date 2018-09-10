# encoding utf-8
# from test
from data_connect import Db
import pandas as pd

#做法包括以下几个：
## 先将未发生高血压的患者分类分出来
## 再将这些患者的病程拉出来


class Dataclean:
    def __init__(self):
        self.db = Db()

    def define_patients_with_hp(self):
        """
        定义纳入的患者标准：患有入院诊断高血压且诊断为高血压之前未出现心血管疾病诊断，提取数据的标准来自于研究设计。
        sql_find_outpatient + sql_find_inpatient:sql语句判断入院为高血压的时间（门诊+ 住院 通过master_index）
        :return: type:dataframe; info: patient basic information
        """
        #outpatient
        sql_find_outpatient = """SELECT * FROM (SELECT a.DISEASE_NAME,a.DIAG_TIME,c.*,row_number()
        over (partition by c.MASTER_INDEX order by a.DIAG_TIME asc) as row_num
        FROM [dbo].[OUT_DIAG]a,[dbo].[OUT_REG_INFO] b,
        [dbo].[PUB_PATIENT] c WHERE a.DISEASE_NAME LIKE '%高血压%' and a.REG_ID=b.REG_ID
        and b.PERSON_ID=c.PERSON_ID)tb where row_num=1 order by MASTER_INDEX"""
        outpatient_diag = self.db.get_dict_list(sql_find_outpatient)
        # outpatient_diag = self.dict_list2data_frame(outpatientdatalist)
       #inpatient
        sql_find_inpatient = """SELECT * FROM (SELECT a.DISEASE_NAME,a.DIAG_TIME,c.*,row_number() over
        (partition by c.MASTER_INDEX order by a.DIAG_TIME asc) as row_num FROM
         [dbo].[INPAT_DIAG]a,[dbo].[INPAT_REG_INFO] b, [dbo].[PUB_PATIENT] c 
         WHERE a.DISEASE_NAME LIKE '%高血压%' and a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID)tb where row_num=1 order by MASTER_INDEX"""
        inpatient_diag = self.db.get_dict_list(sql_find_inpatient)
        # inpatient_diag = self.dict_list2data_frame(inpatientdatalist)

        #merge outpatient and inpatient
        all_patient_info = pd.concat([outpatient_diag,inpatient_diag])
        print(all_patient_info.shape,len(all_patient_info.MASTER_INDEX.unique()))
        #distinct
        all_patient_info.sort_values(by=['MASTER_INDEX'], inplace=True)
        all_patients_diag = all_patient_info.groupby(['MASTER_INDEX'])['DIAG_TIME'].min()
        all_patients_diag.reset_index()
        #检查对应年龄是否因为数据错误有较大的偏差
        all_patient_info.drop(['DIAG_TIME'], axis=1, inplace=True)
        all_patients = pd.merge(left=all_patients_diag, right=all_patient_info, how='left',on='MASTER_INDEX')
        all_patients['diag']='高血压'
        return all_patients

    def distinct_hp(self):
        """
        #该数据是为了将妊娠高血压以及难治性高血压的人剔除
        :return:
        """
        sql_find_outpatient = """SELECT distinct MASTER_INDEX FROM FROM [dbo].[OUT_DIAG]a,[dbo].[OUT_REG_INFO] b,
                [dbo].[PUB_PATIENT] c WHERE (a.DISEASE_NAME LIKE '%难治%高血压%' OR a.DISEASE_NAME LIKE
                 '%妊娠%高血压%') and a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID"""
        outpatient_diag = self.db.get_dict_list(sql_find_outpatient)
        sql_find_inpatient = """SELECT distinct MASTER_INDEX FROM [dbo].[INPAT_DIAG]a,[dbo].[INPAT_REG_INFO] b, [dbo].[PUB_PATIENT] c 
               WHERE (a.DISEASE_NAME LIKE '%难治%高血压%' OR a.DISEASE_NAME LIKE
                 '%妊娠%高血压%') and a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID"""
        inpatient_diag = self.db.get_dict_list(sql_find_inpatient)
        all_patient_info = pd.concat([outpatient_diag, inpatient_diag])
        out_all_hp = all_patient_info.MASTER_INDEX.unique().tolist()
        return out_all_hp

    def define_patients_with_cvd(self):
        """
        定义纳入的患者标准：患有入院诊断高血压且诊断为高血压之前未出现心血管疾病诊断，提取数据的标准来自于研究设计。
        sql_find_all_patient:sql语句判断入院为高血压的时间（门诊+ 住院 通过master_index）
        :return:
        """
        sql_find_outpatient = """SELECT * FROM (SELECT a.DISEASE_NAME,a.DIAG_TIME,c.*,row_number() over (partition by c.PERSON_ID order by a.DIAG_TIME asc) as row_num 
                FROM [dbo].[OUT_DIAG]a,[dbo].[OUT_REG_INFO] b, [dbo].[PUB_PATIENT] c
                WHERE (a.DISEASE_NAME LIKE '%脑卒中%' or a.DISEASE_NAME like '%脑梗%' or a.DISEASE_NAME 
                like '%心%衰%' or a.DISEASE_NAME like '%心%梗%' or a.DISEASE_NAME like '%肾%衰%' 
                or a.DISEASE_NAME like '冠%心%病' or a.DISEASE_NAME like '%肺源性心脏病%' or DISEASE_NAME LIKE '%高血压性心脏病%'  or DISEASE_NAME LIKE '%高血压心脏病%'
                )and a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID)tb where row_num=1 order by MASTER_INDEX"""
        outpatient_diag = self.db.get_dict_list(sql_find_outpatient)
        # outpatient_diag = self.dict_list2data_frame(outpatientdatalist)
        # inpatient
        sql_find_inpatient = """SELECT * FROM (SELECT a.DISEASE_NAME,a.DIAG_TIME,c.*,row_number() over (partition by c.PERSON_ID order by a.DIAG_TIME asc) as row_num 
                FROM [dbo].[INPAT_DIAG]a,[dbo].[INPAT_REG_INFO] b, [dbo].[PUB_PATIENT] c
                WHERE (a.DISEASE_NAME LIKE '%脑卒中%' or a.DISEASE_NAME like '%脑梗%' 
                or a.DISEASE_NAME like '%心%衰%' or a.DISEASE_NAME like '%心%梗%' or a.DISEASE_NAME 
                like '%肾%衰%' or a.DISEASE_NAME like '冠%心%病' or a.DISEASE_NAME like '%肺源性心脏病%'or DISEASE_NAME LIKE '%高血压性心脏病%'  or DISEASE_NAME LIKE '%高血压心脏病%'
                )and a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID)tb where row_num=1 order by MASTER_INDEX"""
        inpatient_diag = self.db.get_dict_list(sql_find_inpatient)
        # inpatient_diag = self.dict_list2data_frame(inpatientdatalist)

        # merge outpatient and inpatient
        all_patient_info = pd.concat([outpatient_diag, inpatient_diag])
        # distinct
        all_patient_info.sort_values(by=['MASTER_INDEX'], inplace=True)
        all_patients_diag = all_patient_info.groupby(['MASTER_INDEX'])['DIAG_TIME'].min()
        all_patients_diag.reset_index()
        # 检查对应年龄是否因为数据错误有较大的偏差
        all_patient_info['AGE'] = all_patient_info['DIAG_TIME'] - all_patient_info['BIRTHDAY']

        all_patient_info.drop(['DIAG_TIME'],axis=1, inplace=True)
        all_patients = pd.merge(left=all_patients_diag, right=all_patient_info, how='left', on='MASTER_INDEX')
        all_patients['diag'] = 'cvd'
        return all_patients

    def define_patients(self,all_patients_with_hp, all_patient_with_cvd, out_all_hp):
        """
        定义纳入的患者标准：患有入院诊断高血压且诊断为高血压之前未出现心血管疾病诊断，提取数据的标准来自于研究设计。
        出生日期为空，诊断日期为空的则删除
        :param all_patients_with_hp:
        :param all_patient_with_cvd:
        :return:
        """
        #合并数据
        all_patient_with_cvd = all_patient_with_cvd['MASTER_INDEX','DIAG_TIME']
        involve_patient=pd.merge(left=all_patients_with_hp, right=all_patient_with_cvd, how='left', on='MASTER_INDEX',sort=False, suffixes=('_x', '_y'))
        involve_patient.reset_index(inplace=True)
        #删除妊娠高血压以及难治性高血压患者
        involve_patient.drop()

        #删除出生日期和诊断日期为空的数据
        out_list_1 = involve_patient[involve_patient.BIRTHDAY_x.isnull()].index.tolist()
        involve_patient.drop(out_list_1,axis=0,inplace=True)
        out_list_2 = involve_patient[involve_patient.DIAG_TIME_x_x.isnull()].index.tolist()
        involve_patient.drop(out_list_2, axis=0, inplace=True)
        out_list_3 = involve_patient[involve_patient.DIAG_TIME_y_x.isnull()].index.tolist()
        involve_patient.drop(out_list_3, axis=0, inplace=True)
        #比较两个诊断日期的大小，确定入组的人群，并画出人群年龄/性别等基本分布,以及缺失值情况，及最近一次就诊时间
        # for i,row in involve_patient.iterrows():
        #         if row['DIAG_TIME_y_x'] and row['DIAG_TIME_y_x'] < row['DIAG_TIME_x_x']:
        #                 involve_patient.drop([i], axis=0, inplace=True)
        involve_patient['time_delta'] = involve_patient['DIAG_TIME_y_x']-involve_patient['DIAG_TIME_x_x']
        f = lambda x: -1 if x.days <= 1 else 1
        involve_patient['time_label'] = involve_patient['time_delta'].apply(f)
        print(involve_patient['time_label'].value_counts())
        involve_patient.to_pickle('involve_patient.pickle')
        return involve_patient



hp = Dataclean()
hp_patients = hp.define_patients_with_hp()
cvd_patients = hp.define_patients_with_cvd()
out_all_hp = hp.distinct_hp()
involve_patient = hp.define_patients(hp_patients, cvd_patients, out_all_hp)

