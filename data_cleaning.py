# encoding utf-8
# from test
import pandas as pd

from data_connect import Db


# 做法包括以下几个：
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
        # outpatient
        sql_find_outpatient = """SELECT * FROM (SELECT a.DISEASE_NAME,a.DIAG_TIME,c.*,row_number()
        over (partition by c.MASTER_INDEX order by a.DIAG_TIME asc) as row_num
        FROM [dbo].[OUT_DIAG]a,[dbo].[OUT_REG_INFO] b,
        [dbo].[PUB_PATIENT] c WHERE a.DISEASE_NAME LIKE '%高血压%' and a.REG_ID=b.REG_ID
        and b.PERSON_ID=c.PERSON_ID)tb where row_num=1 order by MASTER_INDEX"""
        outpatient_diag = self.db.get_dict_list(sql_find_outpatient)
        # outpatient_diag = self.dict_list2data_frame(outpatientdatalist)
        # inpatient
        sql_find_inpatient = """SELECT * FROM (SELECT a.DISEASE_NAME,a.DIAG_TIME,c.*,row_number() over
        (partition by c.MASTER_INDEX order by a.DIAG_TIME asc) as row_num FROM
         [dbo].[INPAT_DIAG]a,[dbo].[INPAT_REG_INFO] b, [dbo].[PUB_PATIENT] c 
         WHERE a.DISEASE_NAME LIKE '%高血压%' and a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID)tb where row_num=1 order by MASTER_INDEX"""
        inpatient_diag = self.db.get_dict_list(sql_find_inpatient)
        # inpatient_diag = self.dict_list2data_frame(inpatientdatalist)

        #  merge outpatient and inpatient
        all_patient_info = pd.concat([outpatient_diag, inpatient_diag])
        # distinct
        all_patient_info.sort_values(by=['MASTER_INDEX'], inplace=True)
        all_patients_diag = all_patient_info.groupby(['MASTER_INDEX'])['DIAG_TIME'].min()
        all_patients_diag = all_patients_diag.reset_index()

        all_patients = pd.merge(left=all_patients_diag, right=all_patient_info, how='left',
                                on=['MASTER_INDEX', 'DIAG_TIME'])
        all_patients['diag'] = '高血压'
        self.print_results("诊断含高血压字段的人数", len(all_patients))
        return all_patients

    def distinct_hp(self):
        """
        #该数据是为了将妊娠高血压以及难治性高血压的人剔除
        :return: 输出两种诊断的人数
        """
        sql_find_outpatient = """SELECT distinct MASTER_INDEX FROM [dbo].[OUT_DIAG]a,[dbo].[OUT_REG_INFO] b,
                [dbo].[PUB_PATIENT] c WHERE (a.DISEASE_NAME LIKE '%难治%高血压%' OR a.DISEASE_NAME LIKE
                 '%妊娠%高血压%') and a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID"""

        outpatient_diag = self.db.get_dict_list(sql_find_outpatient)

        sql_find_inpatient = """SELECT distinct MASTER_INDEX FROM [dbo].[INPAT_DIAG]a,[dbo].[INPAT_REG_INFO] b, [dbo].[PUB_PATIENT] c 
               WHERE (a.DISEASE_NAME LIKE '%难治%高血压%' OR a.DISEASE_NAME LIKE
                 '%妊娠%高血压%') and a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID"""

        inpatient_diag = self.db.get_dict_list(sql_find_inpatient)

        all_patient_info = pd.concat([outpatient_diag, inpatient_diag])
        out_all_hp = all_patient_info.MASTER_INDEX.unique().tolist()
        self.print_results("剔除的人数（难治性高血压以及妊娠高血压）", len(out_all_hp))
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
        all_patients_diag = all_patients_diag.reset_index()

        all_patients = pd.merge(left=all_patients_diag, right=all_patient_info, how='left',
                                on=['MASTER_INDEX', 'DIAG_TIME'])
        all_patients['diag'] = 'cvd'
        self.print_results("诊断含心血管疾病字段的人数", len(all_patients))
        return all_patients

    def fill_diag_time(self, master_indexes):
        """
        主要是将患者的末端就诊时间获取
        :param master_indexes: 患者的master_index
        :return:
        """
        sql_find_outpatient_diag_time = """SELECT DISEASE_NAME,DIAG_TIME,MASTER_INDEX FROM 
                (SELECT a.DISEASE_NAME,a.DIAG_TIME,c.*,row_number() over 
                (partition by c.PERSON_ID order by a.DIAG_TIME desc) as row_num 
                FROM [dbo].[OUT_DIAG]a,[dbo].[OUT_REG_INFO] b, [dbo].[PUB_PATIENT] c
                WHERE a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID)tb where row_num=1 and MASTER_INDEX in {}""".format(
            master_indexes)
        outpatient_diag_time = self.db.get_dict_list(sql_find_outpatient_diag_time)
        sql_find_inpatient_diag_time = """SELECT DISEASE_NAME,DIAG_TIME,MASTER_INDEX FROM 
                (SELECT a.DISEASE_NAME,a.DIAG_TIME,c.*,row_number() over 
                (partition by c.PERSON_ID order by a.DIAG_TIME desc) as row_num 
                FROM [dbo].[INPAT_DIAG]a,[dbo].[INPAT_REG_INFO] b, [dbo].[PUB_PATIENT] c
                WHERE a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID)tb where row_num=1 and MASTER_INDEX in {}""".format(
            master_indexes)
        inpatient_diag_time = self.db.get_dict_list(sql_find_inpatient_diag_time)
        all_patient_diag_time = pd.concat([outpatient_diag_time, inpatient_diag_time])
        all_patients_diag_time = all_patient_diag_time.groupby(['MASTER_INDEX'])['DIAG_TIME'].max()
        all_patients_diag_time = all_patients_diag_time.reset_index()
        all_patients_diag_time.rename(columns={"MASTER_INDEX": "MASTER_INDEX", "DIAG_TIME": "DIAG_TIME_y"},
                                      inplace=True)
        return all_patients_diag_time

    def define_patients(self, all_patients_with_hp, all_patient_with_cvd, out_all_hp):
        """
        定义纳入的患者标准：患有入院诊断高血压且诊断为高血压之前未出现心血管疾病诊断，提取数据的标准来自于研究设计。
        出生日期为空，诊断日期为空的则删除
        :param all_patients_with_hp:
        :param all_patient_with_cvd:
        :return:
        """
        # 合并数据
        all_patient_with_cvd = all_patient_with_cvd[['MASTER_INDEX', 'DIAG_TIME']]
        involve_patient = pd.merge(left=all_patients_with_hp, right=all_patient_with_cvd, how='left', on='MASTER_INDEX',
                                   sort=False, suffixes=('_x', '_y'))
        involve_patient.reset_index(inplace=True)
        # 删除妊娠高血压以及难治性高血压患者
        out_list_patient = involve_patient[involve_patient.MASTER_INDEX.isin(out_all_hp)].index.tolist()
        involve_patient.drop(out_list_patient, axis=0, inplace=True)

        # 删除出生日期和诊断日期为空的数据
        out_list_time = involve_patient[
            (involve_patient.BIRTHDAY.isnull()) | (involve_patient.DIAG_TIME_x.isnull())].index.tolist()
        involve_patient.drop(out_list_time, axis=0, inplace=True)
        # 填充数据，按照数据库更新时间作为终点/按照特定的2017年12月31日/按照最近一次就诊的时间,采用最后一种方式
        master_index = tuple(involve_patient[involve_patient.DIAG_TIME_y.isnull()].MASTER_INDEX.values)
        patients_diag_time = self.fill_diag_time(master_index)
        involve_patient = involve_patient.combine_first(patients_diag_time)

        # 比较两个诊断日期的大小，确定入组的人群，并画出人群年龄/性别等基本分布,以及缺失值情况，及最近一次就诊时间
        involve_patient['time_delta'] = involve_patient['DIAG_TIME_y'] - involve_patient['DIAG_TIME_x']
        f = lambda x: -1 if x.days <= 1 else 1
        involve_patient['time_label'] = involve_patient['time_delta'].apply(f)
        self.print_results('分层结果，1代表在高血压诊断后发生心血管疾病，-1代表在高血压诊断之前发生心血管疾病', involve_patient['time_label'].value_counts())

        # 过滤得到单纯性高血压患者
        involve_patient = involve_patient[involve_patient.time_label == 1]

        # 保存结果
        involve_patient.to_csv('involve_patient', encoding='utf_8_sig')
        involve_patient.to_pickle('involve_patient.pickle')
        self.print_results("研究人群的大小", len(involve_patient))
        return involve_patient

    def clean_diagnose(self, data):
        """
        清洗诊断
        高血压I级，高血压2级，高血压3级，原发性高血压，继发性高血压。可以通过value_counts得到所有的高血压诊断，但是先对所有字段两端除掉空白字段
        :return:
        """
        data['DISEASE_NAME'] = data.DISEASE_NAME.str.strip()
        # data['DISEASE_NAME'] = data['DISEASE_NAME'] .str.replace('1.','')
        self.print_results("diagnose classification", data['DISEASE_NAME'].value_counts())
        return

    def lab_operation(self):
        """
        得到研究人群的实验室检查报告
        :return:
        """
        return

    def medicine_usage(self):
        """
        得到研究人群的用药情况
        :return:
        """
        return

    def print_results(self, result_description, result):
        """

        :param result_description: 结果的文字性描述
        :param result: 结果
        :return:
        """
        print("================")
        print("{} = {}".format(result_description, result))
        print("================")
        return


hp = Dataclean()
hp_patients = hp.define_patients_with_hp()
cvd_patients = hp.define_patients_with_cvd()
out_all_hp = hp.distinct_hp()
involve_patient = hp.define_patients(hp_patients, cvd_patients, out_all_hp)
hp.clean_diagnose(involve_patient)
