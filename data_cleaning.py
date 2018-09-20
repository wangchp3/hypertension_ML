# encoding utf-8
# from test
import pandas as pd

from data_connect import Db
from time_and_memory_use import time_use
import common
import pickle
import re

all_name = pd.read_csv(r"F:\wcp\DM\all_name.csv", encoding='utf-8')
pattern = re.compile('^[\(（].*?[\)）]+')

# 做法包括以下几个：
## 先将未发生高血压的患者分类分出来
## 再将这些患者的病程拉出来


class Dataclean:
    def __init__(self):
        self.db = Db()

    @time_use
    def define_patients_with_hp(self):
        """
        定义纳入的患者标准：患有入院诊断高血压且诊断为高血压之前未出现心血管疾病诊断，提取数据的标准来自于研究设计。
        sql_find_outpatient + sql_find_inpatient:sql语句判断入院为高血压的时间（门诊+ 住院 通过master_index2把每个人的身份证信息统一，trace 到一个人）
        :return: type:dataframe; info: patient basic information
        """
        # outpatient
        sql_find_patient = """SELECT * FROM (SELECT a.DISEASE_NAME,a.DIAG_TIME,c.*,row_number() 
        over (partition by c.MASTER_INDEX2 order by a.DIAG_TIME asc) as row_num FROM 
        [dbo].[OUT_DIAG]a,[dbo].[OUT_REG_INFO] b,[dbo].[PUB_PATIENT] c WHERE 
        (a.DISEASE_NAME LIKE '%高血压%'or a.DISEASE_NAME LIKE '%HBP%') and a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID)tb where row_num=1 
        union all SELECT * FROM (SELECT a.DISEASE_NAME,a.DIAG_TIME,c.*,row_number()
         over (partition by c.MASTER_INDEX2 order by a.DIAG_TIME asc) as row_num FROM 
         [dbo].[INPAT_DIAG]a,[dbo].[INPAT_REG_INFO] b, [dbo].[PUB_PATIENT] c 
         WHERE (a.DISEASE_NAME LIKE '%高血压%'or a.DISEASE_NAME LIKE '%HBP%') and a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID)tb where row_num=1"""
        patient_diag = self.db.get_dict_list(sql_find_patient)
        patient_diag['MASTER_INDEX2'] = patient_diag['MASTER_INDEX2'].apply(int)
        # 去除master_index,列名重命名
        patient_diag.drop(columns=['MASTER_INDEX'], inplace=True)
        patient_diag.rename(columns={'MASTER_INDEX2': 'MASTER_INDEX'}, inplace=True)
        # distinct,DROPNA OF DIAG_TIME AND LOCATION THE MOST EARLY DIAGNOSE TIME
        patient_diag.sort_values(by=['MASTER_INDEX'], inplace=True)
        patient_diag.dropna(subset=['DIAG_TIME'], inplace=True)
        all_patients_diag = patient_diag.groupby(['MASTER_INDEX'], as_index=False)['DIAG_TIME'].min()

        all_patients = all_patients_diag.merge(patient_diag, how='left', on=['MASTER_INDEX', 'DIAG_TIME'])

        self.print_results("诊断含高血压字段的人数", len(all_patients.MASTER_INDEX.unique()))

        return all_patients

    def distinct_hp(self):
        """
        #该数据是为了将妊娠高血压以及难治性高血压的人剔除
        :return: 输出两种诊断的人数
        """
        sql_find_outpatient = """SELECT distinct MASTER_INDEX2 FROM [dbo].[OUT_DIAG]a,[dbo].[OUT_REG_INFO] b,
                [dbo].[PUB_PATIENT] c WHERE (a.DISEASE_NAME LIKE '%难治%高血压%' OR a.DISEASE_NAME LIKE
                 '%妊娠%高血压%') and a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID union 
                 SELECT distinct MASTER_INDEX2 FROM [dbo].[INPAT_DIAG]a,[dbo].[INPAT_REG_INFO] b, [dbo].[PUB_PATIENT] c 
               WHERE (a.DISEASE_NAME LIKE '%难治%高血压%' OR a.DISEASE_NAME LIKE
                 '%妊娠%高血压%') and a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID"""

        patient_diag = self.db.get_dict_list(sql_find_outpatient)
        patient_diag['MASTER_INDEX2'] = patient_diag['MASTER_INDEX2'].apply(int)

        out_all_hp = patient_diag.MASTER_INDEX2.unique().tolist()
        self.print_results("剔除的人数（难治性高血压以及妊娠高血压）", len(out_all_hp))
        return out_all_hp

    @time_use
    def define_patients_with_cvd(self):
        """
        定义纳入的患者标准：患有入院诊断高血压且诊断为高血压之前未出现心血管疾病诊断，提取数据的标准来自于研究设计。
        sql_find_all_patient:sql语句判断入院为高血压的时间（门诊+ 住院 通过master_index）
        :return:
        """
        sql_find_patient = """SELECT * FROM (SELECT a.DISEASE_NAME,a.DIAG_TIME,c.*,row_number() 
                over (partition by c.PERSON_ID order by a.DIAG_TIME asc) as row_num 
                FROM [dbo].[OUT_DIAG]a,[dbo].[OUT_REG_INFO] b, [dbo].[PUB_PATIENT] c
                WHERE (a.DISEASE_NAME LIKE '%脑卒中%' or a.DISEASE_NAME like '%脑梗%' 
                or a.DISEASE_NAME LIKE '%TIA%' or a.DISEASE_NAME LIKE '%短暂性缺血性发作%' or a.DISEASE_NAME LIKE '%房%颤%'
                or a.DISEASE_NAME like '%心%衰%' or a.DISEASE_NAME like '%心%梗%' or a.DISEASE_NAME like '%肾%衰%' 
                or a.DISEASE_NAME like '%冠%心%病%' or a.DISEASE_NAME like '%肺源性心脏病%' or a.DISEASE_NAME like '%CKD%' or a.DISEASE_NAME LIKE '%cad%'
                or a.DISEASE_NAME LIKE '%高血压性心脏病%'  or a.DISEASE_NAME LIKE '%高血压心脏病%' or a.DISEASE_NAME LIKE '%动脉粥样硬化%'
                )and a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID)tb where row_num=1 
                union all SELECT * FROM (SELECT a.DISEASE_NAME,a.DIAG_TIME,
                c.*,row_number() over (partition by c.PERSON_ID order by a.DIAG_TIME asc) as row_num 
                FROM [dbo].[INPAT_DIAG]a,[dbo].[INPAT_REG_INFO] b, [dbo].[PUB_PATIENT] c
                WHERE (a.DISEASE_NAME LIKE '%脑卒中%' or a.DISEASE_NAME like '%脑梗%'  or a.DISEASE_NAME LIKE '%房%颤%'
                or a.DISEASE_NAME LIKE '%TIA%' or a.DISEASE_NAME LIKE '%短暂性缺血性发作%' or a.DISEASE_NAME LIKE '%cad%'
                or a.DISEASE_NAME like '%心%衰%' or a.DISEASE_NAME like '%心%梗%' or a.DISEASE_NAME 
                like '%肾%衰%' or a.DISEASE_NAME like '%冠%心%病%' or a.DISEASE_NAME like '%肺源性心脏病%' or a.DISEASE_NAME LIKE '%动脉粥样硬化%'
                or a.DISEASE_NAME LIKE '%高血压性心脏病%'  or a.DISEASE_NAME LIKE '%高血压心脏病%' or a.DISEASE_NAME like '%CKD%' 
                )and a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID)tb where row_num=1 order by MASTER_INDEX2"""

        patient_diag = self.db.get_dict_list(sql_find_patient)
        patient_diag['MASTER_INDEX2'] = patient_diag['MASTER_INDEX2'].apply(int)

        patient_diag.drop(['MASTER_INDEX'], axis=1, inplace=True)
        patient_diag.rename(columns={'MASTER_INDEX2': 'MASTER_INDEX'}, inplace=True)
        # distinct
        patient_diag.dropna(subset=['DIAG_TIME'], inplace=True)

        all_patients_diag = patient_diag.groupby(['MASTER_INDEX'], as_index=False)['DIAG_TIME'].min()
        # all_patients_diag.set_index(['MASTER_INDEX', 'DIAG_TIME'], inplace=True)
        # index_diag = list(all_patients_diag.index)
        # patient_diag.set_index(['MASTER_INDEX', 'DIAG_TIME'], inplace=True)
        # all_patients = patient_diag[patient_diag.index.isin(index_diag)]
        # all_patients.reset_index(inplace=True)
        all_patients = all_patients_diag.merge(patient_diag, on=['MASTER_INDEX', 'DIAG_TIME'], how='left', sort=False)  #检查是否有重复

        self.print_results("诊断含心血管疾病字段的人数", len(all_patients))

        return all_patients

    @time_use
    def fill_diag_time(self, master_indexes):
        """
        主要是将患者的末端就诊时间获取终点时间 follow_up 的时间
        :param master_indexes: 患者的master_index
        :return:
        """
        sql_find_patient_diag_time = """SELECT DISEASE_NAME,DIAG_TIME,MASTER_INDEX2 FROM 
        (SELECT a.DISEASE_NAME,a.DIAG_TIME,c.*,row_number() over (partition by c.PERSON_ID 
        order by a.DIAG_TIME desc) as row_num FROM [dbo].[OUT_DIAG]a,[dbo].[OUT_REG_INFO] b,
         [dbo].[PUB_PATIENT] c WHERE a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID)tb where row_num=1 and MASTER_INDEX2 in {} union 
         SELECT DISEASE_NAME,DIAG_TIME,MASTER_INDEX2 FROM 
        (SELECT a.DISEASE_NAME,a.DIAG_TIME,c.*,row_number() over (partition by c.PERSON_ID order by
         a.DIAG_TIME desc) as row_num FROM [dbo].[INPAT_DIAG]a,[dbo].[INPAT_REG_INFO] b, 
         [dbo].[PUB_PATIENT] c WHERE a.REG_ID=b.REG_ID and b.PERSON_ID=c.PERSON_ID)tb where row_num=1 and MASTER_INDEX2 in {}""".format(
            master_indexes, master_indexes)
        patient_diag_time = self.db.get_dict_list(sql_find_patient_diag_time)
        patient_diag_time['MASTER_INDEX2'] = patient_diag_time['MASTER_INDEX2'].apply(int)

        patient_diag_time.dropna(subset=['DIAG_TIME'], inplace=True)
        all_patients_diag_time = patient_diag_time.groupby(['MASTER_INDEX2'], as_index=False)['DIAG_TIME'].max()

        all_patients_diag_time = all_patients_diag_time.merge(patient_diag_time, on=['MASTER_INDEX2','DIAG_TIME'])
        all_patients_diag_time.rename(columns={"MASTER_INDEX2": "MASTER_INDEX","DIAG_TIME": "DIAG_TIME_y", "DISEASE_NAME": "DISEASE_NAME_y"},
                                      inplace=True)
        all_patients_diag_time = all_patients_diag_time[["MASTER_INDEX", "DIAG_TIME_y", "DISEASE_NAME_y"]]
        all_patients_diag_time.set_index("MASTER_INDEX",inplace=True)
        return all_patients_diag_time

    def contains_c_class_medication(self):
        """
        使用c类药物（除C01，C10,C11类）的时间
        :return:
        """
        sql_find_index_time = """SELECT e.MASTER_INDEX2,b.CHARGE_TIME,b.ITEM_NAME,a.ATC_CODE FROM [dbo].[DIC_DRUG_ATC_NEW] a, [dbo].[OUT_FEE] b, [dbo].[OUT_REG_INFO] d,[dbo].[PUB_PATIENT] e
               where a.ITEM_NAME = b.ITEM_NAME and b.REG_ID = d.REG_ID and d.PERSON_ID = e.PERSON_ID and a.ATC_CODE like 'C%'
               union all SELECT e.MASTER_INDEX2,c.CHARGE_TIME,c.ITEM_NAME,a.ATC_CODE FROM [dbo].[DIC_DRUG_ATC_NEW] a,[dbo].[INPAT_FEE] c, [dbo].[INPAT_REG_INFO] d,[dbo].[PUB_PATIENT] e
               where a.ITEM_NAME = c.ITEM_NAME and c.REG_ID = d.REG_ID and d.PERSON_ID = e.PERSON_ID and a.ATC_CODE like 'C%'"""
        conn = Db.get_conn()
        time_totake_medicine = pd.read_sql(sql_find_index_time, conn)
        time_totake_medicine['MASTER_INDEX2'] = time_totake_medicine['MASTER_INDEX2'].apply(int)
        time_totake_medicine.to_pickle('time_totake_medicine.pickle')
        # 剔除C01，C10,C11类
        time_totake_medicine.dropna(subset=['CHARGE_TIME'], inplace=True)
        time_totake_medicine.rename(columns={'MASTER_INDEX2': 'MASTER_INDEX'}, inplace=True)
        time_totake_medicine.drop(time_totake_medicine[time_totake_medicine.ATC_CODE.apply(lambda x: x.strip()[:3].__contains__('1'))].index.tolist(),inplace=True)

        time_totake_medicine_grouped = time_totake_medicine.groupby('MASTER_INDEX', as_index=False)['CHARGE_TIME'].min()

        return time_totake_medicine_grouped

    @time_use
    def define_patients(self, all_patients_with_hp, all_patient_with_cvd, out_all_hp, c_class):
        """
        定义纳入的患者标准：患有入院诊断高血压且诊断为高血压之前未出现心血管疾病诊断，提取数据的标准来自于研究设计。
        出生日期为空，诊断日期为空的则删除
        :param all_patients_with_hp:
        :param all_patient_with_cvd:
        :return:
        """
        # 合并数据
        all_patient_with_cvd = all_patient_with_cvd[['MASTER_INDEX', 'DIAG_TIME', 'DISEASE_NAME']]

        c_class= c_class[['MASTER_INDEX','CHARGE_TIME']]
        c_class.set_index(['MASTER_INDEX'], inplace=True)

        involve_patient = pd.merge(left=all_patients_with_hp, right=all_patient_with_cvd, how='left', on='MASTER_INDEX',
                                   sort=False, suffixes=('_x', '_y'))

        involve_patient.set_index(['MASTER_INDEX'], inplace=True)

        print('差集：\n', set(all_patient_with_cvd.index.tolist())-(set(all_patients_with_hp.index.tolist())|set(all_patients_with_hp.index.tolist())))
        # 删除妊娠高血压以及难治性高血压患者
        involve_patient.drop(out_all_hp, axis=0, inplace=True)

        # 删除出生日期和诊断高血压日期为空的数据
        out_list_time = involve_patient[
            (involve_patient.BIRTHDAY.isnull()) | (involve_patient.DIAG_TIME_x.isnull())].index.tolist()
        involve_patient = involve_patient.drop(out_list_time)

        # 填充数据，按照数据库更新时间作为终点/按照特定的2017年12月31日/按照最近一次就诊的时间,采用最后一种方式
        master_index = tuple(involve_patient[involve_patient.DIAG_TIME_y.isnull()].index)
        patients_diag_time = self.fill_diag_time(master_index)
        involve_patient = involve_patient.combine_first(patients_diag_time)

        # 比较两个诊断日期的大小，确定入组的人群，并画出人群年龄/性别等基本分布,以及缺失值情况，及最近一次就诊时间
        involve_patient['time_delta'] = involve_patient['DIAG_TIME_y'].apply(str).apply(lambda x: common.parse_ymd(x)) - involve_patient[
            'DIAG_TIME_x'].apply(str).apply(lambda x: common.parse_ymd(x))
        involve_patient['time_delta'] = involve_patient['time_delta'].apply(lambda x: x.days)
        f = lambda x: -1 if x < 1 else 1
        involve_patient['time_label'] = involve_patient['time_delta'].apply(f)
        self.print_results('分层结果，1代表在高血压诊断后发生心血管疾病，-1代表在高血压诊断之前发生心血管疾病', involve_patient['time_label'].value_counts())

        # 过滤得到单纯性高血压患者
        involve_patient = involve_patient[involve_patient.time_label == 1]

        # 保存结果
        involve_patient.to_csv('involve_patient.csv', encoding='utf_8_sig')
        involve_patient.to_pickle('involve_patient.pickle')
        self.print_results("研究人群的大小", len(involve_patient))
        return involve_patient

    def medicine_name_clean(self):
        """
        将高血压用药所有拉出来得到表1，对atc表格进行mapping,清洗，得到一个标准的atc
        :return: type:dataframe
        """
        # outpatient
        sql_find_patient_atc = """SELECT DISTINCT(ITEM_NAME) FROM ((SELECT DISTINCT(ITEM_NAME) 
        FROM [dbo].[OUT_FEE] WHERE I_TYPE_NAME like '%药费%') UNION (SELECT DISTINCT(ITEM_NAME) 
        FROM [dbo].[INPAT_FEE] WHERE I_TYPE_NAME like '%药费%'))tb"""
        medications = self.db.get_dict_list(sql_find_patient_atc)
        medications.dropna(subset=['ITEM_NAME'], inplace=True)
        medications['ITEM_NAME'] = medications['ITEM_NAME'].apply(lambda s:pattern.sub('', s))
        medications.to_pickle('medications.pickle')

    @staticmethod
    def medicine_name_standard():
        with open('medications.pickle', 'rb') as pickle_file:
            medications = pickle.load(pickle_file)
        target = list(all_name.drop_duplicates("medical_name").medical_name.str.strip())
        origin = list(medications.ITEM_NAME)
        atc_dict = dict(zip(all_name.medical_name, all_name.atc))
        item_name = []
        standard_item_name = []

        tree = common.Tree()
        for word in target:
            tree.insert(word)
        for index, word in enumerate(origin):
            if type(word) is str:
                item_name.append(word)
                standard_medical_word = tree.find(word)
                standard_item_name.append(standard_medical_word)
                # print("{}  {}-> {}".format(index, word, standard_medical_word))

            else:
                item_name.append(word)
                standard_medical_word = "空值"
                standard_item_name.append(standard_medical_word)
                # print("{}  {}-> {}".format(index, word, "空值"))
        count_match = (len(origin) - standard_item_name.count(None)) / len(origin)
        print("匹配上的有：{}".format(count_match))
        medications_clean = pd.DataFrame(data=[origin, standard_item_name])
        medications_clean = medications_clean.T
        medications_clean.columns = ['origin_name', 'standardize_name']
        medications_clean['atc'] = medications_clean['standardize_name'].map(atc_dict)
        return medications_clean

    def insert_atc_data(self):
        """
        update database atc table
        :param medications_clean: 包含了atc,origin_name,standardize_name
        :return:
        """
        with open("medication_clean.pickle", 'rb') as pic_file:
            medications_clean=pickle.load(pic_file)
        medications_clean.dropna(subset=['atc'], inplace=True)
        medications = [(value.atc, value.standardize_name, '%'+value.origin_name+'%') for i,value in medications_clean.iterrows()]
        # for i, value in medications_clean.iterrows():
        sql = """update [dbo].[DIC_DRUG_ATC_NEW] set ATC_CODE = ?,I_ITEM_NAME = ? where ATC_CODE is null and ITEM_NAME like ? """
        rows = self.db.exec_many(sql, medications)
        print('影响行数：{}'.format(rows))

    def clean_diagnose(self, data):
        """
        清洗诊断  按照诊断，确定人群
        高血压I级，高血压2级，高血压3级，原发性高血压，继发性高血压。可以通过value_counts得到所有的高血压诊断，但是先对所有字段两端除掉空白字段
        :return:
        """
        data['DISEASE_NAME_hp'] = data.DISEASE_NAME_x.str.strip().str.replace('?','').str.replace('？', '')
        data['DISEASE_NAME_cvd'] = data.DISEASE_NAME_y.str.strip().str.replace('?','').str.replace('？', '')
        self.print_results("diagnose classification", data['DISEASE_NAME_x'].value_counts())
        return

    def year_three_times(self):
        """

        :return:
        """
        with open('c_class.pickle', 'rb') as pic_file1:
            c_class = pickle.load(pic_file1)
        with open('involve_patient.pickle', 'rb') as pic_file2:
            involve_patient = pickle.load(pic_file2)

        c_class_involve = c_class[c_class.MASTER_INDEX2.isin(involve_patient.MASTER_INDEX)]


        return

    def lab_operation(self):
        """
        得到研究人群的实验室检查报告(index time之后的实验报告)
        :return:
        """
        sql_operation = """"""
        return

    def medicine_usage(self):
        """
        得到研究人群的用药情况(index time之后的用药报告)
        :return:
        """
        sql_medication = """"""
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


if __name__ == "__main__":

    hp = Dataclean()
    # 先将药品名称全部清洗，并更新到数据库中
    # hp.medicine_name_clean()
    # medication = hp.medicine_name_standard()
    # medication.to_csv('medication_clean.csv', encoding='utf-8-sig')
    # medication.to_pickle('medication_clean.pickle')
    # hp.insert_atc_data()
    # 将定义研究人群限定
    # hp_patients = hp.define_patients_with_hp()
    # cvd_patients = hp.define_patients_with_cvd()
    # out_all_hp = hp.distinct_hp()
    c_class = hp.contains_c_class_medication()
    # involve_patient = hp.define_patients(hp_patients, cvd_patients, out_all_hp, c_class)
    # hp.clean_diagnose(involve_patient)
