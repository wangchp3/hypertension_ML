# encoding utf-8
# from test

# 特征提取
import pickle

import matplotlib.pyplot as plt

# 导入数据
with open('involve_patient.pickle', 'rb') as pickle_file:
    involve_patient = pickle.load(pickle_file)
print(involve_patient.shape)
print(involve_patient.columns)


# 获取单纯性高血压患者的其他信息
# 删除不需要的列
def picture_age(data):
    """
    描述数据中的年龄分布，分层分析
    :param data:
    :return:
    """
    data['age'] = data['DIAG_TIME_x'] - data['BIRTHDAY']
    f = lambda time_delta: time_delta.days / 365.2425
    age = data['age'].apply(f)
    plt.hist(age)
    plt.title("Age Histogram")
    plt.xlabel("Value")
    plt.ylabel("Frequency")

    plt.show()


def from_hp_to_cvd(data):
    """
    描述患者患高血压到发生
    :param data:
    :return:
    """
    data['incident_time'] = data['DIAG_TIME_y'] - data['DIAG_TIME_x']
    f = lambda time_delta: time_delta.days / 365.2425
    incident_time = data['incident_time'].apply(f)
    plt.hist(incident_time)
    plt.title("CVD Occur Time Histogram")
    plt.xlabel("Value")
    plt.ylabel("Frequency")

    plt.show()


def picture_gender(data):
    gender_dict = {'男': 0, '女': 1}
    data['SEX']=data['SEX'].map(gender_dict)
