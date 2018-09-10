# encoding utf-8
# from test

##特征提取
import pickle
import plotly.plotly as py
import plotly.tools as tls

#导入数据
with open('involve_patient.pickle', 'rb') as pickle_file:
    involve_patient = pickle.load(pickle_file)
print(involve_patient.shape)
print(involve_patient.columns)

#过滤得到单纯性高血压患者
in_patients = involve_patient[involve_patient.time_label==1]
#删除不需要的列
to_drop = ['CITY_y', 'COUNTY_y', 'ID_CARD_y','MARRIED_y', 'NATION_y', 'NATIONALITY_y', 'OCCUPATION_y', 'ORG_CODE_y', 'PERSON_CODE_y', 'PERSON_ID_y', 'PROVINCE_y', 'SEX_y', 'UPLOAD_TIME_y', 'row_num_y','BIRTHDAY_y', 'BLOOD_y' ,'row_num_x']
in_patients.drop(columns=to_drop,inplace=True)

#清洗诊断
#妊娠高血压，难治性高血压，高血压I级，高血压2级，高血压3级，原发性高血压，继发性高血压。可以通过value_counts得到所有的高血压诊断，但是先对所有字段两端除掉空白字段

def picture_age(data):
    data['age'] = data['DIAG_TIME_x_x'] - data['BIRTHDAY_x']
    f = lambda time_delta: time_delta.days / 365
    data['age'] = data['age'].apply(f)
    plt.hist(data)
    plt.title("Age Histogram")
    plt.xlabel("Value")
    plt.ylabel("Frequency")

    fig = plt.gcf()
    plotly_fig = tls.mpl_to_plotly(fig)
    py.iplot(plotly_fig, filename='age-basic-histogram')
