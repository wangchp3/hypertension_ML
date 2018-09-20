# encoding utf-8
# from test

# import pandas as pd
import datetime

class Node:
    def __init__(self, ch):
        self.ch = ''  # 字符
        self.word = None  # 词语
        self.child = {}  # 子树


d = {"（": "(", "）": ")"}


def change(ch):
    return d[ch] if ch in d else ch


class Tree:
    def __init__(self):
        self.root = Node('')

    def insert(self, word):
        cur = self.root
        for ch in word:
            ch = change(ch)
            if ch not in cur.child:
                cur.child[ch] = Node(ch)
            cur = cur.child[ch]
        cur.word = word

    def find(self, word2):
        cur = self.root
        for ch in word2:
            ch = change(ch)
            if ch in cur.child:
                cur = cur.child[ch]
            else:
                continue
        # 额外补充
        if not cur.word:
            if '片' in cur.child:
                cur = cur.child['片']
        return cur.word


def parse_ymd(s):
    s1 = s.split(' ')
    year_s, mon_s, day_s = s1[0].split('-')
    return datetime.datetime(int(year_s), int(mon_s), int(day_s))

#
# if __name__ == "__main__":
#     # 导入标准的ATC表格
#     all_name = pd.read_csv(r"all_name.csv", encoding='utf-8')
#
#     # 导入原始药品名称表
#     origin_df = pd.read_csv(r"medication.csv", encoding='utf-8')
#     # domestic.rename(columns={'ATC_modify': 'atc'}, inplace=True)
#     # imported.rename(columns={'ATC_modify': 'atc'}, inplace=True)
#     #
#     # atc_all = pd.concat([domestic, imported])
#     # atc_dict = dict(zip(atc_all.medical_name, atc_all.atc))
#
#     target = list(all_name.drop_duplicates("medical_name").medical_name.str.strip())
#     # target = list(target_df.medical_name)
#
#     origin = origin_df.ITEM_NAME.str.strip()
#     origin = list(origin)
#
#     item_name = []
#     standard_item_name = []
#
#     tree = Tree()
#     for word in target:
#         tree.insert(word)
#     for index, word in enumerate(origin):
#         if type(word) == type('str'):
#             item_name.append(word)
#             standard_medical_word = tree.find(word)
#             standard_item_name.append(standard_medical_word)
#             print("{}  {}-> {}".format(index, word, standard_medical_word))
#
#         else:
#             print("{}  {}-> {}".format(index, word, "空值"))
#     count_none= 1000-standard_item_name.count(None)
#     print("1000个中匹配上的有：{}".format(count_none))