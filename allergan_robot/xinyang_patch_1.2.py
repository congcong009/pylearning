# coding=utf-8
import pandas as pd
import sqlite3
import glob
import os
import numpy as np
import re

pd.set_option('expand_frame_repr', False)  # 当列太多时不换行

inputdir = r'/Users/congcong009/Downloads/爬虫2020/2020-2-21/安心购/'  #/Users/congcong009/Downloads/安心购/'  # E:/Hammer_Studio/tmp-project/咨询项目/艾尔建/爬虫项目/data_warehouse/新氧/'
df_empty = pd.DataFrame()
table = {ord(f): ord(t) for f, t in zip(u'，。！？【】（）％＃＠＆１２３４５６７８９０[]/-_#~•①②③④、&*●°@『』｛｝',
                                        u'++++++++++++1234567890++++++++++++++++++++++')}  # u',.!?++()%#@&1234567890')}
city_key = ['成都', '北京', '广州', '上海']  # ['成都', '北京', '西安', '昆明', '重庆', '广州', '上海']
project_key = ['除皱瘦脸', '玻尿酸', '脂肪填充', '眼部整形', '鼻部整形', '皮肤美容', '美体塑形', '面部轮廓', '胸部整形', '私密整形']
# ['双眼皮', '眼泡', '内眼角', '开眼角''鼻部', '脂肪', '隆胸', '除皱', '瘦脸', '填充', '线雕', '皮肤', '美体', '塑形', '玻尿酸', '自体', '隆鼻', '鼻尖', '鼻翼', '鼻头', '鼻基底', '塌鼻']
brand_key = ['保妥适', '衡力', '润百颜', '伊婉', '嗨体', '法思丽', '乔雅登', '姣兰', '海薇', '瑞蓝', '润致',
             '爱芙莱', '舒颜', '艾莉薇', '德蔓', '婕尔', '逸美', '贝丽姿', '菲洛嘉', '热玛吉', '奥昵', '馨妍', '玻菲']
# ['保妥适', '衡力', '无品牌', '润百颜', '伊婉', '嗨体', '法思丽', '乔雅登', '姣兰', '海薇', '瑞蓝', '润致', '爱芙莱', '舒颜', '艾莉薇', '德蔓', '婕尔', '逸美', '公主', '贝丽姿', '皮秒', '水光',
#              '菲洛嘉', '去妊娠纹', '激光祛斑', '热玛吉', '超声提升', '热提拉', '微针', '疤痕修复', '美白', '祛痘', '抗敏修复', '嫩肤', '注射祛疤', '深蓝射频', '点阵激光', '小气泡', '激光点痣', '焕肤', '射频紧肤',
#              '手术祛疤', '射频美肤', '水氧活肤', '激光修复', '玻尿酸导入', '果酸焕肤', '娇兰', '奥昵', '馨妍', '玻菲']
dose_key = ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0', 'ml', '单位', '.']  # ['1', '2', '3', '4', '5', '6', '7', '8', '9', '0', '价', 'ml', '单位', '.']

for parents, dirnames, filenames in os.walk(inputdir):
    for filename in filenames:
        # df = pd.read_excel(os.path.join(parents, filename))
        df = pd.read_csv(os.path.join(parents, filename))  #
        col_name = df.columns.tolist()
        print(col_name)
        col_name.insert(0, '区域')
        col_name.insert(1, '采集时间')
        col_name.insert(2, '项目')
        col_name.insert(3, '品牌')
        # col_name.insert(4, '剂量')
        df = df.reindex(columns=col_name)
        print(col_name)
        df['区域'] = str(re.findall('|'.join(city_key), filename))
        df['采集时间'] = str(filename[:9])
        print(df)
        # exit()
        for i in range(df.shape[0]):
            # df.iloc[i, 2] = str(re.findall('|'.join(project_key), str(df.iloc[i, 4])))
            df.iloc[i, 2] = str(re.findall('|'.join(project_key), filename))
            df.iloc[i, 3] = str(re.findall('|'.join(brand_key), str(df.iloc[i, 4])))
            # df.iloc[i, 4] = str(re.findall('|'.join(dose_key), str(df.iloc[i])))
            df.iloc[i, 0] = df.iloc[i, 0].translate(table)
            df.iloc[i, 0] = df.iloc[i, 0].replace('+', '')
            df.iloc[i, 2] = df.iloc[i, 2].translate(table)
            df.iloc[i, 2] = df.iloc[i, 2].replace('+', '')
            df.iloc[i, 3] = df.iloc[i, 3].translate(table)
            df.iloc[i, 3] = df.iloc[i, 3].replace('+', '')
            # df.iloc[i, 3] = df.iloc[i, 3].replace('\s+', '')
            # df.iloc[i, 4] = df.iloc[i, 4].translate(table)
            # df.iloc[i, 5] = df.iloc[i, 5].translate(table)
            # df.iloc[i, 3] = df.iloc[i].str.replace(', ', '')
            # df.iloc[i] = df.iloc[i].str.replace('\'', '')
        print(df)
        # exit()
        df_empty = df_empty.append(df, ignore_index=True)
        df_empty = df_empty.loc[:, ~df_empty.columns.str.contains("^Unnamed")]
# print(df.loc[:, '标题'])
print(df_empty)
con = sqlite3.connect("/Users/congcong009/Downloads/爬虫2020/agn.db")
df_empty.to_sql('xinyang0221', con)
exit()
