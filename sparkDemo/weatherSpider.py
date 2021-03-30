# coding:utf-8
import math
import time

import requests
import json
import csv
import os

import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType

from matplotlib.font_manager import FontProperties
import matplotlib.pyplot as plt


class Crawler:

    def parse_text(self, url):
        # 请求头
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}

        # POST发送的data必须为bytes或bytes类型的可迭代对象，不能是字符串
        # form_data = urllib.parse.urlencode(form_data).encode()

        # url = 'http://www.nmc.cn/f/rest/province'
        requests.adapters.DEFAULT_RETRIES = 5
        request = requests.get(url=url, headers=headers)
        return request.text

    def parse_json(self, url):
        obj = self.parse_text(url)
        if obj:
            json_obj = json.loads(obj)
        else:
            json_obj = list()
        return json_obj

    def write_csv(self, file, data):
        if data:
            print("begin to write to " + file)
            with open(file, 'a+', encoding='utf8', newline='') as f:
                f_csv = csv.DictWriter(f, data[0].keys())
                f_csv.writeheader()  # 写入头
                f_csv.writerows(data)
            print("end to write to " + file)

    def write_header(self, file, data):
        if data:
            print("begin to write to " + file)
            with open(file, 'a+', encoding='utf8', newline='') as f:
                f_csv = csv.DictWriter(f, data[0].keys())
                f_csv.writeheader()  # 写入头
                f_csv.writerows(data)
            print("end to write to " + file)

    def write_row(self, file, data):
        if data:
            print("begin to write to " + file)
            with open(file, 'a+', encoding='utf8', newline='') as f:
                f_csv = csv.DictWriter(f, data[0].keys())
                f_csv.writerows(data)
            print("end to write to " + file)

    def read_csv(self, file):
        print("begin to read " + file)
        with open(file, 'r', encoding='utf8') as f:
            data = csv.DictReader(f)
            print("end to read " + file)
            return list(data)

    def get_provinces(self):
        province_file = 'input/province.csv'
        if not os.path.exists(province_file):
            print("begin crawl province")
            provinces = self.parse_json('http://www.nmc.cn/f/rest/province')
            print("end crawl province")
            self.write_csv(province_file, provinces)
        else:
            provinces = self.read_csv(province_file)
        return provinces

    def get_cities(self):
        city_file = 'input/city.csv'
        if not os.path.exists(city_file):
            cities = list()
            print("begin crawl city")
            for province in self.get_provinces():
                print(province['name'])
                url = province['url'].split('/')[-1].split('.')[0]
                cities.extend(self.parse_json('http://www.nmc.cn/f/rest/province/' + url))
            self.write_csv(city_file, cities)
            print("end crawl city")
        else:
            cities = self.read_csv(city_file)
        return cities

    def get_passed_weather(self, province):
        weather_passed_file = 'input/passed_weather_' + province + '.csv'
        if os.path.exists(weather_passed_file):
            return
        passed_weather = list()
        count = 0
        if province == 'ALL':
            print("begin crawl passed weather")
            for city in self.get_cities():
                print(city['province'] + ' ' + city['city'] + ' ' + city['code'])
                data = self.parse_json('http://www.nmc.cn/f/rest/passed/' + city['code'])
                if data:
                    count = count + 1
                    for item in data:
                        item['city_code'] = city['code']
                        item['province'] = city['province']
                        item['city_name'] = city['city']
                        item['city_index'] = str(count)
                    # print(data)
                    passed_weather.extend(data)
                # time.sleep(1)
                if count % 50 == 0:
                    if count == 50:
                        self.write_header(weather_passed_file, passed_weather)
                    else:
                        self.write_row(weather_passed_file, passed_weather)
                    passed_weather = list()
            if passed_weather:
                if count <= 50:
                    self.write_header(weather_passed_file, passed_weather)
                else:
                    self.write_row(weather_passed_file, passed_weather)
            print("end crawl passed weather")
        else:
            print("begin crawl passed weather")
            select_city = filter(lambda x: x['province'] == province, self.get_cities())
            for city in select_city:
                print(city['province'] + ' ' + city['city'] + ' ' + city['code'])
                data = self.parse_json('http://www.nmc.cn/f/rest/passed/' + city['code'])
                if data:
                    count = count + 1
                    for item in data:
                        item['city_index'] = str(count)
                        item['city_code'] = city['code']
                        item['province'] = city['province']
                        item['city_name'] = city['city']
                    passed_weather.extend(data)
                # time.sleep(1)
            self.write_csv(weather_passed_file, passed_weather)
            print("end crawl passed weather")

    def run(self, range='ALL'):
        self.get_passed_weather(range)

    # 分析降水排名
    def passed_rain_analyse(self, filename):  # 计算各个城市过去24小时累积雨量
        print("begin to analyse passed rain")
        spark = SparkSession.builder.master("local").appName("passed_rain_analyse").getOrCreate()
        df = spark.read.csv(filename, header=True)
        df_rain = df.select(df['province'], df['city_name'], df['city_code'],
                            df['rain1h'].cast(DecimalType(scale=1))).filter(df['rain1h'] < 1000)  # 筛选数据，去除无效数据
        df_rain_sum = df_rain.groupBy("province", "city_name", "city_code").agg(F.sum("rain1h").alias("rain24h")).sort(
            F.desc("rain24h"))  # 分组、求和、排序
        df_rain_sum.cache()
        df_rain_sum.coalesce(1).write.csv("input/passed_rain_analyse")
        print("end analysing passed rain")
        return df_rain_sum.head(20)

    # 分析气温排名
    def passed_temperature_analyse(self, filename):
        print("begin to analyse passed temperature")
        spark = SparkSession.builder.master("local").appName("passed_temperature_analyse").getOrCreate()
        df = spark.read.csv(filename, header=True)
        df_temperature = df.select(  # 选择需要的列
            df['province'],
            df['city_name'],
            df['city_code'],
            df['temperature'].cast(DecimalType(scale=1)),
            F.date_format(df['time'], "yyyy-MM-dd").alias("date"),  # 得到日期数据
            F.hour(df['time']).alias("hour")  # 得到小时数据
        )
        # 筛选四点时次
        # df_4point_temperature = df_temperature.filter(df_temperature['hour'].isin([2, 8, 12, 20]))
        # df_4point_temperature.printSchema()
        df_avg_temperature = df_temperature.groupBy("province", "city_name", "city_code", "date") \
            .agg(F.count("temperature"), F.avg("temperature").alias("avg_temperature")) \
            .sort(F.asc("avg_temperature")) \
            .select("province", "city_name", "city_code", "date",
                    F.format_number('avg_temperature', 1).alias("avg_temperature"))
        df_avg_temperature.cache()
        avg_temperature_list = df_avg_temperature.collect()
        df_avg_temperature.coalesce(1).write.json("input/passed_temperature_analyse")
        print("end analysing passed temperature")
        return avg_temperature_list[0:10]

    # 数据可视化
    def draw_rain(self, rain_list):
        print("begin to draw the picture of passed rain")
        font = FontProperties(fname='input/ttf/simhei.ttf')  # 设置字体
        name_list = []
        num_list = []
        for item in rain_list:
            name_list.append(item.province[0:2] + '\n' + item.city_name)
            num_list.append(item.rain24h)
        index = [i for i in range(0, len(num_list))]
        rects = plt.bar(index, num_list, width=0.5, color=['r', 'g', 'b', 'r', 'g', 'b'])
        # plt.rcParams['figure.figsize'] = (25, 16)  # 单位是inches
        plt.xticks([i for i in index], name_list, fontproperties=font)
        plt.ylim(ymax=(int(max(num_list) + 20) / 100) * 100, ymin=0)
        plt.xlabel("城市", fontproperties=font)
        plt.ylabel("雨量", fontproperties=font)
        plt.title("过去24小时累计降雨量全国前20名", fontproperties=font)
        for rect in rects:
            height = rect.get_height()
            plt.text(rect.get_x() + rect.get_width() / 2, height + 1, str(height), ha="center", va="bottom")
        plt.savefig('input/passed_rain_analyse')
        plt.show()
        print("ending drawing the picture of passed rain")

    def draw_temperature(self, temperature_list):
        print("begin to draw the picture of passed temperature")
        font = FontProperties(fname='input/ttf/simhei.ttf')
        name_list = []
        num_list = []
        date = temperature_list[0].date
        for item in temperature_list:
            name_list.append(item.province[0:2] + '\n' + item.city_name)
            num_list.append(float(item.avg_temperature))
        index = [i for i in range(0, len(num_list))]
        rects = plt.bar(index, num_list, width=0.5, color=['r', 'g', 'b', 'r', 'g', 'b'])
        plt.xticks([i for i in index], name_list, fontproperties=font)
        plt.ylim(ymax=math.ceil(float(max(num_list)+10)), ymin=min(num_list))
        plt.xlabel("城市", fontproperties=font)
        plt.ylabel("日平均气温", fontproperties=font)
        plt.title(date + "全国日平均气温最低前10名", fontproperties=font)
        for rect in rects:
            height = rect.get_height()
            plt.text(rect.get_x() + rect.get_width() / 2, height + 0.1, str(height), ha="center", va="bottom")
        plt.savefig('input/passed_temperature_analyse')
        plt.show()
        print("ending drawing the picture of passed temperature")


if __name__ == '__main__':
    # Crawler().run()
    # Crawler().passed_rain_analyse("./input/passed_weather_ALL.csv")
    # Crawler().passed_temperature_analyse("./input/passed_weather_ALL.csv")
    # Crawler().draw_rain(Crawler().passed_rain_analyse("./input/passed_weather_ALL.csv"))
    Crawler().draw_temperature(Crawler().passed_temperature_analyse("./input/passed_weather_ALL.csv"))
