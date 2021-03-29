# coding:utf-8
import requests
import json
import csv
import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession

from pyspark.sql.types import DecimalType


class Crawler:

    def parse_text(self, url):
        # 请求头
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:23.0) Gecko/20100101 Firefox/23.0'}

        # POST发送的data必须为bytes或bytes类型的可迭代对象，不能是字符串
        # form_data = urllib.parse.urlencode(form_data).encode()

        # url = 'http://www.nmc.cn/f/rest/province'
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
        with open(file, 'r') as f:
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
                    print(data)
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

    def passed_rain_analyse(self, filename):  # 计算各个城市过去24小时累积雨量
        print("begin to analyse passed rain")
        spark = SparkSession.builder.master("local").appName("passed_rain_analyse").getOrCreate()
        df = spark.read.csv(filename, header=True)
        df_rain = df.select(df['province'], df['city_name'], df['city_code'],
                            df['rain1h'].cast(DecimalType(scale=1))).filter(df['rain1h'] < 1000)  # 筛选数据，去除无效数据
        df_rain_sum = df_rain.groupBy("province", "city_name", "city_code").agg(F.sum("rain1h").alias("rain24h")).sort(
            F.desc("rain24h"))  # 分组、求和、排序
        df_rain_sum.cache()
        df_rain_sum.coalesce(1).write.csv("input/passed_rain_analyse1")
        print("end analysing passed rain")
        return df_rain_sum.head(20)


if __name__ == '__main__':
    # Crawler().run()
    Crawler().passed_rain_analyse("./input/passed_weather_ALL.csv")
