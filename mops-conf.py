import re
import os
import sys
import time
import codecs
import random
import shutil
import smtplib
import requests
import itertools
import configparser
import pandas as pd
from bs4 import BeautifulSoup as BS
from chardet import detect
from datetime import datetime, date, timedelta
from urlextract import URLExtract
from dateutil.relativedelta import relativedelta

def _load_config():
    config_path = "./config.ini"
    with open(config_path, "rb") as ef:
        config_encoding = detect(ef.read())["encoding"]
    config = configparser.ConfigParser()
    config.read_file(codecs.open(config_path, "r", config_encoding))
    return config

def _requests_session(config, status_forcelist=(500, 502, 504), session=None):
    session = requests.session()
    headers = {"user-agent": config["Requests_header"]["user-agent"]}
    session.headers.update(headers)
    return session

def multiple_replace(sub_dict, text):
     # Create a regular expression  from the dictionary keys
    regex = re.compile("(%s)" % "|".join(map(re.escape, sub_dict.keys())))

     # For each match, look-up corresponding value in dictionary
    return regex.sub(lambda mo: sub_dict[mo.string[mo.start():mo.end()]], text)

def date_to_datetime(x):
    return datetime(x.year, x.month, x.day)

def isHoliday(json_object, date_str):
    return [obj for obj in json_object if obj["date"]==date_str][0]["isHoliday"]

class mops_conf:
    def __init__(self):
        self.config = _load_config()
        self.dir_set()
        self.date_set()
        self.path_set()
        self.xq_path = self.get_xq_path()

    def is_tmw_holiday(self):
        tomorrow = date.today() + timedelta(days=1)
        tw_cal_url = "https://cdn.jsdelivr.net/gh/ruyut/TaiwanCalendar/data/{}.json".format(str(tomorrow.year))
        rs = _requests_session(self.config)
        tw_cal = rs.get(tw_cal_url).json()
        tmw_str = datetime.strftime(tomorrow, "%Y%m%d")
        return isHoliday(tw_cal, tmw_str)

    def hyperlink(self, text, url):
        return '<a href="{}">{}</a>'.format(url, text)

    def hyper_url(self, text):
        if text == "":
            return ""
        extractor = URLExtract()
        hyper_dict = list(map(self.hyperlink, extractor.find_urls(text), extractor.find_urls(text)))
        if not hyper_dict:
            return text
        rep = dict(zip(extractor.find_urls(text), hyper_dict))
        return multiple_replace(rep, text)

    def dir_set(self):
        self.conf_dir = "./conf"
        self.new_dir = "./new"
        self.msg_dir = "./msg"
        dir_list = [self.conf_dir, self.new_dir, self.msg_dir]
        for i in dir_list:
            os.makedirs(i, exist_ok=True)

    def date_set(self):
        if 0 <= datetime.now().hour < 9:
            self.date_sub = date.today()
            self.date_last = date.today() - timedelta(days=1)
        else:
            self.date_sub = date.today() + timedelta(days=1)
            self.date_last = date.today()

        conf_list = []
        if not os.listdir(self.conf_dir):
            print("沒更新過")
            self.date_previous = self.date_last
        else:
            for file in os.listdir(self.conf_dir):
                conf_list.append(re.search("\d{4}-\d{2}-\d{2}", file)[0])
            conf_list.sort(reverse=False)
            conf_date = [datetime.strptime(d, "%Y-%m-%d") for d in conf_list]

            if conf_date[-1].month == self.date_last.month:
                if conf_date[-1] < date_to_datetime(self.date_last):
                    self.date_previous = conf_date[-1]
                elif conf_date[-1] == date_to_datetime(self.date_last) and len(conf_date) == 1:
                    self.date_previous = self.date_last
                else:
                    self.date_previous = conf_date[-2]
            else:
                self.date_previous = conf_date[-1]

    def path_set(self):
        self.previous_path = "{}/conf-future-{}.html".format(self.conf_dir, self.date_previous.strftime("%Y-%m-%d"))
        self.last_path = "{}/conf-future-{}.html".format(self.conf_dir, self.date_last.strftime("%Y-%m-%d"))
        self.new_path = "{}/conf-new-{}.html".format(self.new_dir, self.date_last.strftime("%Y-%m-%d"))
        self.msg_path = "{}/conf-msg-{}.html".format(self.msg_dir, self.date_last.strftime("%Y-%m-%d"))

    def get_xq_path(self):
        xq_dir = "./xq"
        if not os.path.exists(xq_dir):
            print("沒有xq公司簡介")
            sys.exit()
        elif not os.listdir(xq_dir):
            print("沒有xq公司簡介")
            sys.exit()
        else:
            xq_list = os.listdir(xq_dir)
            self.xq_date = datetime.strptime(re.search("\d{4}-\d{2}\d{2}", xq_list[-1])[0], "%Y-%m%d").date()
        xq_list.sort(reverse=False)
        return "{}/{}".format(xq_dir, xq_list[-1])

    def html_utf8_convert(self, file_path):
        re_dict = {
            "&lt;": "<",
            "&gt;": ">"
        }
        with open(file_path, "rb") as ef:
            input_encoding = detect(ef.read())["encoding"]
            if input_encoding.lower() == "big5":
                input_encoding = "cp950"
        temp_path = "{}.tmp".format(file_path)
        with open(file_path, "r", encoding=input_encoding) as sourceFile, \
            open(temp_path, "w", encoding="utf-8") as targetFile:
            contents = sourceFile.read()
            targetFile.write(multiple_replace(re_dict, contents))
        os.remove(file_path)
        shutil.move(temp_path, file_path)

    def html_table_colorize(self, file_path):
        with open(file_path, "rb") as inf:
            soup = BS(inf.read(), "lxml")
        soup.html.insert(0, soup.new_tag("head"))
        soup.head.append(soup.new_tag("style", type="text/css"))
        soup.head.style.append("table thead th{background-color: #165C98; color: #FFFFFF;}")
        soup.head.style.append("table tbody tr:nth-child(odd){background-color: #F6F6F6;}")
        soup.head.style.append("table tbody tr:nth-child(even){background-color: #EBEBEB;}")
        soup.thead.tr["style"] = "text-align: center;"
        with open(file_path, "w", encoding="utf-8") as save:
            save.write(soup.prettify())

    def post_payload(self, market_type, payload_datetime):
        payload = {
            "encodeURIComponent": "1",
            "step": "1",
            "firstin": "1",
            "off": "1",
            "TYPEK": market_type,
            "year": payload_datetime.year - 1911,
            "month": "{:02d}".format(payload_datetime.month),
            "co_id": ""
        }
        return payload

    def get_conf(self, payload):
        url = "https://mopsov.twse.com.tw/mops/web/t100sb02_1"
        res = self.rs.post(url, data=payload)
        soup = BS(res.text, "lxml")

        if not soup.find_all("table", {"class": "hasBorder"}):
            return
        else:
            df = pd.read_html(soup.prettify(), header=None, attrs = {"class": "hasBorder"})[0]
            df.columns = df.columns.droplevel()
            cols = ["代號", "名稱", "法說日期", "法說時間", "法說地點", "法說訊息", \
                "中文簡報", "英文簡報", \
                "公司網站相關資訊", "影音連結", "其他應敘明事項", "歷年法說"]
            if [i for i in df.columns if i.startswith("影音連結")]:
                df.columns = cols
            else:
                df.columns = [i for i in cols if not i.startswith("影音連結")]
            df["代號"] = df["代號"].astype("str")
            df = df[~(df["代號"] == "公司代號")]
            df["temp"] = df["法說日期"].str[-9:]
            df["temp"] = df["temp"].apply(lambda x: "{}{}".format(str(int(x[:3])+1911), x[3:]))
            df["temp"] = pd.to_datetime(df["temp"], format="%Y/%m/%d")
            df_future = df[df["temp"] >=  pd.Timestamp(date.today())]
            df_temp = df_future.fillna("", inplace=False)

            df_c = df_future.copy()
            df_c["中文簡報"] = df_future["中文簡報"].apply(lambda x: self.hyperlink(x, "{}{}".format("http://mopsov.twse.com.tw/nas/STR/", x)) if "內容" not in x else "")
            df_c["英文簡報"] = df_future["英文簡報"].apply(lambda x: self.hyperlink(x, "{}{}".format("http://mopsov.twse.com.tw/nas/STR/", x)) if "內容" not in x else "")
            df_c["公司網站相關資訊"] = df_future["公司網站相關資訊"].apply(lambda x: self.hyperlink(x, "{}".format(x, x)) if "無" not in x else x)
            if [i for i in df_temp.columns if i.startswith("影音連結")]:
                df_c["影音連結"] = df_temp["影音連結"].apply(lambda x: self.hyper_url(x))
            df_c.drop(labels=["歷年法說"], axis=1, inplace=True)
            df_c["代號"] = df_c["代號"].astype("int64")

        return df_c

    def coming_conf(self):
        min_sleep = float(self.config["Sleep_time"]["min"])
        max_sleep = float(self.config["Sleep_time"]["max"])

        payload = []
        markets = ["sii", "otc", "rotc", "pub"]
        dates = [datetime.now(), datetime.now() + relativedelta(months=1)]
        for market, payload_date in list(itertools.product(markets, dates)):
            payload.append(self.post_payload(market, payload_date))

        self.rs = _requests_session(self.config)
        dfs = []
        sleep = 0
        for post_data in payload:
            dfs.append(self.get_conf(post_data))
            if sleep < len(payload)-1:
                time.sleep(random.uniform(min_sleep, max_sleep))
                sleep += 1

        df_conf = [x for x in dfs if x is not None]
        if len(df_conf) == 1:
            df_future = df_conf[0]
        elif len(df_conf) > 1:
            df_future = pd.concat(df_conf, ignore_index=True)
        else:
            print("近月無資料")
            sys.exit()
        df_future.sort_values(["temp", "法說時間"], ascending=[True, True], inplace=True)
        df_future.drop(labels=["temp"], axis=1, inplace=True)

        return df_future

    def xq_merge(self, df):
        dfxq = pd.read_csv(self.xq_path, encoding="utf-8-sig", engine="python")
        dfxq.drop(labels=["商品"], axis=1, inplace=True)
        dfx = df.merge(dfxq, left_on="代號", right_on="代碼", how="left", indicator=False)
        dfx.drop(labels=["代碼"], axis=1, inplace=True)

        dfx.to_html(self.last_path, index=False)
        self.html_utf8_convert(self.last_path)
        self.html_table_colorize(self.last_path)

    def get_addition_conf(self):
        df_1 = pd.read_html(self.last_path, encoding="utf-8")[0]
        df_2 = pd.read_html(self.previous_path, encoding="utf-8")[0]
        df_1_2 = df_1.merge(df_2, on="代號", how="left", indicator=True)
        df_1_not_2 = df_1_2[df_1_2["_merge"] == "left_only"]
        df_1_not_2.columns = df_1_not_2.columns.str.replace("_x", "")
        df_1_not_2 = df_1_not_2.drop(list(df_1_not_2.filter(regex = "_")), axis = 1, inplace = False).fillna("", inplace=False)

        df_c = df_1_not_2.copy()
        df_c["中文簡報"] = df_1_not_2["中文簡報"].apply(lambda x: self.hyperlink(x, "{}{}".format("http://mopsov.twse.com.tw/nas/STR/", x)) if "內容" not in x else "")
        df_c["英文簡報"] = df_1_not_2["英文簡報"].apply(lambda x: self.hyperlink(x, "{}{}".format("http://mopsov.twse.com.tw/nas/STR/", x)) if "內容" not in x else "")
        df_c["公司網站相關資訊"] = df_1_not_2["公司網站相關資訊"].apply(lambda x: self.hyperlink(x, "{}".format(x, x)) if "無" not in x else x)
        df_c["影音連結"] = df_1_not_2["影音連結"].apply(lambda x: self.hyper_url(x))

        df_c.to_html(self.new_path, index=False)
        self.html_utf8_convert(self.new_path)
        self.html_table_colorize(self.new_path)

    def html_concat(self, update_time):
        with open(self.new_path, "r", encoding="utf-8") as inf:
            soup_0 = BS(inf.read(), "lxml")
        with open(self.last_path, "r", encoding="utf-8") as inf:
            soup_1 = BS(inf.read(), "lxml")

        update_text = datetime.strftime(update_time, "%Y-%m-%d %H:%M")
        if self.date_previous == self.date_last:
            previous_text = "無"
        else:
            previous_text = datetime.strftime(self.date_previous, "%Y-%m-%d")
        xq_text = datetime.strftime(self.xq_date, "%Y-%m-%d")

        soup_0.body.insert(0, soup_0.new_tag("div"))

        soup_0.body.div.append("法說更新時間: {}".format(update_text))
        soup_0.body.div.append(soup_0.new_tag("br"))
        soup_0.body.div.append("上次更新時間: {}".format(previous_text))
        soup_0.body.div.append(soup_0.new_tag("br"))
        soup_0.body.div.append("簡介更新時間: {}".format(xq_text))
        soup_0.body.div.append(soup_0.new_tag("br"))
        soup_0.body.div.append(soup_0.new_tag("br"))

        b = soup_0.new_tag("b")
        b.append("新增")
        soup_0.body.div.append(b)

        d = soup_0.new_tag("div")
        d.append(soup_0.new_tag("br"))
        d.append(soup_0.new_tag("br"))
        b = soup_0.new_tag("b")
        b.append("近月")
        d.append(b)
        soup_0.body.append(d)

        x = soup_1.body.table
        soup_0.body.append(x)

        with open(self.msg_path, "w", encoding="utf-8") as save:
            save.write(soup_0.prettify())

    def mail(self):
        with open(self.msg_path, "r", encoding="utf-8") as inf:
            html = inf.read()

        to_list = self.config["SMTP"]["to"].replace(" ", "").split(",")
        ccto_list = self.config["SMTP"]["ccto"].replace(" ", "").split(",")
        bccto_list = self.config["SMTP"]["bccto"].replace(" ", "").split(",")

        # Import the email modules
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText

        # Define email addresses to use
        addr_to = ",".join(to_list)    # 注意，不是分號
        addr_cc = ",".join(ccto_list)
        # addr_bcc = ",".join(bccto_list)
        addr_from = self.config["SMTP"]["from"]

        receive = to_list
        receive.extend(ccto_list)
        receive.extend(bccto_list)

        # Define SMTP email server details
        smtp_server = self.config["SMTP"]["smtp_server"]
        smtp_user   = self.config["SMTP"]["smtp_user"]
        smtp_pass   = self.config["SMTP"]["smtp_pass"]

        # Construct email
        msg = MIMEMultipart("alternative")

        msg["To"] = addr_to
        msg["CC"] = addr_cc
        msg["From"] = addr_from
        msg["Subject"] = "{}-新增法說".format(self.date_sub.strftime("%Y-%m-%d"))

        part = MIMEText(html, "html")

        # Attach parts into message container.
        # According to RFC 2046, the last part of a MIMEMultipart message, in this case
        # the HTML message, is best and preferred.
        msg.attach(part)

        # Send the message via an SMTP server
        s = smtplib.SMTP_SSL(smtp_server, 465)
        s.ehlo()
        s.login(smtp_user, smtp_pass)
        s.sendmail(addr_from, receive, msg.as_string())
        s.quit()
        print("Email sent!\n")

    def process(self):
        if self.is_tmw_holiday():
            print("Tomorrow is holiday!")
        else:
            time.sleep(random.uniform(0, 300))
            start_time = datetime.now().replace(microsecond=0)
            update_time = datetime.now()
            df_coming = self.coming_conf()
            self.xq_merge(df_coming)
            self.get_addition_conf()
            self.html_concat(update_time)
            self.mail()
            print("==完成 花費時間: {}==".format(str(datetime.now().replace(microsecond=0) - start_time)))

mops = mops_conf()
mopsov.process()