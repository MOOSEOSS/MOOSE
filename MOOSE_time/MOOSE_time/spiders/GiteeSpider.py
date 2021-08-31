import scrapy
import json
import requests
from MOOSE_time.items import *

from influxdb import InfluxDBClient
import time
import re
import random
import pymysql
import os
import emoji
from MOOSE_time import settings
from MOOSE_time.spiders.request_header import *
from bs4 import BeautifulSoup
import re
import base64
import csv
import datetime
import time
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

requests.packages.urllib3.disable_warnings()
requests.adapters.DEFAULT_RETRIES = 5

def dbHandle():
    conn = pymysql.connect(
        host=settings.MYSQL_HOST,
        db=settings.MYSQL_DBNAME,
        user=settings.MYSQL_USER,
        passwd=settings.MYSQL_PASSWD,
        charset='utf8',
        use_unicode=True
    )
    return conn

class GiteeTimeSpider(scrapy.Spider):
    name = 'MOOSE_Gitee'

    def __init__(self, repo=None, *args, **kwargs):
        super(GiteeTimeSpider, self).__init__(*args, **kwargs)

    client = InfluxDBClient('ip:ip:ip:ip', 8086, 'username', 'password', 'dbname')#ip port username password database
    #client.query("drop measurement moose_issue")
    #client.query("delete from  moose_pull where community_id=24")
    #exit(0)
    '''
    client.query("drop measurement moose_index")
    client.query("drop measurement moose_issue")
    client.query("drop measurement moose_commit")
    client.query("drop measurement moose_comment")
    client.query("drop measurement moose_issue_comment")
    client.query("drop measurement moose_index_detail")
    client.query("drop measurement moose_pull")
    exit(0)
    '''
    '''
    #查询
    result_issue = client.query("select * from moose_issue where  oss_id='39469487';").get_points()
    issue_user = dict()
    for aa in result_issue:
        print(aa)
    #获取用户数量排名
    exit(0)
    '''
    proxy = '218.203.132.117:808'
    proxies = {
        'http': 'http://175.4.68.43:8118',
        'https': 'https://218.203.132.117:808',
    }
    '''
    result_issue = client.query("select * from moose_pull where  oss_id='74175805';").get_points()
    issue_user = dict()
    num = 0
    temp_date = ''
    f = open('result/train2.csv', 'w', newline='')
    csv_writer = csv.writer(f)
    csv_writer.writerow(
        ['Datetime', 'Count'])
    for aa in result_issue:
        pull_date = aa['time'][:10]
        if temp_date == '':
            temp_date = pull_date
        if temp_date == pull_date:
            num += 1
        else:
            temp_date = pull_date
            csv_writer.writerow([pull_date, num])
            num = 1
        print(pull_date)
        #if aa['user_id'] in issue_user.keys():
        #    issue_user[aa['user_id']] += 1
        #else:
        #    issue_user.update({aa['user_id']:0})
    #a = sorted(issue_user.items(), key=lambda item: item[1], reverse=True)
    #print(num)
    exit(0)
    '''

    scrapyed_list = []
    handle_httpstatus_list = [401, 404, 500]
    oss_event_id = dict()
    community_id = dict()
    early_date = "2013-08-01"#“2020-05-01”
    fmt = '%Y-%m-%d'
    early_date = datetime.datetime.strptime(str(early_date), fmt)

    dbObject = dbHandle()
    cursor = dbObject.cursor()
    with open("repo_churn_gitee.txt", "r") as f:  #repo_test_gitee.txt
        for line in f.readlines():
            line = line.strip('\n')  # 去掉列表中每一个元素的换行符
            oss_id = int(line.split('\t')[1])
            oss_name = line.split('\t')[0]

            oss_event_id.update({oss_id: []})
            scrapyed_list.append("https://gitee.com/api/v5/repos/" + oss_name)
            cursor.execute("select event_id, issue_id, pullrequest_id, commit_time, comment_id, issue_comment_id, fork_id from moose_event_id where oss_id='" + str(oss_id) + "'")
            last_oss_event_id = cursor.fetchone()
            if last_oss_event_id != None and len(last_oss_event_id)>0:
                for per_last_id in last_oss_event_id:
                    if per_last_id is None or per_last_id=='':
                        per_last_id = 0
                    oss_event_id[oss_id].append(per_last_id)
            else:
                cursor.execute("insert into  moose_event_id  (oss_id) values ('" + str( oss_id) + "')")
                dbObject.commit()
                for i in range(7):#将event_id, issue_id, pullrequest_id, commit_time, comment_id, issue_comment_id, fork_id均设为0
                    oss_event_id[oss_id].append(0)
            community_id[oss_id] = 30   ### github为31，gitee为30
    repo_index = len(scrapyed_list)-1

    start_urls = [scrapyed_list[repo_index]]

    #start_urls = ['https://api.github.com/repos/istio/istio']

    def parse(self, response):
        if response.status in self.handle_httpstatus_list:
            self.repo_index = self.repo_index + 1
            if self.repo_index <= len(self.scrapyed_list) - 1:
                yield scrapy.Request(self.scrapyed_list[self.repo_index], meta={"is_first": 1}, callback=self.parse,
                                     headers=getGiteeHeader())
        try:
            repos_data = json.loads(response.body.decode('utf-8'))
            oss_id = repos_data['id']
            oss_members = repos_data['members']
            collaborators_url = repos_data['collaborators_url'][0:-15]
            contributors_url = repos_data['contributors_url']

            #获取collaborators
            s = requests.session()
            s.keep_alive = False
            collaborators_html = requests.get(collaborators_url, headers=getGiteeHeader(), verify=False).text
            collaborators_info = json.loads(collaborators_html)
            oss_collaborators = []
            for collaborator in collaborators_info:
                oss_collaborators.append(collaborator['login'])

            #获取contributors
            s = requests.session()
            s.keep_alive = False
            contributors_html = requests.get(contributors_url, headers=getGiteeHeader(), verify=False).text
            contributors_info = json.loads(contributors_html)
            oss_contributors_name = []  #注意是name不是login
            for contributor in contributors_info:
                oss_contributors_name.append(contributor['name'])

            # issues data crawl
            issues_url = repos_data["issues_url"][0:-9] + "?state=all&sort=created&direction=desc&per_page=100"
            yield scrapy.Request(issues_url, meta={"oss_id": oss_id,"oss_members":oss_members, "oss_collaborators":oss_collaborators,
                                                   "oss_contributors_name":oss_contributors_name}, callback=self.parse_issue, headers=getGiteeHeader())

            # issues comment data crawl
            issues_comment_url = repos_data["issue_comment_url"][0:-9] + "?sort=created&direction=desc&per_page=100"
            yield scrapy.Request(issues_comment_url, meta={"oss_id": oss_id,"oss_members":oss_members, "oss_collaborators":oss_collaborators,
                                                   "oss_contributors_name":oss_contributors_name}, callback=self.parse_issue_comment, headers=getGiteeHeader())

            # pulls data crawl
            pulls_url = repos_data["pulls_url"][0:-9] + "?state=all&sort=created&direction=desc&per_page=100"
            yield scrapy.Request(pulls_url, meta={"oss_id": oss_id, "pull_url": repos_data["pulls_url"][0:-9],"oss_members": oss_members, "oss_collaborators": oss_collaborators,
                                                   "oss_contributors_name": oss_contributors_name}, callback=self.parse_pullrequest, headers=getGiteeHeader())

            # commit data crawl
            commit_url = repos_data['commits_url'][0:-6] + "?sort=updated&direction=desc&per_page=100"
            yield scrapy.Request(commit_url, meta={"oss_id": oss_id}, callback=self.parse_commit, headers=getGiteeHeader())

            # commit comment data crawl

            commit_comment_url = repos_data["comments_url"][0:-9] + "?page=1000&per_page=100"
            commit_comment_html = requests.get(commit_comment_url, headers=getGiteeHeader(), verify=False)
            commit_comment_header = commit_comment_html.headers
            listLink_prev_url = re.findall(r'(?<=<).[^<]*(?=>; rel=\'last)', str(commit_comment_header))#\"prev改为\'last,直接从最后一页开始爬取
            if len(listLink_prev_url) > 0:
                yield scrapy.Request(listLink_prev_url[0], meta={"oss_id": oss_id, "commit_comment_url": repos_data["comments_url"][0:-9],"oss_members":oss_members, "oss_collaborators":oss_collaborators,
                                                   "oss_contributors_name":oss_contributors_name}, callback=self.parse_commit_comment, headers=getGiteeHeader())

            # index data crawl
            #event_url = repos_data["events_url"]
            #yield scrapy.Request(event_url, meta={"oss_id": oss_id}, callback=self.parse_event, headers=getGiteeHeader())

            # fork data crawl
            fork_url = repos_data["forks_url"] + "?per_page=100"
            yield scrapy.Request(fork_url, meta={"oss_id": oss_id}, callback=self.parse_fork, headers=getGiteeHeader())

            #star data crawl
            star_url = repos_data['stargazers_url']+"?per_page=100" #去掉内容：sort=starred_at&direction=desc&
            yield scrapy.Request(star_url, meta={"oss_id": oss_id}, callback=self.parse_star, headers=getGiteeHeader())
        except:
            pass

        finally:
            self.repo_index = self.repo_index - 1
            if self.repo_index >= 0:
                yield scrapy.Request(self.scrapyed_list[self.repo_index], meta={"is_first": 1}, callback=self.parse, headers=getGiteeHeader())

    def parse_issue(self, response):
        repos_data = json.loads(response.body.decode('utf-8'))
        repos_header = response.headers
        oss_id = response.meta['oss_id']
        oss_members = response.meta['oss_members']
        oss_collaborators = response.meta['oss_collaborators']
        oss_contributors_name = response.meta['oss_contributors_name']
        try:
            is_first = response.meta['is_first']
        except Exception as ex:
            is_first = 1
        if is_first == 1:
            if len(repos_data) > 0:
                issue_id_last = repos_data[0]['id']
                issue_time_last = repos_data[0]['created_at'] #根据created时间降序排序的
                # store latest issue id
                try:
                    update_sql = "update moose_event_id set issue_id = %s , issue_time= %s where oss_id=%s"
                    self.cursor.execute(update_sql, (str(issue_id_last), str(issue_time_last), str(oss_id)))
                    self.dbObject.commit()
                except Exception as ex:
                    print(203)
                    print(ex)

        finish = 0
        for repos_per_data in repos_data:
            issue_id_current = repos_per_data['id']
            if int(self.oss_event_id[oss_id][1]) != 0 and int(self.oss_event_id[oss_id][1]) >= int(issue_id_current):
                finish = 1
                break
            '''if 'pull_request' in repos_per_data:
                continue'''
            create_time = repos_per_data['created_at']
            d1 = datetime.datetime.strptime(str(create_time[0:10]), self.fmt)
            if d1 < self.early_date:#不考虑早于early_date的issue
                break
            title = repos_per_data['title']
            issue_body = repos_per_data['body']
            if issue_body is None:
                issue_body = ''
            if repos_per_data['state'] == 'closed':
                issue_state = 1
                close_time = repos_per_data['finished_at']  #GitHub：closed_at
                if close_time is None:
                    close_time = ''
                body = [
                    {
                        "measurement": "moose_issue_close",#关闭的issue
                        "time": close_time,
                        "tags": {
                            "oss_id": oss_id,
                            "community_id": self.community_id[oss_id],
                            "issue_id": issue_id_current
                        },
                        "fields": {
                            "create_time": create_time,
                        },
                    }
                ]
                res = self.client.write_points(body)
            else:
                issue_state = 0
                close_time = ''

            #在members、collaborators、contributors中查找，判断issue_user_type
            issue_user_type = 'NONE'
            issue_user_login = repos_per_data['user']['login']
            is_member=0
            for member_login in oss_members:
                if member_login == issue_user_login:
                    issue_user_type='MEMBER'
                    is_member = 1
                    break
            if is_member ==0:
                is_collaborator = 0
                for collaborator_login in oss_collaborators:
                    if collaborator_login == issue_user_login:
                        issue_user_type = 'COLLABORATOR'
                        is_collaborator = 1
                        break
                if is_collaborator == 0:
                    issue_user_name = repos_per_data['user']['name']
                    is_contributor = 0
                    for contributor_name in oss_contributors_name:
                        if contributor_name == issue_user_name:
                            is_contributor = 1
                            issue_user_type = 'CONTRIBUTOR'
                            break
                    if is_contributor == 0:
                        issue_user_type = 'NONE'
            #print("issue_user_type:",issue_user_type)
            #issue_user_type = repos_per_data['author_association']
            if issue_user_type == 'MEMBER' or issue_user_type == 'COLLABORATOR':
                core_issue = 1
            else:
                core_issue = 0

            issue_comment_count = repos_per_data['comments']
            if issue_comment_count is None:
                issue_comment_count = 0
            #获取number
            number = str(repos_per_data['number'])
            #获取labels
            labels_str = ''
            labels = repos_per_data['labels']
            if labels is not None and len(labels) > 0:
                for per_label in labels:
                    labels_str += per_label['name'] + ","
            labels_str = labels_str[0:-1]
            user_id = repos_per_data['user']['id']
            # statistic user

            # user_exit = self.is_user_exit(repos_per_data['user']['id'])
            # if (user_exit == 1):
            #     User_Info_Repo_item = MOOSEUserRepo()
            #     User_Info_Repo_item['user_id'] = repos_per_data['user']['id']
            #     User_Info_Repo_item['oss_id'] = oss_id
            #     User_Info_Repo_item['user_type'] = issue_user_type
            #     yield User_Info_Repo_item
            # else:
            #     owner_url = repos_per_data['user']['url']
            #     yield scrapy.Request(owner_url, meta={"user_type": issue_user_type, "oss_id": oss_id}, callback=self.user_parse, headers=getGiteeHeader())
            body = [
                {
                    "measurement": "moose_issue",
                    "time": create_time,
                    "tags": {
                        "oss_id": oss_id,
                        "community_id": self.community_id[oss_id],
                        "issue_id": issue_id_current
                    },
                    "fields": {
                        "number": number,
                        "core_issue": core_issue,
                        "issue_state": issue_state,
                        "title": title,
                        "body": issue_body,
                        "issue_comment_count": issue_comment_count,
                        "close_time": close_time,
                        "user_id": user_id,
                        "labels": labels_str,
                        "user_name":issue_user_login #########2021-08-30添加
                    },
                }
            ]
            res = self.client.write_points(body)
        if finish != 1:
            listLink_next_url = re.findall(r'(?<=<).[^<]*(?=>; rel=\'next)', str(repos_header)) #\"next改为\'next
            if len(listLink_next_url) > 0:
                yield scrapy.Request(listLink_next_url[0], meta={"is_first": 2, "oss_id": oss_id}, callback=self.parse_issue, headers=getGiteeHeader())

    def parse_issue_comment(self, response):
        repos_data = json.loads(response.body.decode('utf-8'))
        repos_header = response.headers
        oss_id = response.meta['oss_id']
        oss_members = response.meta['oss_members']
        oss_collaborators = response.meta['oss_collaborators']
        oss_contributors_name = response.meta['oss_contributors_name']
        try:
            is_first = response.meta['is_first']
        except Exception as ex:
            is_first = 1
        if is_first == 1:
            if len(repos_data) > 0:
                issue_comment_id_last = repos_data[0]['id']
                issue_comment_time_last = repos_data[0]['created_at']
                # store latest issue comment id
                try:
                    update_sql = "update moose_event_id set issue_comment_id = %s , issue_comment_time= %s where oss_id=%s"
                    self.cursor.execute(update_sql, (str(issue_comment_id_last), str(issue_comment_time_last), str(oss_id)))
                    self.dbObject.commit()
                except Exception as ex:
                    print(ex)
        finish = 0
        sid = SentimentIntensityAnalyzer()
        for repos_per_data in repos_data:
            issue_comment_id_current = repos_per_data['id']
            if int(self.oss_event_id[oss_id][5]) != 0 and int(self.oss_event_id[oss_id][5]) >= int(issue_comment_id_current):
                finish = 1
                break
            create_time = repos_per_data['created_at']
            d1 = datetime.datetime.strptime(str(create_time[0:10]), self.fmt)
            if d1 < self.early_date:
                finish = 1
                break
            issue_comment_body = repos_per_data['body']
            if issue_comment_body is None:
                issue_comment_body = ''
            #分析body的极性
            label = sid.polarity_scores(issue_comment_body)['compound']
            if label >= 0.3:
                polarity = 'positive'
            elif label <= -0.3:
                polarity = 'negative'
            else:
                polarity = 'neutral'
            #提取issue_number
            #issue_url = repos_per_data['issue_url']
            try:
                #issue_number = issue_url.split('/')[-1]
                issue_number = str(repos_per_data['target']['issue']['number'])
            except Exception as ex:
                print(342)
                print(ex)
                issue_number = '1'
            # 在members、collaborators、contributors中查找，判断issue_comment_user_type
            issue_comment_user_type = 'NONE'
            issue_user_login = repos_per_data['user']['login']
            is_member = 0
            for member_login in oss_members:
                if member_login == issue_user_login:
                    issue_comment_user_type = 'MEMBER'
                    is_member = 1
                    break
            if is_member == 0:
                is_collaborator = 0
                for collaborator_login in oss_collaborators:
                    if collaborator_login == issue_user_login:
                        issue_comment_user_type = 'COLLABORATOR'
                        is_collaborator = 1
                        break
                if is_collaborator == 0:
                    issue_user_name = repos_per_data['user']['name']
                    is_contributor = 0
                    for contributor_name in oss_contributors_name:
                        if contributor_name == issue_user_name:
                            is_contributor = 1
                            issue_comment_user_type = 'CONTRIBUTOR'
                            break
                    if is_contributor == 0:
                        issue_comment_user_type = 'NONE'
            # print("issue_comment_user_type:",issue_comment_user_type)
            #issue_comment_user_type = repos_per_data['author_association']
            if issue_comment_user_type == 'MEMBER' or issue_comment_user_type == 'COLLABORATOR':
                core_issue_comment = 1
            else:
                core_issue_comment = 0

            user_id = repos_per_data['user']['id']
            # statistic user

            # user_exit = self.is_user_exit(repos_per_data['user']['id'])
            # if (user_exit == 1):
            #     User_Info_Repo_item = MOOSEUserRepo()
            #     User_Info_Repo_item['user_id'] = repos_per_data['user']['id']
            #     User_Info_Repo_item['oss_id'] = oss_id
            #     User_Info_Repo_item['user_type'] = issue_comment_user_type
            #     yield User_Info_Repo_item
            # else:
            #     owner_url = repos_per_data['user']['url']
            #     yield scrapy.Request(owner_url, meta={"user_type": issue_comment_user_type, "oss_id": oss_id}, callback=self.user_parse, headers=getGiteeHeader())
            body = [
                {
                    "measurement": "moose_issue_comment",
                    "time": create_time,
                    "tags": {
                        "oss_id": oss_id,
                        "community_id": self.community_id[oss_id],
                        "issue_comment_id": issue_comment_id_current
                    },
                    "fields": {
                        "core_issue_comment": core_issue_comment,
                        "body": issue_comment_body,
                        "polarity": polarity,
                        "user_id": user_id,
                        "issue_number": issue_number,
                        "user_name": issue_user_login  #########2021-08-30添加
                    },
                }
            ]
            res = self.client.write_points(body)
        if finish != 1:
            listLink_next_url = re.findall(r'(?<=<).[^<]*(?=>; rel=\'next)', str(repos_header)) #\"next改为\'next
            if len(listLink_next_url) > 0:
                yield scrapy.Request(listLink_next_url[0], meta={"is_first": 2, "oss_id": oss_id}, callback=self.parse_issue_comment, headers=getGiteeHeader())

    def parse_pullrequest(self, response):
        repos_data = json.loads(response.body.decode('utf-8'))
        repos_header = response.headers
        oss_id = response.meta['oss_id']
        oss_members = response.meta['oss_members']
        oss_collaborators = response.meta['oss_collaborators']
        oss_contributors_name = response.meta['oss_contributors_name']
        pull_url = response.meta['pull_url']
        try:
            is_first = response.meta['is_first']
        except Exception as ex:
            is_first = 1
        if is_first == 1:
            if len(repos_data) > 0:
                pull_id_last = repos_data[0]['id']
                pull_time_last = repos_data[0]['created_at']
                # store latest pr id
                try:
                    update_sql = "update moose_event_id set pullrequest_id = %s , pullrequest_time= %s where oss_id=%s"
                    self.cursor.execute(update_sql, (str(pull_id_last), str(pull_time_last), str(oss_id)))
                    self.dbObject.commit()
                except Exception as ex:
                    print(407)
                    print(ex)
        finish = 0
        for repos_per_data in repos_data:
            try:
                pull_id_current = repos_per_data['id']
                pull_no = str(repos_per_data['number'])
                if int(self.oss_event_id[oss_id][2]) != 0 and int(self.oss_event_id[oss_id][2]) >= int(pull_id_current):
                    finish = 1
                    break
                create_time = repos_per_data['created_at']
                d1 = datetime.datetime.strptime(str(create_time[0:10]), self.fmt)
                if d1 < self.early_date:
                    finish = 1
                    break
                title = repos_per_data['title']
                pull_body = repos_per_data['body']
                if pull_body is None:
                    pull_body = ''
                if repos_per_data['state'] == 'open':
                    pull_state = 0
                elif repos_per_data['state'] == 'closed':
                    pull_state = 1
                else:   #merged
                    pull_state = 2

                close_time = repos_per_data['closed_at']
                if repos_per_data['merged_at'] is None:
                    pull_merged = 0
                    merged_time = ''
                else:
                    pull_merged = 1
                    merged_time = repos_per_data['merged_at']
                    body = [
                        {
                            "measurement": "moose_pull_merged",
                            "time": merged_time,
                            "tags": {
                                "oss_id": oss_id,
                                "community_id": self.community_id[oss_id],
                                "pull_id": pull_id_current,
                            },
                            "fields": {
                                "create_time": create_time,
                            }
                        }
                    ]
                    res = self.client.write_points(body)

                # 在members、collaborators、contributors中查找，判断pull_user_type
                pull_user_type = 'NONE'
                pull_user_login = repos_per_data['user']['login']
                is_member = 0
                for member_login in oss_members:
                    if member_login == pull_user_login:
                        pull_user_type = 'MEMBER'
                        is_member = 1
                        break
                if is_member == 0:
                    is_collaborator = 0
                    for collaborator_login in oss_collaborators:
                        if collaborator_login == pull_user_login:
                            pull_user_type = 'COLLABORATOR'
                            is_collaborator = 1
                            break
                    if is_collaborator == 0:
                        pull_user_name = repos_per_data['user']['name']
                        is_contributor = 0
                        for contributor_name in oss_contributors_name:
                            if contributor_name == pull_user_name:
                                is_contributor = 1
                                pull_user_type = 'CONTRIBUTOR'
                                break
                        if is_contributor == 0:
                            pull_user_type = 'NONE'
                # print("pull_user_type:",pull_user_type)
                #pull_user_type = repos_per_data['author_association']
                if pull_user_type == 'MEMBER' or pull_user_type == 'COLLABORATOR':
                    core_pull = 1
                else:
                    core_pull = 0

                # 进入详情页，计算comment数量和review comment数量
                s = requests.session()
                s.keep_alive = False
                # pull_detail_url = pull_url + "/" + str(pull_no)
                # pull_detail_html = requests.get(pull_detail_url, headers=getGiteeHeader(), verify=False).text
                # pull_datail_html_info = json.loads(pull_detail_html)
                # if (pull_datail_html_info is not None and len(pull_datail_html_info) > 0 and "message" not in pull_datail_html_info):
                #     pull_comment_count = pull_datail_html_info['comments']
                #     if pull_comment_count is None:
                #         pull_comment_count = 0
                #     pull_review_comment_count = pull_datail_html_info['review_comments']
                #     if pull_review_comment_count is None:
                #         pull_review_comment_count = 0
                # else:
                #     pull_comment_count = 0
                #     pull_review_comment_count = 0
                pull_comment_url=pull_url + '/' + str(pull_no) + '/comments'
                pull_comment_html=requests.get(pull_comment_url, headers=getGiteeHeader(), verify=False).text
                pull_comment_info=json.loads(pull_comment_html)
                if (pull_comment_info is not None and "message" not in pull_comment_info):
                    pull_comment_count=len(pull_comment_info)
                else:
                    print("null")
                    pull_comment_count =0
                pull_review_comment_count = pull_comment_count

                '''注意Gitee没有单独的review的信息，只有review_comments的信息'''
                # 查询review
                # s = requests.session()
                # s.keep_alive = False
                # pull_review_url = pull_url + "/" + str(pull_no) + "/reviews"
                # pull_review_html = requests.get(pull_review_url, headers=getGiteeHeader(), verify=False).text
                # pull_review_html_info = json.loads(pull_review_html)
                # core_review_count = 0
                # if (pull_review_html_info is not None and len(pull_review_html_info) > 0 and "message" not in pull_review_html_info):
                #     pull_reviewed = 1
                #     for review in pull_review_html_info:
                #         review_user_type = review['author_association']
                #         if review_user_type == 'MEMBER' or review_user_type == 'COLLABORATOR':
                #             core_review_count += 1
                #         review_date = review['submitted_at']
                #         review_id = review['id']
                #         user_id = review['user']['id']
                #         body = [
                #             {
                #                 "measurement": "moose_review",
                #                 "time": review_date,
                #                 "tags": {
                #                     "oss_id": oss_id,
                #                     "community_id": self.community_id[oss_id],
                #                     "pull_id": pull_id_current,
                #                     "review_id": review_id
                #                 },
                #                 "fields": {
                #                     "user_type": review_user_type,
                #                     "user_id": user_id
                #                 }
                #             }
                #         ]
                #         res = self.client.write_points(body)
                # else:
                #     pull_reviewed = 0
                core_review_count=0 ####### gitee没有review的信息，设置core_review_count为 0
                if pull_review_comment_count>0: ###根据review comments个数判断是否已经审查
                    pull_reviewed = 1
                else:
                    pull_reviewed = 0

                # 查询review comment
                core_review_comment_count = 0
                #pull_review_comment_count = 1

                if pull_review_comment_count > 0:
                    s = requests.session()
                    s.keep_alive = False
                    pull_review_comment_url = pull_url + "/" + str(pull_no) + "/comments"
                    pull_review_comment_html = requests.get(pull_review_comment_url, headers=getGiteeHeader(), verify=False).text
                    pull_review_comment_html_info = json.loads(pull_review_comment_html)
                    if (pull_review_comment_html_info is not None and len(pull_review_comment_html_info) > 0 and "message" not in pull_review_comment_html_info):
                        for review_comment in pull_review_comment_html_info:
                            # 在members、collaborators、contributors中查找，判断review_comment_user_type
                            review_comment_user_type = 'NONE'
                            review_user_login = review_comment['user']['login']
                            is_member = 0
                            for member_login in oss_members:
                                if member_login == review_user_login:
                                    review_comment_user_type = 'MEMBER'
                                    is_member = 1
                                    break
                            if is_member == 0:
                                is_collaborator = 0
                                for collaborator_login in oss_collaborators:
                                    if collaborator_login == review_user_login:
                                        review_comment_user_type = 'COLLABORATOR'
                                        is_collaborator = 1
                                        break
                                if is_collaborator == 0:
                                    review_user_name = review_comment['user']['name']
                                    is_contributor = 0
                                    for contributor_name in oss_contributors_name:
                                        if contributor_name == review_user_name:
                                            is_contributor = 1
                                            review_comment_user_type = 'CONTRIBUTOR'
                                            break
                                    if is_contributor == 0:
                                        pull_user_type = 'NONE'
                            # print("review_comment_user_type:", review_comment_user_type)
                            # review_comment_user_type = review_comment['author_association']
                            if review_comment_user_type == 'MEMBER' or review_comment_user_type == 'COLLABORATOR':
                                core_review_comment_count += 1
                            review_comment_date = review_comment['created_at']
                            review_comment_id = review_comment['id']
                            review_id = 0   #review_comment['pull_request_review_id']###Gitee没有该信息
                            user_id = review_comment['user']['id']
                            body = [
                                {
                                    "measurement": "moose_review_comment",
                                    "time": review_comment_date,
                                    "tags": {
                                        "oss_id": oss_id,
                                        "community_id": self.community_id[oss_id],
                                        "review_comment_id": review_comment_id,
                                        "pull_id": pull_id_current,
                                        "review_id": review_id
                                    },
                                    "fields": {
                                        "user_type": review_comment_user_type,
                                        "user_id": user_id,
                                        "user_name": review_user_login  #########2021-08-30添加
                                    }
                                }
                            ]
                            res = self.client.write_points(body)
                    else:
                        core_review_comment_count = 0

                s = requests.session()
                s.keep_alive = False
                # statistic user

                # user_exit = self.is_user_exit(repos_per_data['user']['id'])
                # if (user_exit == 1):
                #     User_Info_Repo_item = MOOSEUserRepo()
                #     User_Info_Repo_item['user_id'] = repos_per_data['user']['id']
                #     User_Info_Repo_item['oss_id'] = oss_id
                #     User_Info_Repo_item['user_type'] = pull_user_type
                #     yield User_Info_Repo_item
                # else:
                #     owner_url = repos_per_data['user']['url']
                #     yield scrapy.Request(owner_url, meta={"user_type": pull_user_type, "oss_id": oss_id}, callback=self.user_parse, headers=getGiteeHeader())

                user_id = repos_per_data['user']['id']

                body = [
                    {
                        "measurement": "moose_pull",
                        "time": create_time,
                        "tags": {
                            "oss_id": oss_id,
                            "community_id": self.community_id[oss_id],
                            "pull_id": pull_id_current,
                            "pull_no":pull_no
                        },
                        "fields": {
                            "core_pull": core_pull,
                            "pull_state": pull_state,
                            "pull_merged": pull_merged,
                            "pull_reviewed": pull_reviewed,
                            "title": title,
                            "body": pull_body,
                            "pull_comment_count": pull_comment_count,
                            "pull_review_comment_count": pull_review_comment_count,
                            "close_time": close_time,
                            "merged_time": merged_time,
                            "user_id": user_id,
                            "core_review_count": core_review_count,
                            "core_review_comment_count": core_review_comment_count,
                            "user_name": pull_user_login  #########2021-08-30添加
                        },
                    }
                ]
                res = self.client.write_points(body)
            except Exception as ex:
                print(595)
                print(ex)
        if finish != 1:
            listLink_next_url = re.findall(r'(?<=<).[^<]*(?=>; rel=\'next)', str(repos_header)) #\"next改为\'next
            if len(listLink_next_url) > 0:
                yield scrapy.Request(listLink_next_url[0], meta={"is_first": 2, "oss_id": oss_id, "pull_url": pull_url}, callback=self.parse_pullrequest, headers=getGiteeHeader())

    def parse_event(self, response):
        repos_data = json.loads(response.body.decode('utf-8'))
        repos_header = response.headers
        oss_id = response.meta['oss_id']
        try:
            is_first = response.meta['is_first']
        except Exception as ex:
            is_first = 1
        try:
            if is_first == 1:
                if len(repos_data) > 0:
                    event_id_last = repos_data[0]['id']
                    event_time_last = repos_data[0]['created_at']
                    # store latest event id
                    update_sql = r"update moose_event_id set event_id = " + str(
                        event_id_last) + ", event_time='" + event_time_last + "' where oss_id=" + str(oss_id)
                    self.cursor.execute(update_sql)
                    self.dbObject.commit()

            index_dict = dict()
            finish = 0
            for per_event in repos_data:
                event_id_current = per_event['id']
                oss_id = per_event['repo']['id']
                if int(self.oss_event_id[oss_id][0]) != 0 and int(self.oss_event_id[oss_id][0]) >= int(event_id_current):
                    finish = 1
                    break

                event_type = per_event['type']
                event_time = per_event['created_at'][:10]
                # try:
                #     action = per_event['payload']['action']
                # except:
                #     action = 'none'
                action = 'none'##################

                try:
                    event_user = per_event['actor']['id']
                    event_user_login = per_event['actor']['login']
                except:
                    event_user = 0
                    event_user_login = ''
                #all event
                body = [
                    {
                        "measurement": "moose_index_detail",
                        "time": per_event['created_at'],
                        "tags": {
                            "oss_id": oss_id,
                            "event_id": event_id_current,
                            "community_id": self.community_id[oss_id],
                            "index_type": event_type
                        },
                        "fields": {
                            "action": action,
                            "user_id": event_user,
                            "user_name":event_user_login ########2021-08-30添加
                        },
                    }
                ]
                res = self.client.write_points(body)


                #event count
                if event_type in index_dict:
                    if event_time in index_dict[event_type]:
                        index_dict[event_type][event_time] += 1
                    else:
                        index_dict[event_type].update({event_time: 1})
                else:
                    index_dict.update({event_type: {event_time: 1}})
            for index_type in index_dict:
                for index_time in index_dict[index_type]:
                    query = "select * from moose_index where index_type='" + index_type + "' and time='" + index_time + "' and oss_id='" + str(oss_id) + "' ;"
                    result = self.client.query(query).get_points()
                    index_count = index_dict[index_type][index_time]
                    try:
                        for point in result:
                            index_count += point[u'index_count']#############??????????????
                    except Exception as ex:
                        index_count = index_dict[index_type][index_time]
                    body = [
                        {
                            "measurement": "moose_index",
                            "time": index_time,
                            "tags": {
                                "oss_id": oss_id,
                                "community_id": self.community_id[oss_id],
                                "index_type": index_type
                            },
                            "fields": {
                                "index_count": index_count
                            },
                        }
                    ]
                    res = self.client.write_points(body)
            if finish != 1:
                listLink_next_url = re.findall(r'(?<=<).[^<]*(?=>; rel=\'next)', str(repos_header)) #\"next改为\'next
                if len(listLink_next_url) > 0:
                    yield scrapy.Request(listLink_next_url[0], meta={"is_first": 2,"oss_id": oss_id}, callback=self.parse_event,
                                         headers=getGiteeHeader())
        except Exception as ex:
            print(701)
            print(ex)

    def parse_commit(self, response):
        repos_data = json.loads(response.body.decode('utf-8'))
        repos_header = response.headers
        oss_id = response.meta['oss_id']
        try:
            is_first = response.meta['is_first']
        except Exception as ex:
            is_first = 1
        commit_node_last =''
        commit_time_last =''
        if is_first == 1:
            if len(repos_data) > 0:
                commit_node_last = repos_data[0]['url'] ### Gitee没有node_id，用url代替
                commit_time_last = repos_data[0]['commit']['author']['date']
                # # store latest commit id
                # try:
                #     update_sql = "update moose_event_id set commit_id = %s , commit_time= %s where oss_id=%s"
                #     self.cursor.execute(update_sql, (str(commit_node_last), str(commit_time_last), str(oss_id)))
                #     self.dbObject.commit()
                # except Exception as ex:
                #     print(717)
                #     print(ex)
        finish = 0
        for repos_per_data in repos_data:
            commit_node = repos_per_data['url'] ### Gitee没有node_id，用url代替
            commit_time = repos_per_data['commit']['author']['date']
            if is_first==1 and commit_time>commit_time_last:
                # print("update commit ")
                commit_node_last=commit_node
                commit_time_last=commit_time
            d1 = datetime.datetime.strptime(str(commit_time[0:10]), self.fmt)
            if d1 < self.early_date:
                finish = 1
                break
            if self.oss_event_id[oss_id][3] != 0 and self.oss_event_id[oss_id][3] >= commit_time:
                finish = 1
                break
            message = repos_per_data['commit']['message']
            if message is None:
                message = ''
            try:
                user_id = repos_per_data['author']['id']
                user_name = repos_per_data['author']['login']
            except:
                user_id = 0
                user_name = ''

            body = [
                {
                    "measurement": "moose_commit",
                    "time": commit_time,
                    "tags": {
                        "oss_id": oss_id,
                        "community_id": self.community_id[oss_id],
                        "commit_node": commit_node
                    },
                    "fields": {
                        "message": message,
                        "user_id": user_id,
                        "user_name": user_name
                    },
                }
            ]
            res = self.client.write_points(body)
        if is_first==1 and commit_node_last!='' and commit_time_last!='':
            # print(commit_node_last,commit_time_last)
            # store latest commit id
            try:
                update_sql = "update moose_event_id set commit_id = %s , commit_time= %s where oss_id=%s"
                self.cursor.execute(update_sql, (str(commit_node_last), str(commit_time_last), str(oss_id)))
                self.dbObject.commit()
            except Exception as ex:
                print(717)
                print(ex)
        if finish != 1:
            listLink_next_url = re.findall(r'(?<=<).[^<]*(?=>; rel=\'next)', str(repos_header))
            if len(listLink_next_url) > 0:
                yield scrapy.Request(listLink_next_url[0], meta={"is_first": 2, "oss_id": oss_id}, callback=self.parse_commit, headers=getGiteeHeader())

    def parse_commit_comment(self, response):
        repos_data = json.loads(response.body.decode('utf-8'))
        repos_header = response.headers
        oss_id = response.meta['oss_id']
        oss_members = response.meta['oss_members']
        oss_collaborators = response.meta['oss_collaborators']
        oss_contributors_name = response.meta['oss_contributors_name']
        #从最后一页爬取
        # if repos_data=='' or len(repos_data)==0:
        #     listLink_next_url = re.findall(r'(?<=<).[^<]*(?=>; rel=\'last)', str(repos_header)) #\"prev改为\'last
        #     if len(listLink_next_url) > 0:
        #         scrapy.Request(listLink_next_url[0],meta={"is_first": 1, "oss_id": oss_id},
        #             callback=self.parse_commit_comment, headers=getGiteeHeader())
        # 本就直接从最后一页爬取，不需要这一段。就算加上也不会死循环：scrapy 的request逻辑里面  dont_filter=False，也就是重复网页不爬取

        try:
            is_first = response.meta['is_first']
        except Exception as ex:
            is_first = 1
        if is_first == 1:
            if len(repos_data) > 0:
                comment_id_last = repos_data[len(repos_data)-1]['id']
                comment_time_last = repos_data[len(repos_data)-1]['created_at']
                # store latest issue id
                try:
                    update_sql = "update moose_event_id set comment_id = %s , comment_time= %s where oss_id=%s"
                    self.cursor.execute(update_sql, (str(comment_id_last), str(comment_time_last), str(oss_id)))
                    self.dbObject.commit()
                except Exception as ex:
                    print(784)
                    print(ex)
        finish = 0
        for i in range(len(repos_data)-1, -1, -1):#倒数
            comment_id = repos_data[i]['id']
            comment_time = repos_data[i]['created_at']
            d1 = datetime.datetime.strptime(str(comment_time[0:10]), self.fmt)
            if d1 < self.early_date:
                finish = 1
                break
            if int(self.oss_event_id[oss_id][4]) != 0 and int(self.oss_event_id[oss_id][4]) >= int(comment_id):
                finish = 1
                break
            commment_body = repos_data[i]['body']
            if commment_body is None:
                commment_body = ''

            # 在members、collaborators、contributors中查找，判断commit_comment_user_type
            commit_comment_user_type = 'NONE'
            commit_user_login = repos_data[i]['user']['login']
            is_member = 0
            for member_login in oss_members:
                if member_login == commit_user_login:
                    commit_comment_user_type = 'MEMBER'
                    is_member = 1
                    break
            if is_member == 0:
                is_collaborator = 0
                for collaborator_login in oss_collaborators:
                    if collaborator_login == commit_user_login:
                        commit_comment_user_type = 'COLLABORATOR'
                        is_collaborator = 1
                        break
                if is_collaborator == 0:
                    commit_user_name = repos_data[i]['user']['name']
                    is_contributor = 0
                    for contributor_name in oss_contributors_name:
                        if contributor_name == commit_user_name:
                            is_contributor = 1
                            commit_comment_user_type = 'CONTRIBUTOR'
                            break
                    if is_contributor == 0:
                        commit_comment_user_type = 'NONE'
            # print("commit_comment_user_type:", commit_comment_user_type)
            # commit_comment_user_type = repos_data[i]['author_association']
            if commit_comment_user_type == 'MEMBER' or commit_comment_user_type == 'COLLABORATOR':
                core_commit_comment = 1
            else:
                core_commit_comment = 0

            try:
                user_id = repos_data[i]['user']['id']
                user_login = repos_data[i]['user']['login']
            except:
                user_id = 0
                user_login = ''
            body = [
                {
                    "measurement": "moose_comment",
                    "time": comment_time,
                    "tags": {
                        "oss_id": oss_id,
                        "community_id": self.community_id[oss_id],
                        "comment_id": comment_id
                    },
                    "fields": {
                        "body": commment_body,
                        "user_id": user_id,
                        "core_commit_comment": core_commit_comment,
                        "user_name":user_login ########2021-08-30
                    },
                }
            ]
            res = self.client.write_points(body)
        if finish != 1:
            listLink_next_url = re.findall(r'(?<=<).[^<]*(?=>; rel=\'prev)', str(repos_header))
            if len(listLink_next_url) > 0:
                yield scrapy.Request(listLink_next_url[0], meta={"is_first": 2, "oss_id": oss_id}, callback=self.parse_commit_comment, headers=getGiteeHeader())

    def parse_fork(self, response):
        repos_data = json.loads(response.body.decode('utf-8'))
        repos_header = response.headers
        oss_id = response.meta['oss_id']
        try:
            is_first = response.meta['is_first']
        except Exception as ex:
            is_first = 1
        fork_id_last = 0
        fork_time_last = ''
        if is_first == 1:
            if len(repos_data) > 0:
                fork_id_last = repos_data[0]['id']
                fork_time_last = repos_data[0]['created_at']###############???????????sorted???
        #         # store latest issue id
        #         try:
        #             update_sql = "update moose_event_id set fork_id = %s , fork_time= %s where oss_id=%s"
        #             self.cursor.execute(update_sql, (str(fork_id_last), str(fork_time_last), str(oss_id)))
        #             self.dbObject.commit()
        #         except Exception as ex:
        #             print(845)
        #             print(ex)

        finish = 0
        for repos_per_data in repos_data:
            fork_id = repos_per_data['id']
            fork_time = repos_per_data['created_at']
            if is_first==1 and fork_time > fork_time_last:#通过遍历第一页找到最新的fork_time
                # print("update fork_time")
                # print("update fork_id")
                fork_id_last = fork_id
                fork_time_last =fork_time
            if int(self.oss_event_id[oss_id][6]) != 0 and int(self.oss_event_id[oss_id][6]) >= int(fork_id):
                finish = 1
                break
            fork_full_name = repos_per_data['full_name']
            user_id = repos_per_data['owner']['id']##########2021-08-22
            user_login = repos_per_data['owner']['login']##########2021-08-30
            body = [
                {
                    "measurement": "moose_fork",
                    "time": fork_time,
                    "tags": {
                        "oss_id": oss_id,
                        "community_id": self.community_id[oss_id],
                        "fork_id": fork_id
                    },
                    "fields": {
                        "fullname": fork_full_name,
                        "user_id":user_id,
                        "user_name":user_login
                    },
                }
            ]
            res = self.client.write_points(body)
        if is_first==1 and fork_id_last!=0 and fork_time_last!='':#通过遍历第一页找到最新的fork_time
            #print(fork_id_last,fork_time_last)
            try:
                update_sql = "update moose_event_id set fork_id = %s , fork_time= %s where oss_id=%s"
                self.cursor.execute(update_sql, (str(fork_id_last), str(fork_time_last), str(oss_id)))
                self.dbObject.commit()
            except Exception as ex:
                print(845)
                print(ex)
        if finish != 1:
            listLink_next_url = re.findall(r'(?<=<).[^<]*(?=>; rel=\'next)', str(repos_header))
            if len(listLink_next_url) > 0: yield scrapy.Request(listLink_next_url[0], meta={"is_first": 2, "oss_id": oss_id}, callback=self.parse_fork, headers=getGiteeHeader())

    def parse_star(self, response):
        repos_data = json.loads(response.body.decode('utf-8'))
        repos_header = response.headers
        oss_id = response.meta['oss_id']

        for repos_per_data in repos_data:
            # star_time = repos_per_data['star_at']
            # user_id = repos_per_data['user']['id']
            # user_name = repos_per_data['user']['login']
            star_time = repos_per_data['star_at']
            user_id = repos_per_data['id']
            user_name = repos_per_data['login']
            body = [
                {
                    "measurement": "moose_star",
                    "time": star_time,
                    "tags": {
                        "oss_id": oss_id,
                        "community_id": self.community_id[oss_id],
                        "user_id": user_id
                    },
                    "fields": {
                        "user_name": user_name,
                    },
                }
            ]
            res = self.client.write_points(body)

        listLink_next_url = re.findall(r'(?<=<).[^<]*(?=>; rel=\'next)', str(repos_header))
        if len(listLink_next_url) > 0: yield scrapy.Request(listLink_next_url[0], meta={"oss_id": oss_id}, callback=self.parse_star, headers=getGiteeHeader())

    def user_parse(self, response):
        oss_id = response.meta['oss_id']
        user_type = response.meta['user_type']
        repos_data = json.loads(response.body.decode('utf-8'))
        User_Info_item = MOOSEUser()
        User_Info_item['user_id'] = repos_data['id']
        User_Info_item['user_name'] = repos_data['login']
        if repos_data['name'] != None:
            User_Info_item['user_fullname'] = repos_data['name']
        else:
            User_Info_item['user_fullname'] = repos_data['login']
        User_Info_item['avatar_url'] = repos_data['avatar_url']
        try:
            User_Info_item['follows_count'] = repos_data['followers']
        except BaseException as e:
            User_Info_item['follows_count'] = 0
        User_Info_item['repos_count'] = repos_data['public_repos']
        User_Info_item['blog_url'] = str(repos_data['blog'])
        User_Info_item['location'] = ''
        User_Info_item['email_url'] = str(repos_data['email'])
        User_Info_item['company'] = str(repos_data['company'])
        User_Info_item['org_member_count'] = 0
        User_Info_item['user_type'] = repos_data['type']
        User_Info_item['user_create_time'] = repos_data['created_at']
        User_Info_item['update_time'] = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        User_Info_item['user_update_time'] = repos_data['updated_at']
        yield User_Info_item

        User_Info_Repo_item = MOOSEUserRepo()
        User_Info_Repo_item['user_id'] = repos_data['id']
        User_Info_Repo_item['oss_id'] = oss_id
        User_Info_Repo_item['user_type'] = user_type
        yield User_Info_Repo_item

    def is_user_exit(self, uid):
        dbObject = dbHandle()
        cursor = dbObject.cursor()
        cursor.execute("select * from moose_user where user_id=%s", (uid))
        result = cursor.fetchone()
        if result:
            return 1
        else:
            return 0