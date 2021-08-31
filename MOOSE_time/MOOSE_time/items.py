# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class MooseTimeItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    id = scrapy.Field()
    event_id = scrapy.Field()
    oss_id = scrapy.Field()
    event_time = scrapy.Field()
    issue_id = scrapy.Field()
    issue_time = scrapy.Field()
    pullrequest_id = scrapy.Field()
    pullrequest_time = scrapy.Field()
    commit_id = scrapy.Field()
    commit_time = scrapy.Field()


class MOOSEUser(scrapy.Item):
    user_id = scrapy.Field()   #用户id
    user_name = scrapy.Field()                #用户登陆姓名
    user_fullname = scrapy.Field()                #用户姓名全程
    avatar_url = scrapy.Field()               #头像地址
    follows_count = scrapy.Field()              #被关注数
    repos_count = scrapy.Field()                #项目数
    blog_url = scrapy.Field()                 #bolg地址
    email_url = scrapy.Field()                #emall地址
    belong_org = scrapy.Field()               #所属组织
    org_member_count = scrapy.Field()           # 组织会员数
    user_type = scrapy.Field()                  #类别 0 user 1 org 存字符
    user_create_time = scrapy.Field()
    user_update_time = scrapy.Field()
    update_time = scrapy.Field()
    location = scrapy.Field()
    company = scrapy.Field()

class MOOSEUserRepo(scrapy.Item):
    user_id = scrapy.Field()   #用户id
    oss_id = scrapy.Field()   #仓库id
    user_type = scrapy.Field()   #用户type

