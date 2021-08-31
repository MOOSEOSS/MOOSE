from MOOSE_time.items import *
import pymysql
from MOOSE_time import settings
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

class MooseTimePipeline(object):
    def process_item(self, item, spider):
        dbObject = dbHandle()
        cursor = dbObject.cursor()
        if isinstance(item, MOOSEUser):
            cursor.execute("select * from moose_user where user_id=%s", (item['user_id']))
            result = cursor.fetchone()
            if result:
                sql = "update moose_user set user_name=%s , user_fullname =%s , " \
                      "avatar_url=%s,follows_count=%s,repos_count=%s ,blog_url=%s ,email_url=%s ," \
                      "org_member_count=%s,user_type =%s,user_create_time=%s ,user_update_time=%s ,update_time =%s ," \
                      "user_location = %s,user_company = %s where user_id =%s"
                try:
                    cursor.execute(sql, (item['user_name'], item['user_fullname'],
                                         item['avatar_url'], item['follows_count'], item['repos_count'],
                                         item['blog_url'], item['email_url'],
                                         item['org_member_count'], item['user_type'], item['user_create_time'],
                                         item['user_update_time'], item['update_time'], item['location'],
                                         item['company'],
                                         item['user_id']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()

                #pass
            else:
                sql = "insert into moose_user(user_id , user_name , user_fullname , " \
                      "avatar_url,follows_count,repos_count ,blog_url ,email_url ," \
                      "org_member_count,user_type ,user_create_time ,user_update_time ,update_time," \
                      "user_location,user_company)" \
                      " VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
                try:

                    cursor.execute(sql, (item['user_id'], item['user_name'], item['user_fullname'],
                                         item['avatar_url'], item['follows_count'], item['repos_count'],
                                         item['blog_url'], item['email_url'],
                                         item['org_member_count'], item['user_type'], item['user_create_time'],
                                         item['user_update_time'], item['update_time'], item['location'],
                                         item['company']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()
        elif isinstance(item, MOOSEUserRepo):
            cursor.execute("select * from moose_user_repo where user_id=%s and oss_id = %s", (item['user_id'],item['oss_id']))
            result = cursor.fetchone()
            if result:
                sql = "update moose_user_repo set user_type=%s  where user_id =%s and oss_id = %s"
                try:
                    cursor.execute(sql, (item['user_type'], item['user_id'], item['oss_id']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()
            else:
                sql = "insert into moose_user_repo(user_id , oss_id , user_type )" \
                      " VALUES(%s,%s,%s)"
                try:
                    cursor.execute(sql, (item['user_id'], item['oss_id'], item['user_type']))
                    cursor.connection.commit()
                except BaseException as e:
                    print(e)
                    dbObject.rollback()
        return item
