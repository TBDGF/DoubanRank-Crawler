import traceback
from asyncio import sleep
import pymysql
from bs4 import BeautifulSoup
import aiohttp
import asyncio
from dateutil.parser import parse
from dateutil.relativedelta import relativedelta
import datetime
import PostSolution
import Secret
from pymysql.converters import escape_string

# 请求头
headers = {
    "User-Agent": Secret.USER_AGENT,
    "Cookie": Secret.COOKIE
}

# 数据库连接
conn = pymysql.connect(host="localhost", user=Secret.DB_USER, password=Secret.DB_PASSWORD, database="douban",
                       port=3306,
                       charset="utf8mb4")
cursor = conn.cursor()


async def insert_group_info(group_id, member, sum, page_cnt):
    sql = "REPLACE INTO `group_info`(group_id,group_member,group_comment,topic_count) VALUES(%s,%s,%s,%s)"
    para = (group_id, member, sum, page_cnt)
    try:
        cursor.execute(sql, para)
        conn.commit()
        print("insert finished")
    except:
        traceback.print_exc()
        conn.rollback()


async def delete_group_list_by_group_id(group_id):
    sql = """ DELETE FROM `group_list` WHERE `group_id`=%s """
    para = (group_id)
    try:
        cursor.execute(sql, para)
        conn.commit()
        print("delete finished")
    except:
        traceback.print_exc()
        conn.rollback()


async def replace_group_ranked():
    sql = """set @rank=0;
DELETE FROM group_ranked;
REPLACE into group_ranked SELECT info.* ,@rank:=@rank + 1 AS rank_no FROM (SELECT l.group_id,l.group_name,l.group_url,i.group_member,i.group_comment,i.update_time FROM group_list l INNER JOIN group_info i on l.group_id=i.group_id ORDER BY i.group_comment DESC) info;"""
    sql = escape_string(sql)
    try:
        cursor.execute(sql)
        conn.commit()
        print("replace finished")
    except:
        traceback.print_exc()
        conn.rollback()


async def delete_group_info_by_group_id(group_id):
    sql = """ DELETE FROM `group_info` WHERE `group_id`=%s """
    para = (group_id)
    try:
        cursor.execute(sql, para)
        conn.commit()
        print("delete finished")
    except:
        traceback.print_exc()
        conn.rollback()


async def get_group_id_list():
    sql = "SELECT `group_id` FROM `group_list`"
    result = []
    try:
        cursor.execute(sql)
        origin = cursor.fetchall()
        for item in origin:
            result.append(item[0])
        print(result)
    except:
        traceback.print_exc()
        conn.rollback()
    return result


async def clean_group_info():
    sql = "SELECT l.group_id id,group_name,group_url,i.group_id,i.group_comment FROM group_list l RIGHT JOIN group_info i on l.group_id=i.group_id WHERE l.group_id is null"
    result = []
    try:
        cursor.execute(sql)
        origin = cursor.fetchall()
        for item in origin:
            result.append(item[3])
        print("unavailable list :")
        print(result)
    except:
        traceback.print_exc()
        conn.rollback()
    for group_id in result:
        await delete_group_info_by_group_id(group_id)


async def get_group_comment(session, group_id):
    success = None
    is_denied = None
    next_start = 0
    comment_sum = 0
    topic_count = 0
    topic_index = 0
    while not success or is_denied:  # 循环结构
        url = "https://www.douban.com/group/{0}/discussion?start={1}".format(group_id, next_start)
        await sleep(0.5)
        try:
            async with session.get(url, headers=headers) as response:  # 获取网页HTML
                is_denied = response.url.__str__()[:33] == "https://www.douban.com/misc/sorry"  # 判断是否被拒绝
                if not is_denied:  # 如果没有被拒绝，则获取网页元素
                    text = await response.text()
                    soup = BeautifulSoup(text, "lxml")
                    comment_elements = soup.find_all("td", attrs={"class": "r-count"})[1:]
                    time_elements = soup.find_all("td", attrs={"class": "time"})
                    comment_flag = soup.select("#content > h1")
                    is_empty = len(comment_elements) <= 0 or len(time_elements) <= 0
                    is_denied = len(comment_flag) <= 0
                if is_denied:  # 如果被拒绝，则提交验证码，再次循环
                    await PostSolution.main()
                    await sleep(2)
                    continue

                if not is_empty:
                    over_time = False
                    for index, comment in enumerate(comment_elements):  # 判断评论是否过期并计数
                        time = parse(time_elements[index].get_text())
                        NOW = datetime.datetime.now()
                        text = comment.get_text()
                        if time > NOW + relativedelta(weeks=-1):
                            if text.isdigit() or text == "":
                                num = eval(text if text != "" else "1")
                                comment_sum += num if num <= 1000 else 0
                        else:
                            over_time = True
                            topic_index = index
                            break
                    print(next_start)

                    # 获取下一页地址
                    next_start_element = soup.select("#content > div > div.article > div.paginator > span.next > a")
                    is_end = len(next_start_element) <= 0 or over_time
                else:
                    is_end = True

            if not is_end:  # 判断是否还有下一页，如果没有，结束循环
                next_start = next_start_element[0].get('href')
                next_start = next_start[next_start.index("=") + 1:]
                topic_count = eval(next_start)
            else:
                topic_count += topic_index
                success = True
        except:
            traceback.print_exc()
            success = None

    return comment_sum, topic_count


async def get_group_member(session, group_id):
    url = "https://www.douban.com/group/{0}/".format(group_id)
    success = None
    is_denied = None
    cnt = 0
    while not success or is_denied:  # 循环结构
        await sleep(0.5)
        try:
            async with session.get(url, headers=headers) as response:  # 获取网页HTML
                is_denied = response.url.__str__()[:33] == "https://www.douban.com/misc/sorry"  # 判断是否被拒绝
                if not is_denied:  # 如果没有被拒绝，则获取网页元素
                    text = await response.text()
                    soup = BeautifulSoup(text, "lxml")
                    member_element = soup.select(
                        "#content > div.grid-16-8.clearfix > div.aside > div.mod.side-nav > p:nth-child(1) > a")
                    is_denied = len(member_element) <= 0
                if is_denied:  # 如果被拒绝，则提交验证码，再次循环
                    if len(soup.select("#wrapper > div:nth-child(1) > ul > li:nth-child(1)")) > 0:  # 判断小组是否寄了
                        return -1
                    else:
                        await PostSolution.main()
                        continue

                if len(member_element) <= 0:  # 如果没获取到人数，计数重复3次，如果失败则返回-1
                    cnt += 1
                    if cnt <= 3:
                        await sleep(0.5)
                    else:
                        return -1
                else:
                    member_text = member_element[0].get_text()
                    return eval(member_text[member_text.rfind("(") + 1:-1])  # 返回小组人数
        except:
            print("failed once")


async def main():
    async with aiohttp.ClientSession() as session:
        group_list = await get_group_id_list()  # 从数据库读取小组列表
        await PostSolution.main()
        start = True
        for index, group_id in enumerate(group_list):
            if start or group_id == "700332":
                start = True
            else:
                continue
            member = await get_group_member(session, group_id)  # 获取小组人数
            if member == -1:  # 如果获取失败，一般是这个小组寄了，所以要删除
                await delete_group_list_by_group_id(group_id)
                continue
            print(group_id, member)
            sum, page_cnt = await get_group_comment(session, group_id)  # 获取该小组讨论数
            print(sum, page_cnt)
            if sum <= 100:
                await delete_group_list_by_group_id(group_id)  # 如果讨论数小于100，则从数据库中删除
            else:
                await insert_group_info(group_id, member, sum, page_cnt)  # 将信息插入数据库
        print("fetch over")
        await clean_group_info()  # 清理无效小组信息
        # await replace_group_ranked()  # 对小组排名并存进数据库，减少服务器压力


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
