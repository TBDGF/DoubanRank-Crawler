import asyncio
import traceback
from asyncio import sleep

import aiohttp
import pymysql
from bs4 import BeautifulSoup

import PostSolution
import Secret

headers = {
    "User-Agent": Secret.USER_AGENT,
    "Cookie": Secret.COOKIE
}

conn = pymysql.connect(host="localhost", user=Secret.DB_USER, password=Secret.DB_PASSWORD, database="douban", port=3306,
                       charset="utf8mb4")
cursor = conn.cursor()


async def insert_group(group_id,grou_name,group_url):
    sql = "INSERT ignore INTO  `group_list` VALUES (%s, %s, %s)"
    data=(group_id,grou_name,group_url)
    try:
        cursor.execute(sql, data)
        conn.commit()
        print("insert finished")
    except:
        traceback.print_exc()
        conn.rollback()

async def delete_waited(group_id):
    sql="delete  from group_waited where group_id=%s"
    try:
        cursor.execute(sql, group_id)
        conn.commit()
        print("delete finished")
    except:
        traceback.print_exc()
        conn.rollback()


async def get_group_name(session, group_id):
    url = "https://www.douban.com/group/{0}/".format(group_id)
    success = None
    is_denied = None
    cnt = 0
    while not success or is_denied:  # 循环结构
        try:
            async with session.get(url, headers=headers) as response:  # 获取网页HTML
                is_denied = response.url.__str__()[:33] == "https://www.douban.com/misc/sorry"  # 判断是否被拒绝
                if not is_denied:  # 如果没有被拒绝，则获取网页元素
                    text = await response.text()
                    soup = BeautifulSoup(text, "lxml")
                    name_element = soup.select(
                        "#group-info > div > h1")
                    is_denied = len(name_element) <= 0
                if is_denied:  # 如果被拒绝，则提交验证码，再次循环
                    if len(soup.select("#wrapper > div:nth-child(1) > ul > li:nth-child(1)")) >0:
                        return -1
                    else:
                        await PostSolution.main()
                        continue

                if len(name_element) <= 0:  # 如果没获取到组名，计数重复3次，如果失败则返回-1
                    cnt += 1
                    if cnt <= 3:
                        await sleep(0.5)
                    else:
                        return -1
                else:
                    return name_element[0].get_text().strip()
        except:
            print("failed once")

async def get_waited_list():
    sql="select * from group_waited"
    result = {}
    try:
        cursor.execute(sql)
        origin = cursor.fetchall()
        for item in origin:
            result.update({item[0]: item[1]})
        print(result)
    except:
        traceback.print_exc()
        conn.rollback()
    return result

async def main():
    async with aiohttp.ClientSession() as session:
        waited_list = await get_waited_list()
        for index,waited_item in enumerate(waited_list.items()):
            group_name = await get_group_name(session, waited_item[0])
            print((waited_item[0], group_name))
            if (group_name == -1):
                print("this group was gg")
                await delete_waited(waited_item[0])
                continue
            await insert_group(waited_item[0], group_name, waited_item[1])
            await delete_waited(waited_item[0])
        print("fetch over")



loop = asyncio.get_event_loop()
loop.run_until_complete(main())