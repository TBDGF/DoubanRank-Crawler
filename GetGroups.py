import traceback
from bs4 import BeautifulSoup
import pymysql
import requests
import aiohttp
import asyncio

import Secret

url = 'https://www.douban.com/group/explore/ent'
headers = {
    'User-Agent': """Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36 Edg/92.0.902.62""",
    "Cookie": """bid=_ZEDpdyQTEI; douban-fav-remind=1; __yadk_uid=BF7ojLCw5Rs3x7Wz6ivgbZDhIFCnnI8p; __gads=ID=e1bfeef345ac1326-221c65e555ca00e6:T=1626315568:RT=1626315568:S=ALNI_MabL_79dfCpSI3s3h3A_tpxwa3XQA; __utmz=30149280.1627486824.2.2.utmcsr=baidu|utmccn=(organic)|utmcmd=organic; __utmc=30149280; ll="118097"; ct=y; dbcl2="238444551:/SE+i3D1Yy0"; ck=Un0d; push_noty_num=0; push_doumail_num=0; __utmv=30149280.23844; _pk_ref.100001.8cb4=["","",1628087786,"https://www.baidu.com/link?url=s26VBmJ5bN0FrsAgi-UivAkcKb3H90_VoUc9U-47IwYpjPxbe6qqahpn1ptEpIJk1GbUj5nn3-TZCJVNbfQc9K&wd=&eqid=923bea960001fcd10000000361017a5d"]; __utma=30149280.985138014.1626315565.1628062921.1628087801.5; _pk_id.100001.8cb4=88b0785cdb983b34.1626315562.5.1628087825.1628064427."""
}

conn = pymysql.connect(host="localhost", user=Secret.DB_USER, password=Secret.DB_PASSWORD, database="douban", port=3306,
                       charset="utf8mb4")
cursor = conn.cursor()


def groups_insert(page_data):
    sql = "INSERT ignore INTO  `group-list` VALUES (%s, %s, %s)"
    try:
        cursor.executemany(sql, page_data)
        conn.commit()
        print("insert finished")
    except:
        traceback.print_exc()
        conn.rollback()


def insert_group_by_url(group_url):
    async def main():
        async with aiohttp.ClientSession() as session:
            async with session.get(group_url, headers=headers) as response:
                html = await response.text()
                soup = BeautifulSoup(html, "lxml")
                data = soup.select("#group-info > div > h1")
                group_name = data[0].get_text().strip()
                group_id = group_url[group_url.index("group/") + 6:-1]
                sql = "INSERT ignore INTO  `group-list` VALUES (%s, %s, %s)"
                try:
                    cursor.execute(sql, (group_id, group_name, group_url))
                    conn.commit()
                    print("insert finished")
                except:
                    traceback.print_exc()
                    conn.rollback()

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())


async def read_page(html_text):
    soup = BeautifulSoup(html_text, "lxml")
    data = soup.select(
        "#content > div > div.article > div:nth-child(1) > div > div.bd > div.source > span.from > a")
    result = []
    for item in data:
        result.append(
            (item.get('href')[29:-1],  # group-id
             item.get_text()[:-2],  # group-name
             item.get('href'))  # group-url
        )
    next = soup.select("#content > div > div.article > div.paginator > span.next > a")
    is_end = len(next) <= 0
    groups_insert(result)
    return {
        "data": result,
        "is_end": is_end,
        "next_start": next[0].get('href') if not is_end else ""
    }


def read_to_end():
    async def fetch(session, url):
        async with session.get(url, headers=headers) as response:
            text = await response.text()
            result = await read_page(text)
            return result

    async def main(origin_url):
        async with aiohttp.ClientSession() as session:
            result = await fetch(session, origin_url + "?start=6060")
            print(result.get("next_start")[7:])
            while not result.get('is_end'):
                result = await fetch(session, origin_url + result.get("next_start"))
                print(result.get("next_start")[7:])

    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(url))


if __name__ == '__main__':
    # read_to_end()

    insert_group_by_url("https://www.douban.com/group/726142/")
    conn.close()
