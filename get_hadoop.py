#-*- coding: utf-8 -*-

import requests
import sys  
reload(sys)  
sys.setdefaultencoding("utf-8" )  
import MySQLdb
import datetime


def get_heml(url):
    response = requests.get(url)
    response.encoding = "urf-8"
    return response.text

'''
def get_content(html):
    result = []
    soup = BeautifulSoup(html, 'lxml')
    tag = soup.find_all("tbody", attrs={'class', 'ui-widget-content'})
    #print tag
    for d in tag:
        content = str(d.get_text())
        for d in content.strip().split("\n"):
            temp = d.strip()
            if temp != "":
                result.append(temp)
    return result
'''

def get_content_new(html):
    import re
    result = []
    p = re.compile(r'<td>(.*?)</td>', re.S)
    td_content = re.findall(p, html)
    td_content = td_content[:16]
    for d in td_content:
        temp = d.strip()
        if temp != "":
            result.append(temp)

    return result

def insert_db(content):
    conn = MySQLdb.connect("localhost", "root", "123456", "hadoop", charset='utf8' )
    cursor = conn.cursor()
    nowTime=datetime.datetime.now().strftime('%Y%m%d%H%M')
    time_id=nowTime
    apps_submitted = content[0]
    apps_peding = content[1]
    apps_running = content[2]
    apps_completed = content[3]
    containers_running = content[4]
    memory_used = content[5]
    memory_total = content[6]
    memory_reserved = content[7]
    sql = "insert into hd_info(time_id, apps_submitted, apps_peding, apps_running, apps_completed, containers_running, memory_used, memory_total, memory_reserved) \
        values('%s', %s, %s, %s, %s, %s, '%s', '%s', '%s')" % (time_id, apps_submitted, apps_peding, apps_running, apps_completed, containers_running, memory_used, memory_total, memory_reserved)

    cursor.execute(sql)
    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    url = "http://10.142.97.3:8088/cluster/scheduler"
    html = get_heml(url)
    result = get_content_new(html)
    insert_db(result)
    #
