import pika,os,json,re
from bilibiliupload import *
import MySQLdb

class MIGO(object):

    # 数据库连接模块
    # def __init__(self):
    #     self.migo_db = MySQLdb.connect("localhost","root","（此处填写密码）","miguo_matrix")
    #     self.cursor = self.migo_db.cursor()
    
    def run(self):

        credentials = pika.PlainCredentials('Hocassian', '')
        connection = pika.BlockingConnection(pika.ConnectionParameters(host = 'localhost',port = 5672,virtual_host = '/upload',credentials = credentials))
        channel = connection.channel()
        
        # 创建临时队列，队列名传空字符，consumer关闭后，队列自动删除
        channel.queue_declare(queue='videoSendingQueue',durable = True)

        channel.exchange_declare(exchange = 'videoSendingExchange',durable = True, exchange_type='direct')
        # 绑定exchange和队列  exchange 使我们能够确切地指定消息应该到哪个队列去

        channel.queue_bind(exchange = 'videoSendingExchange', queue='videoSendingQueue', routing_key='videoSendingRouting')

        #channel.basic_qos(prefetch_count=1)
        # 告诉rabbitmq，用callback来接受消息

        # 设置成 False，在调用callback函数时，未收到确认标识，消息会重回队列。True，无论调用callback成功与否，消息都被消费掉
        channel.basic_consume('videoSendingQueue',self.callback, auto_ack = False)
        channel.start_consuming()


    # 定义一个回调函数来处理消息队列中的消息，这里是打印出来
    def callback(self, ch, method, properties, body):
        ch.basic_ack(delivery_tag = method.delivery_tag)
        print(body.decode())
        # video_info_json = body.decode()
        video_info_dict = json.loads(body.decode())
        _video_callback = self.video_upload(video_info_dict)

        video_callback = str(_video_callback)

        # 测试值 _video_callback = "{'code': 0, 'message': '0', 'ttl': 1, 'data': {'aid': 980000000, 'bvid': 'BV1LE411P7vz'}}"

        video_callback_dict = json.loads(video_callback.replace('\'','\"'))

        print(_video_callback)

        self.sql_callback(video_callback_dict, video_info_dict)

        

    def video_upload(self, video_info):
        
        b = Bilibili()

        login_status = b.login('','')

        print("登陆情况", login_status)

        # tags = list(video_info['videoTag'])

        print(video_info['messageData']['videoTag'])

        tags = json.loads(video_info['messageData']['videoTag'])
        
        
        # tags = "{u\"测试\",u\"ceshi\",u\"中文字符\",u\"多人\",u\"234324\"}"
        
        print(tags)

        abs_path = './file/'

        video_upload_path = os.path.join(abs_path, video_info['messageData']['videoPath'])

        pic_upload_path = os.path.join(abs_path, video_info['messageData']['videoPic'])

        videoPart = VideoPart(video_upload_path, video_info['messageData']['videoTitle'])

        print(video_upload_path)

        video_callback = b.upload(parts=videoPart, title=video_info['messageData']['videoTitle'], tid= int(video_info['messageData']['videoClass']), tag = tags, desc = video_info['messageData']['videoProfile'], cover=b.cover_up(pic_upload_path))

        os.remove(video_upload_path)

        return video_callback


    # 数据库更改模块
    def sql_callback(self, video_callback, video_info):
        
        a_sql = "UPDATE media_video SET video_url = '%s', video_status = '%s', video_cid = '%s' WHERE id = '%s'" % (str(video_callback['data']['aid']), 'complete', str(video_callback['data']['bvid']), video_info['messageData']['id'])

        b_sql = "INSERT INTO client_video(id, video_title, video_profile, video_url, video_cid, video_date, video_cover, video_status, video_author, create_at, create_by, update_at, update_by, is_del) \
                VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %s)" % \
                (video_info['messageId'], video_info['messageData']['videoTitle'], video_info['messageData']['videoProfile'], str(video_callback['data']['aid']), str(video_callback['data']['bvid']),
                video_info['messageData']['updateAt'], video_info['messageData']['videoPic'], 'draft', video_info['messageData']['videoAuthor'], video_info['messageData']['createAt'],
                video_info['messageData']['createBy'], video_info['messageData']['updateAt'], video_info['messageData']['updateBy'], False)

        migo_db = MySQLdb.connect("localhost","root","","miguo_matrix")

        # 这个是拿来本地测试的
        # migo_db = MySQLdb.connect("localhost","root","123456","miguo_matrix")

        cursor = migo_db.cursor()

        try:
            cursor.execute(a_sql)
            cursor.execute(b_sql)
            migo_db.commit()
        except:
            migo_db.rollback()

        cursor.close()

        migo_db.close()

if __name__ == '__main__':
        migo = MIGO()
        migo.run()
