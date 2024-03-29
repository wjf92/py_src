import json
import logging
import datetime
import time
import re
import threading
import simplejson
import ast
from django.http import HttpResponse
from rest_framework.response import Response
from libs.serializers import Query_review, Query_list
from libs import baseview, send_email, util
from libs import con_database
from core.models import DatabaseList, Account, querypermissions, query_order, globalpermissions, user_collect
from rest_framework.views import APIView
# wjf
from django.db.models import Count

CUSTOM_ERROR = logging.getLogger('Yearning.core.views')


class DateEncoder(simplejson.JSONEncoder):  # 感谢的凉夜贡献

    def default(self, o):
        if isinstance(o, datetime.datetime) or isinstance(o, datetime.date) or isinstance(o, datetime.time):
            return o.__str__()
        return simplejson.JSONEncoder.default(self, o)


class search(baseview.BaseView):
    '''
    :argument   sql查询接口, 过滤非查询语句并返回查询结果。
                可以自由limit数目 当limit数目超过配置文件规定的最大数目时将会采用配置文件的最大数目

    '''

    def post(self, request, args=None):
        sql_offset = request.data["sql_offset"]
        limit = util.get_conf_limit()
        sql = request.data['sql']
        check = str(sql).strip().split(';\n')
        # user = query_order.objects.filter(username=request.user).order_by('-id').first()
        un_init = util.init_conf()
        custom_com = ast.literal_eval(un_init['other'])
        dbname_list = get_dblist_foruser(request.user)
        if request.data['address'] not in dbname_list:
            return Response({'error': '无权限，请先申请权限！'})
        if check[-1].strip().lower().startswith('s') != 1:
            return Response({'error': '只支持查询功能或删除不必要的空白行！'})
        else:
            _c = DatabaseList.objects.filter(
                connection_name=request.data['address']
            ).first()
            try:
                with con_database.SQLgo(
                        ip=_c.ip,
                        password=_c.password,
                        user=_c.username,
                        port=_c.port,
                        db=_c.dbname
                ) as f:
                    query_sql = check[-1].strip().strip(";") + ' limit ' + str(limit) + ' offset ' + str(sql_offset)
                    data_set = f.search(sql=query_sql)
                    for l in data_set['data']:
                        for k, v in l.items():
                            if isinstance(v, bytes):
                                for n in range(data_set['len']):
                                    data_set['data'][n].update({k: 'blob字段为不可呈现类型'})
                            for i in custom_com['sensitive_list']:
                                if k == i:
                                    for n in range(data_set['len']):
                                        data_set['data'][n].update({k: '********'})
                    querypermissions.objects.create(
                       # work_id=user.work_id,
                        username=request.user,
                        statements=query_sql,
                        create_time=util.datetime(),
                        connect_name=request.data['address']
                    )
                    return HttpResponse(simplejson.dumps(data_set, cls=DateEncoder, bigint_as_string=True))
            except Exception as e:
                CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
                return Response({'error': str(e)})



    def put(self, request, args: str = None):
        base = request.data['base']
        table = request.data['table']
        query_per = query_order.objects.filter(username=request.user).order_by('-id').first()
        if query_per.query_per == 1:
            _c = DatabaseList.objects.filter(
                connection_name=query_per.connection_name,
                computer_room=query_per.computer_room
            ).first()
            try:
                with con_database.SQLgo(
                        ip=_c.ip,
                        password=_c.password,
                        user=_c.username,
                        port=_c.port,
                        db=base
                ) as f:
                    data_set = f.search(sql='desc %s' % table)
                return Response(data_set)
            except:
                return Response('')
        else:
            return Response({'error': '已超过申请时限请刷新页面后重新提交申请'})


def replace_limit(sql, limit):
    '''

    :argument 根据正则匹配分析输入信息 当limit数目超过配置文件规定的最大数目时将会采用配置文件的最大数目

    '''

    if sql[-1] != ';':
        sql += ';'
    if sql.startswith('show') == -1:
        return sql
    sql_re = re.search(r'limit\s.*\d.*;', sql.lower())
    length = ''
    if sql_re is not None:
        c = re.search(r'\d.*', sql_re.group())
        if c is not None:
            if c.group().find(',') != -1:
                length = c.group()[-2]
            else:
                length = c.group().rstrip(';')
        if int(length) <= int(limit):
            return sql
        else:
            sql = re.sub(r'limit\s.*\d.*;', 'limit %s;' % limit, sql)
            return sql
    else:
        sql = sql.rstrip(';') + ' limit %s;' % limit
        return sql


class query_worklf(baseview.BaseView):

    @staticmethod
    def query_callback(timer, work_id):
        query_order.objects.filter(work_id=work_id).update(query_per=1)
        # wjf del

        # try:
        #     time.sleep(int(timer) * 60)
        # except:
        #     time.sleep(60)
        # finally:
        #     t_close = threading.Thread(target=query_worklf.conn_close, args=(work_id,))
        #     t_close.start()
        #     t_close.join()

    @staticmethod
    def conn_close(work_id=None):
        query_order.objects.filter(work_id=work_id).update(query_per=3)

    def get(self, request, args: str = None):
        page = request.GET.get('page')
        page_number = query_order.objects.count()
        start = int(page) * 20 - 20
        end = int(page) * 20
        info = query_order.objects.all().order_by('-id')[start:end]
        serializers = Query_review(info, many=True)
        return Response({'page': page_number, 'data': serializers.data})

    def post(self, request, args: str = None):

        work_id = request.data['workid']
        user = request.data['user']
        data = querypermissions.objects.filter(work_id=work_id, username=user).all().order_by('-id')
        serializers = Query_list(data, many=True)
        return Response(serializers.data)

    def put(self, request, args: str = None):

        if request.data['mode'] == 'put':
            instructions = request.data['instructions']
            connection_name = request.data['connection_name']
            computer_room = request.data['computer_room']
            timer = int(request.data['timer'])
            export = request.data['export']
            audit = request.data['audit']
            un_init = util.init_conf()
            query_switch = ast.literal_eval(un_init['other'])
            query_per = 2
            work_id = util.workId()
            if not query_switch['query']:
                query_per = 1
            else:
                userinfo = Account.objects.filter(username=audit, group='admin').first()
                try:
                    thread = threading.Thread(
                        target=push_message,
                        args=(
                        {'to_user': request.user, 'workid': work_id}, 5, request.user, userinfo.email, work_id, '提交')
                    )
                    thread.start()
                except Exception as e:
                    CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
            # 最大可申请7天权限
            timer = 168 if timer > 168 else timer
            query_order.objects.create(
                work_id=work_id,
                instructions=instructions,
                username=request.user,
                timer=timer,
                date=datetime.datetime.now() + datetime.timedelta(hours=timer),
                query_per=query_per,
                connection_name=','.join(connection_name),
                computer_room=computer_room,
                export=export,
                audit=audit,
                time=util.datetime()
            )
            if not query_switch['query']:
                t = threading.Thread(target=query_worklf.query_callback, args=(timer, work_id))
                t.start()
            ## 钉钉及email站内信推送
            return Response('查询工单已提交，等待管理员审核！')

        elif request.data['mode'] == 'agree':
            work_id = request.data['work_id']
            query_info = query_order.objects.filter(work_id=work_id).order_by('-id').first()
            t = threading.Thread(target=query_worklf.query_callback, args=(query_info.timer, work_id))
            t.start()
            userinfo = Account.objects.filter(username=query_info.username).first()
            thread = threading.Thread(target=push_message, args=(
                {'to_user': query_info.username, 'workid': query_info.work_id}, 6, query_info.username, userinfo.email,
                work_id, '同意'))
            thread.start()
            return Response('查询工单状态已更新！')

        elif request.data['mode'] == 'disagree':
            work_id = request.data['work_id']
            query_order.objects.filter(work_id=work_id).update(query_per=0)
            query_info = query_order.objects.filter(work_id=work_id).order_by('-id').first()
            userinfo = Account.objects.filter(username=query_info.username).first()
            thread = threading.Thread(target=push_message, args=(
                {'to_user': query_info.username, 'workid': query_info.work_id}, 7, query_info.username, userinfo.email,
                work_id, '驳回'))
            thread.start()
            return Response('查询工单状态已更新！')

        elif request.data['mode'] == 'status':
            try:
                limit = util.get_conf_limit()
                curr_time = util.datetime()
                status = query_order.objects.filter(username=request.user).filter(query_per=1).filter(date__gt=curr_time).aggregate(count=Count('id'))
                if status['count'] > 0:
                    return Response({'limit': limit, 'status': 1})
                else:
                    return Response({'limit': limit, 'status': 0})
            except:
                return Response(0)

        elif request.data['mode'] == 'end':
            try:
                query_order.objects.filter(username=request.user).order_by('-id').update(query_per=3)
                return Response('结束查询工单成功！')
            except Exception as e:
                return Response(e)

        elif request.data['mode'] == 'dblist':
            dbname_list = get_dblist_foruser(request.user)
            return Response(dbname_list)

        elif request.data['mode'] == 'tablist':
            dbname = request.data['dbname']
            _connection = DatabaseList.objects.filter(connection_name=dbname).first()
            with con_database.SQLgo(ip=_connection.ip,
                                    user=_connection.username,
                                    password=_connection.password,
                                    port=_connection.port,
                                    db=_connection.dbname) as f:
                tabname = f.query_info(sql='show tables')
            return Response([list(i.values())[0] for i in tabname])

        elif request.data['mode'] == 'tabmeta':
            dbname = request.data['dbname']
            tabname = request.data['tabname']
            get_tabmeta_sql = ("show create table {0}".format(tabname))
            _connection = DatabaseList.objects.filter(connection_name=dbname).first()
            with con_database.SQLgo(ip=_connection.ip,
                                    user=_connection.username,
                                    password=_connection.password,
                                    port=_connection.port,
                                    db=_connection.dbname) as f:
                tabmeta = f.query_info(sql=get_tabmeta_sql)
            try:
                tabmeta_deal = tabmeta[0]["Create Table"]
                return Response(tabmeta_deal)
            except Exception:
                return Response('')

        elif request.data['mode'] == 'addcollect':
            try:
                user_collect.objects.create(
                    username=request.user,
                    connect_name=request.data["conn_name"],
                    sql_text=request.data["sql_text"],
                    collect_alias=request.data["alias_name"],
                    create_time=util.datetime(),
                    update_time=util.datetime()
                )
                return Response('ok')
            except Exception as e:
                CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
                return HttpResponse(status=500)

        elif request.data['mode'] == 'getcollect':
            try:
                my_collect = user_collect.objects.filter(username=request.user).order_by("-sort_flag").order_by("-update_time")
                data_dict = [
                    {'title': "别名", "key": "collect_alias", "fixed": "left", "sortable": "true", "width": 150},
                    {'title': "连接名", "key": "connect_name", "fixed": "left", "sortable": "true", "width": 150},
                    {'title': "sql", "key": "sql_text", "fixed": "left", "sortable": "true", "width": 265},
                ]
                data_result = []
                len = 0
                for i in my_collect:
                    len = len + 1
                    data_result.append({
                        "id": i.id,
                        "collect_alias": i.collect_alias,
                        "connect_name": i.connect_name,
                        "sql_text": i.sql_text
                    })
                collect_fin = {'data': data_result, 'title': data_dict, 'len': len}
                return HttpResponse(simplejson.dumps(collect_fin, cls=DateEncoder, bigint_as_string=True))
            except Exception as e:
                CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
                return HttpResponse(status=500)

        elif request.data['mode'] == 'delcollect':
            try:
                user_collect.objects.filter(id=request.data["id"]).delete()
                return Response('ok')
            except Exception as e:
                CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
                return HttpResponse(status=500)

        elif request.data['mode'] == 'setcollecttop':
            try:
                user_collect.objects.filter(id=request.data["id"]).update(sort_flag=999, update_time=util.datetime())
                return Response('ok')
            except Exception as e:
                CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
                return HttpResponse(status=500)
        elif request.data['mode'] == 'info':
            tablelist = []
            database_list_from_db = query_order.objects.filter(username=request.user).filter(query_per=1)
            database_list = []
            dbname_list = []
            for row in database_list_from_db:
                for i in row.connection_name.split(","):
                    if i not in dbname_list:
                        dbname_list.append(i)
                        database_list.append({"connection_name": i,
                                              "export": row.export})
            data = []
            for database in database_list:
                _connection = DatabaseList.objects.filter(connection_name=database["connection_name"]).first()
                with con_database.SQLgo(ip=_connection.ip,
                                        user=_connection.username,
                                        password=_connection.password,
                                        port=_connection.port) as f:
                    dataname = f.query_info(sql='show databases')
                children = []
                ignore = ['information_schema', 'sys', 'performance_schema', 'mysql']
                for index, uc in enumerate(dataname):
                    for cc in ignore:
                        if uc['Database'] == cc:
                            del dataname[index]
                            index = index - 1
                for i in dataname:
                    with con_database.SQLgo(ip=_connection.ip,
                                            user=_connection.username,
                                            password=_connection.password,
                                            port=_connection.port,
                                            db=i['Database']) as f:
                        tablename = f.query_info(sql='show tables')
                    for c in tablename:
                        key = 'Tables_in_%s' % i['Database']
                        children.append({
                            'title': c[key]
                        })
                    tablelist.append({
                        'title': i['Database'],
                        'children': children
                    })
                    children = []
                data.append({
                    'title': database,
                    'expand': 'true',
                    'children': tablelist
                })
            return Response({'info': json.dumps(data), 'status': database["export"]})

    def delete(self, request, args: str = None):

        data = query_order.objects.filter(username=request.user).order_by('-id').first()
        query_order.objects.filter(work_id=data.work_id).delete()
        return Response('')


def push_message(message=None, type=None, user=None, to_addr=None, work_id=None, status=None):
    try:
        tag = globalpermissions.objects.filter(authorization='global').first()
        if tag.message['mail']:
            put_mess = send_email.send_email(to_addr=to_addr)
            put_mess.send_mail(mail_data=message, type=type)
    except Exception as e:
        CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
    else:
        try:
            if tag.message['ding']:
                un_init = util.init_conf()
                webhook = ast.literal_eval(un_init['message'])
                util.dingding(content='查询申请通知\n工单编号:%s\n发起人:%s\n状态:%s' % (work_id, user, status),
                              url=webhook['webhook'])
        except ValueError as e:
            CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')


class Query_order(APIView):

    def get(self, request, args: str = None):
        if request.GET.get('type') and request.GET.get('type') == "myorder":
            page_size = 5
            page = request.GET.get('page')
            pn = query_order.objects.filter(username=request.user).count()
            start = int(page) * page_size - page_size
            end = int(page) * page_size
            user_list = query_order.objects.filter(username=request.user).order_by('-id')[start:end]
            serializers = Query_review(user_list, many=True)
            return Response({'data': serializers.data, 'pn': pn})
        else:
            page = request.GET.get('page')
            pn = query_order.objects.filter(audit=request.user).count()
            start = int(page) * 10 - 10
            end = int(page) * 10
            user_list = query_order.objects.filter(audit=request.user).order_by('-id')[start:end]
            serializers = Query_review(user_list, many=True)
            return Response({'data': serializers.data, 'pn': pn})

    def post(self, request, args: str = None):
        work_id_list = json.loads(request.data['work_id'])
        for i in work_id_list:
            query_order.objects.filter(work_id=i).delete()
        return Response('申请记录已删除!')


# 新增函数，获取用户有权限的连接名list
def get_dblist_foruser(v_username):
    curr_time = util.datetime()
    database_list_from_db = query_order.objects.filter(username=v_username).filter(query_per=1).filter(date__gt=curr_time)
    database_alive = DatabaseList.objects.all()
    dbname_list_alive = [j.connection_name for j in database_alive]
    dbname_list = []
    for row in database_list_from_db:
        for i in row.connection_name.split(","):
            if i not in dbname_list and i in dbname_list_alive:
                dbname_list.append(i)
    return dbname_list