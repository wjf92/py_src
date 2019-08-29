import logging
import json
from libs import baseview, util
from libs import call_inception
from core.task import submit_push_messages
from rest_framework.response import Response
from django.http import HttpResponse
from core.models import (
    DatabaseList,
    SqlOrder,
    workflow_record,
    workflow_config
)

CUSTOM_ERROR = logging.getLogger('Yearning.core.views')

conf = util.conf_path()
addr_ip = conf.ipaddress


class sqlorder(baseview.BaseView):
    '''

    :argument 手动模式工单提交相关接口api

    put   美化sql  测试sql

    post 提交工单

    '''

    def put(self, request, args=None):
        if args == 'beautify':
            try:
                data = request.data['data']
            except KeyError as e:
                CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
            else:
                try:
                    res = call_inception.Inception.BeautifySQL(sql=data)
                    return HttpResponse(res)
                except Exception as e:
                    CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
                    return HttpResponse(status=500)

        elif args == 'test':
            try:
                connect_name = request.data['connect_name']
                sql = request.data['sql']
                sql = str(sql).strip('\n').strip().rstrip(';')
                data = DatabaseList.objects.filter(connection_name=connect_name).first()
                info = {
                    'host': data.ip,
                    'user': data.username,
                    'password': data.password,
                    'db': data.dbname,
                    'port': data.port
                }
            except KeyError as e:
                CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
            else:
                try:
                    with call_inception.Inception(LoginDic=info) as test:
                        res = test.Check(sql=sql)
                        return Response({'result': res, 'status': 200})
                except Exception as e:
                    CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
                    return Response(e)

    def post(self, request, args=None):
        try:
            data = json.loads(request.data['data'])
            tmp = json.loads(request.data['sql'])
            user = request.data['user']
            type = request.data['type']
            id = request.data['id']
        except KeyError as e:
            CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
            return HttpResponse(status=500)
        else:
            try:
                x = [x.rstrip(';') for x in tmp]
                sql = ';'.join(x)
                sql = sql.strip(' ').rstrip(';')
                workId = util.workId()
                next_handle_user = workflow_config.objects.filter(name=data['assigned']).filter(step_num=2).values("handler_user").first()
                # print(next_handle_user["handler_user"])
                SqlOrder.objects.get_or_create(
                    username=user,
                    date=util.date(),
                    work_id=workId,
                    status=2,
                    basename=data['basename'],
                    sql=sql,
                    type=type,
                    text=data['text'],
                    backup=data['backup'],
                    bundle_id=data['connection_name'],
                    assigned=data['assigned'],
                    delay=data['delay'],
                    next_deal_user=next_handle_user["handler_user"]
                )
                result_workflow_info = workflow_config.objects.filter(name=data['assigned']).filter(step_num=1).values("step_name").first()
                workflow_record.objects.get_or_create(
                    create_time = util.datetime(),
                    work_id = workId,
                    workflow_name= data['assigned'],
                    step_num=1,
                    step_name=result_workflow_info["step_name"],
                    handler_user=user,
                    handler_result="发起dml申请",
                    opinion=data['text']
                )
                submit_push_messages(
                    workId=workId,
                    user=user,
                    addr_ip=addr_ip,
                    text=data['text'],
                    assigned=data['assigned'],
                    id=id
                ).start()
                return Response('已提交，请等待管理员审核!')
            except Exception as e:
                CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
                return HttpResponse(status=500)

def order_next_handler_user(workid, curr_step_num):
    get_order_info = SqlOrder.objects.filter(work_id=workid).first()
    order_workflow_name = get_order_info.assigned
    order_owner = get_order_info.username
    max_step_num = workflow_config.objects.filter(name=order_workflow_name).aggregate(max(step_num))
    if int(curr_step_num) == int(max_step_num):
        return false
    else:
        next_step = int(curr_step_num) + 1
        next_handler_user = workflow_config.objects.filter(name=order_workflow_name).filter(step_num=next_step).first().values(handler_user)
        if next_handler_user == "owner":
            return order_owner
        else:
            return next_handler_user
