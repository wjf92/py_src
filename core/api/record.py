import logging
import json
from libs import baseview, rollback, util
from rest_framework.response import Response
from django.http import HttpResponse
from core.models import SqlOrder, SqlRecord, workflow_config, workflow_record
from libs.serializers import Record
from django.core import serializers
from django.forms.models import model_to_dict

CUSTOM_ERROR = logging.getLogger('Yearning.core.views')


class record_order(baseview.SuperUserpermissions):
    '''

    :argument 记录展示请求接口api

    :return 记录及记录总数

    '''

    def get(self, request, args=None):
        try:
            page = request.GET.get('page')
            username = request.GET.get('username')
        except KeyError as e:
            CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
            return HttpResponse(status=500)
        else:
            try:
                pagenumber = SqlOrder.objects.filter(status=1, assigned=username).count()
                start = int(page) * 10 - 10
                end = int(page) * 10
                sql = SqlOrder.objects.raw(
                    '''
                    select core_sqlorder.*,core_databaselist.connection_name, \
                    core_databaselist.computer_room from core_sqlorder \
                    INNER JOIN core_databaselist on \
                    core_sqlorder.bundle_id = core_databaselist.id where core_sqlorder.status = 1 and core_sqlorder.assigned = '%s'\
                    ORDER BY core_sqlorder.id desc
                    ''' % username
                )[start:end]
                data = util.ser(sql)
                return Response({'data': data, 'page': pagenumber})
            except Exception as e:
                CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
                return HttpResponse(status=500)


class order_detail(baseview.BaseView):
    '''

    :argument 执行工单的详细信息请求接口api

    '''

    def get(self, request, args: str = None):

        '''

        :argument 详细信息数据展示

        :param args: 根据获得的work_id 获取该单据的详细信息，审批流，审批记录，详细内容

        :return:

        '''
        try:
            work_id = request.GET.get('workid')
        except KeyError as e:
            CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
        else:
            try:
                workflow_info = workflow_config.objects.raw("select * from core_workflow_config b "
                                                            "where b.name = (SELECT a.assigned FROM core_sqlorder a "
                                                            "WHERE a.work_id = '%s' GROUP BY a.assigned LIMIT 1)" % work_id)
                workflow_record_info = workflow_record.objects.filter(work_id=work_id).values()

                workflow_record_info_data = []
                max_record_step = 1
                for j in workflow_record_info:
                    workflow_record_info_data.append(j)
                    max_record_step = (int(j["step_num"]) if int(j["step_num"]) > max_record_step else max_record_step)

                workflow_info_data = []
                workflow_next = {}
                for i in workflow_info:
                    dic_i = model_to_dict(i)
                    workflow_info_data.append(dic_i)
                    if int(dic_i["step_num"]) == int(max_record_step) + 1:
                        workflow_next = dic_i

                order_info = SqlOrder.objects.filter(work_id=work_id).first()
                order_info_data = model_to_dict(order_info)

                return Response({"workflow_info": workflow_info_data,
                                 "workflow_record": workflow_record_info_data,
                                 "workflow_next": workflow_next,
                                 "order_info": order_info_data})
                # return HttpResponse(simplejson.dumps({"workflow_info": workflow_info, "workflow_record": workflow_record_info}, cls=DateEncoder, bigint_as_string=True))
            except Exception as e:
                CUSTOM_ERROR.error(f'{e.__class__.__name__} : {e}')
                return HttpResponse(status=500)

    def put(self, request, args: str = None):

        '''

        :argument 当工单驳回后重新提交功能接口api

        :param args: 根据获得order_id 返回对应被驳回的sql

        :return:

        '''

        try:
            order_id = request.data['id']
        except KeyError as e:
            CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
        else:
            try:
                info = SqlOrder.objects.raw(
                    "select core_sqlorder.*,core_databaselist.connection_name,\
                    core_databaselist.computer_room from core_sqlorder INNER JOIN \
                    core_databaselist on core_sqlorder.bundle_id = core_databaselist.id \
                    WHERE core_sqlorder.id = %s" % order_id)
                data = util.ser(info)
                sql = data[0]['sql'].split(';')
                _tmp = ''
                for i in sql:
                    _tmp += i + ";\n"
                return Response({'data': data[0], 'sql': _tmp.strip('\n'), 'type': 0})
            except Exception as e:
                CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
                return HttpResponse(status=500)

    def post(self, request, args: str = None):

        '''

        :argument 当工单执行后sql回滚功能接口api

        :param args: 根据获得order_id 返回对应的回滚sql

        :return: {'data': data[0], 'sql': rollback_sql, 'type': 1}

        '''

        try:
            order_id = request.data['id']
            info = list(set(json.loads(request.data['opid'])))
        except KeyError as e:
            CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
            return HttpResponse(status=500)
        else:
            try:
                sql = []
                rollback_sql = []
                for i in info:
                    info = SqlOrder.objects.raw(
                        "select core_sqlorder.*,core_databaselist.connection_name,\
                        core_databaselist.computer_room from core_sqlorder INNER JOIN \
                        core_databaselist on core_sqlorder.bundle_id = core_databaselist.id \
                        WHERE core_sqlorder.id = %s"
                        % order_id)
                    data = util.ser(info)
                    _data = SqlRecord.objects.filter(sequence=i).first()
                    roll = rollback.rollbackSQL(db=_data.backup_dbname, opid=i)
                    link = _data.backup_dbname + '.' + roll
                    sql.append(rollback.roll(backdb=link, opid=i))
                for i in sql:
                    for c in i:
                        rollback_sql.append(c['rollback_statement'])
                rollback_sql = sorted(rollback_sql)
                if rollback_sql == []: return HttpResponse(status=500)
                return Response({'data': data[0], 'sql': rollback_sql, 'type': 1})
            except Exception as e:
                CUSTOM_ERROR.error(f'{e.__class__.__name__}: {e}')
                return HttpResponse(status=500)
