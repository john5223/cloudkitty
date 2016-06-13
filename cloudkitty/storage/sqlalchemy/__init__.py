# -*- coding: utf-8 -*-
# Copyright 2014 Objectif Libre
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.
#
# @author: Stéphane Albert
#
from decimal import Decimal
import json

from oslo_db.sqlalchemy import utils
import sqlalchemy
from sqlalchemy import Text
from sqlalchemy.dialects.postgresql import JSON

from cloudkitty import db
from cloudkitty import storage
from cloudkitty.storage.sqlalchemy import migration
from cloudkitty.storage.sqlalchemy import models
from cloudkitty import utils as ck_utils

from oslo_log import log as logging
LOG = logging.getLogger(__name__)


class SQLAlchemyStorage(storage.BaseStorage):
    """SQLAlchemy Storage Backend

    """
    frame_model = models.RatedDataFrame

    def __init__(self, **kwargs):
        super(SQLAlchemyStorage, self).__init__(**kwargs)
        self._session = {}

    @staticmethod
    def init():
        migration.upgrade('head')

    def _pre_commit(self, tenant_id):
        self._check_session(tenant_id)
        if not self._has_data.get(tenant_id):
            empty_frame = {'vol': {'qty': 0, 'unit': 'None'},
                           'rating': {'price': 0}, 'desc': ''}
            self._append_time_frame('_NO_DATA_', empty_frame, tenant_id)

    def _commit(self, tenant_id):
        self._session[tenant_id].commit()

    def _post_commit(self, tenant_id):
        super(SQLAlchemyStorage, self)._post_commit(tenant_id)
        del self._session[tenant_id]

    def _check_session(self, tenant_id):
        session = self._session.get(tenant_id)
        if not session:
            self._session[tenant_id] = db.get_session()
            self._session[tenant_id].begin()

    def _dispatch(self, data, tenant_id):
        self._check_session(tenant_id)
        for service in data:
            for frame in data[service]:
                self._append_time_frame(service, frame, tenant_id)
                self._has_data[tenant_id] = True

    def get_state(self, tenant_id=None):
        session = db.get_session()
        q = utils.model_query(
            self.frame_model,
            session)
        if tenant_id:
            q = q.filter(
                self.frame_model.tenant_id == tenant_id)
        q = q.order_by(
            self.frame_model.begin.desc())
        r = q.first()
        if r:
            return ck_utils.dt2ts(r.begin)

    def get_total(self, begin=None, end=None, tenant_id=None, service=None):
        # Boundary calculation
        if not begin:
            begin = ck_utils.get_month_start()
        if not end:
            end = ck_utils.get_next_month()

        session = db.get_session()
        q = session.query(
            sqlalchemy.func.sum(self.frame_model.rate).label('rate'))
        if tenant_id:
            q = q.filter(
                self.frame_model.tenant_id == tenant_id)
        if service:
            q = q.filter(
                self.frame_model.res_type == service)
        q = q.filter(
            self.frame_model.begin >= begin,
            self.frame_model.end <= end)
        rate = q.scalar()
        return rate

    def get_tenants(self, begin=None, end=None):
        # Boundary calculation
        if not begin:
            begin = ck_utils.get_month_start()
        if not end:
            end = ck_utils.get_next_month()

        session = db.get_session()
        q = utils.model_query(
            self.frame_model,
            session)
        q = q.filter(
            self.frame_model.begin >= begin,
            self.frame_model.end <= end)
        tenants = q.distinct().values(
            self.frame_model.tenant_id)
        return [tenant.tenant_id for tenant in tenants]

    def get_time_frame(self, begin, end, **filters):
        session = db.get_session()
        q = utils.model_query(
            self.frame_model,
            session)
        q = q.filter(
            self.frame_model.begin >= ck_utils.ts2dt(begin),
            self.frame_model.end <= ck_utils.ts2dt(end))
        for filter_name, filter_value in filters.items():
            if filter_value:
                q = q.filter(
                    getattr(self.frame_model, filter_name) == filter_value)
        if not filters.get('res_type'):
            q = q.filter(self.frame_model.res_type != '_NO_DATA_')
        count = q.count()
        if not count:
            raise storage.NoTimeFrame()
        r = q.all()
        return [entry.to_cloudkitty(self._collector) for entry in r]

    def _append_time_frame(self, res_type, frame, tenant_id):
        vol_dict = frame['vol']
        qty = vol_dict['qty']
        unit = vol_dict['unit']
        rating_dict = frame.get('rating', {})
        rate = rating_dict.get('price')
        if not rate:
            rate = Decimal(0)
        desc = json.dumps(frame['desc'])
        self.add_time_frame(begin=self.usage_start_dt.get(tenant_id),
                            end=self.usage_end_dt.get(tenant_id),
                            tenant_id=tenant_id,
                            unit=unit,
                            qty=qty,
                            res_type=res_type,
                            rate=rate,
                            desc=desc)

    def add_time_frame(self, **kwargs):
        """Create a new time frame.

        :param begin: Start of the dataframe.
        :param end: End of the dataframe.
        :param tenant_id: tenant_id of the dataframe owner.
        :param unit: Unit of the metric.
        :param qty: Quantity of the metric.
        :param res_type: Type of the resource.
        :param rate: Calculated rate for this dataframe.
        :param desc: Resource description (metadata).
        """
        frame = self.frame_model(**kwargs)
        self._session[kwargs.get('tenant_id')].add(frame)

    def get_project_usage(self, begin=None, end=None,
                          tenant_id=None, service=None):
        if not begin:
            begin = ck_utils.get_month_start()
        if not end:
            end = ck_utils.get_next_month()

        fm = self.frame_model
        session = db.get_session()
        instance_id = sqlalchemy.func.array_agg(
            sqlalchemy.case(
                [(fm.res_type == 'image',
                  fm.desc.cast(JSON)[('properties.instance_uuid')].astext)
                 ],
                else_=fm.desc.cast(JSON)[('instance_id')].astext
            ).distinct())
        instance_name = sqlalchemy.func.array_agg(
            sqlalchemy.case(
                [(fm.res_type == 'compute',
                  fm.desc.cast(JSON)[('name')].astext),
                 (fm.res_type.in_(['network.bw.in', 'network.bw.out']),
                  fm.desc.cast(JSON)[("display_name")].astext)
                 ],
                else_=fm.desc.cast(JSON)[('instance_name')].astext
            ).distinct())
        q = session.query(
            self.frame_model.tenant_id.label('tenant_id'),
            self.frame_model.res_type.label('res_type'),
            instance_id.label('instance_id'),
            instance_name.label('instance_name'),
            fm.unit.label('unit'),
            sqlalchemy.func.sum(fm.qty).label('qty'),
            sqlalchemy.func.sum(fm.rate).label('rate'))
        if tenant_id:
            q = q.filter(
                self.frame_model.tenant_id == tenant_id)
        if service:
            q = q.filter(
                self.frame_model.res_type == service)
        q = q.filter(
            self.frame_model.begin >= begin,
            self.frame_model.end <= end)
        q = q.filter(self.frame_model.res_type != '_NO_DATA_')
        q = q.group_by(fm.tenant_id, fm.res_type, fm.unit)
        result = [x._asdict() for x in q.all()]
        usage = {}
        for r in result:
            tenant_id = r['tenant_id']
            resource_type = r['res_type']
            usage.setdefault(tenant_id, {})
            tenant_usage = usage[tenant_id]
            tenant_usage.setdefault('total', {'rate': Decimal(0)})
            tenant_usage['total']['rate'] += Decimal(r['rate'])
            tenant_usage.setdefault(resource_type, list())
            tenant_usage[resource_type].append({
                'qty': r['qty'],
                'unit': r['unit'],
                'rate': r['rate']
            })
        return usage

    def get_instance_usage(self, begin=None, end=None,
                           tenant_id=None, service=None):
        if not begin:
            begin = ck_utils.get_month_start()
        if not end:
            end = ck_utils.get_next_month()

        fm = self.frame_model
        session = db.get_session()
        instance_id = sqlalchemy.case(
            [(fm.res_type == 'image',
              fm.desc.cast(JSON)[('properties.instance_uuid')].astext)
             ],
            else_=fm.desc.cast(JSON)[('instance_id')].astext
        )
        instance_name = sqlalchemy.func.array_agg(
            sqlalchemy.case(
                [(fm.res_type == 'compute',
                  fm.desc.cast(JSON)[('name')].astext),
                 (fm.res_type.in_(['network.bw.in', 'network.bw.out']),
                  fm.desc.cast(JSON)[("display_name")].astext),
                 ],
                else_=fm.desc.cast(JSON)[('instance_name')].astext
            ).distinct())
        q = session.query(
            self.frame_model.tenant_id.label('tenant_id'),
            self.frame_model.res_type.label('res_type'),
            self.frame_model.unit.label('unit'),
            instance_id.label('instance_id'),
            instance_name.label('instance_name'),
            sqlalchemy.func.sum(self.frame_model.qty).label('qty'),
            sqlalchemy.func.sum(self.frame_model.rate).label('rate'))
        if tenant_id:
            q = q.filter(
                self.frame_model.tenant_id == tenant_id)
        if service:
            q = q.filter(
                self.frame_model.res_type == service)
        q = q.filter(
            self.frame_model.begin >= begin,
            self.frame_model.end <= end)
        q = q.filter(self.frame_model.res_type != '_NO_DATA_')
        q = q.group_by(
            self.frame_model.tenant_id,
            self.frame_model.res_type,
            self.frame_model.unit,
            'instance_id')
        result = [x._asdict() for x in q.all()]
        usage = {}
        for r in result:
            tenant_id = r['tenant_id']
            usage.setdefault(tenant_id, {})
            tenant_usage = usage[tenant_id]
            tenant_usage.setdefault(r['instance_id'],
                                    {'instance_name': r['instance_name'],
                                     'instance_id': r['instance_id']})
            instance_usage = tenant_usage[r['instance_id']]
            instance_usage.setdefault(r['res_type'], list())
            instance_usage[r['res_type']].append({
                'qty': r['qty'],
                'unit': r['unit'],
                'rate': r['rate']
            })
        return usage

    def get_bandwidth_hourly_usage(self, begin=None, end=None,
                                   tenant_id=None, service=None):
        if not begin:
            begin = ck_utils.get_month_start()
        if not end:
            end = ck_utils.get_next_month()

        fm = self.frame_model
        session = db.get_session()
        bandwidth_in = sqlalchemy.func.sum(
            sqlalchemy.case(
                [(fm.res_type == 'network.bw.in', fm.qty)],
                else_=0
            )
        )
        bandwidth_out = sqlalchemy.func.sum(
            sqlalchemy.case(
                [(fm.res_type == 'network.bw.out', fm.qty)],
                else_=0
            )
        )
        q = session.query(
            fm.begin.cast(Text).label('begin'),
            fm.end.cast(Text).label('end'),
            fm.tenant_id.label('tenant_id'),
            fm.unit.label('unit'),
            bandwidth_in.label('network.bw.in'),
            bandwidth_out.label('network.bw.out'),
        )
        if tenant_id:
            q = q.filter(
                fm.tenant_id == tenant_id)
        if service:
            q = q.filter(
                fm.res_type == service)
        q = q.filter(
            fm.begin >= begin,
            fm.end <= end)
        q = q.filter(fm.res_type.in_(['network.bw.in', 'network.bw.out']))
        q = q.group_by(fm.begin, fm.end, fm.tenant_id, fm.unit)
        result = [x._asdict() for x in q.all()]
        usage = {
            'data': result,
            'total': {
                'network.bw.in': Decimal(0),
                'network.bw.out': Decimal(0),
            }
        }
        for r in result:
            usage['total']['network.bw.in'] += Decimal(r['network.bw.in'])
            usage['total']['network.bw.out'] += Decimal(r['network.bw.out'])
            usage['total']['unit'] = r['unit']
        return usage
