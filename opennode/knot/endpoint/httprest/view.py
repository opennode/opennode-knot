import json
import logging

from grokcore.component import context
from twisted.web.server import NOT_DONE_YET
from zope.authentication.interfaces import IAuthentication
from zope.component import getUtility

from opennode.knot.model.compute import Compute, IVirtualCompute
from opennode.knot.model.machines import Machines
from opennode.knot.model.hangar import Hangar
from opennode.knot.model.virtualizationcontainer import VirtualizationContainer
from opennode.oms.model.model.actions import ActionsContainer
from opennode.oms.model.model.hooks import PreValidateHookMixin
from opennode.oms.model.model.stream import Metrics
from opennode.oms.model.form import RawDataValidatingFactory
from opennode.oms.endpoint.httprest.view import ContainerView
from opennode.oms.endpoint.httprest.base import IHttpRestView
from opennode.oms.endpoint.httprest.root import BadRequest
from opennode.oms.log import UserLogger
from opennode.oms.zodb import db


class MachinesView(ContainerView):
    context(Machines)

    def blacklisted(self, item):
        return super(MachinesView, self).blacklisted(item) or isinstance(item, Hangar)


class SetEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)


class VirtualizationContainerView(ContainerView, PreValidateHookMixin):
    context(VirtualizationContainer)

    def blacklisted(self, item):
        return (super(VirtualizationContainerView, self).blacklisted(item)
                or isinstance(item, ActionsContainer))

    def render_POST(self, request):
        try:
            data = json.load(request.content)
        except ValueError:
            raise BadRequest("Input data could not be parsed")

        if not isinstance(data, dict):
            raise BadRequest("Input data must be a dictionary")

        if 'state' not in data:
            data['state'] = 'active' if data.get('start_on_boot') else 'inactive'

        if data.get('diskspace'):
            data['diskspace'] = {'root': data['diskspace']}

        # XXX: ONC should send us a 'nameserver' list instead of this hackish dns1,dns2
        if 'nameservers' not in data:
            nameservers = []
            for k in ['dns1', 'dns2']:
                if data.get(k, None):
                    nameservers.append(data[k])

            data['nameservers'] = nameservers

        if 'autostart' not in data:
            data['autostart'] = data.get('start_on_boot', False)

        assert data['root_password'] == data['root_password_repeat']
        root_password = data['root_password']

        for k in ('dns1', 'dns2', 'root_password', 'root_password_repeat', 'network-type', 'start_on_boot'):
            if k in data:
                del data[k]

        if 'memory' in data:
            data['memory'] = data['memory'] * 1024  # Memory sent by ONC is in GB, model keeps it in MB

        form = RawDataValidatingFactory(data, Compute, marker=IVirtualCompute)

        if form.errors or not data.get('template'):
            template_error = [dict(id='template', msg="missing value")] if not data.get('template') else []
            return {'success': False,
                    'errors': [dict(id=k, msg=v) for k, v in form.error_dict().items()] + template_error}

        compute = form.create()

        interaction = request.interaction

        if not interaction:
            auth = getUtility(IAuthentication, context=None)
            principal = auth.getPrincipal(None)
        else:
            principal = interaction.participations[0].principal

        @db.transact
        def handle_success(r, compute, principal):
            compute.__owner__ = principal

            compute.root_password = root_password
            self.context.add(compute)

            data['id'] = compute.__name__

            self.add_log_event(principal,
                               'Creation of %s (%s) (via web) successful' % (compute.hostname, compute))

            request.write(json.dumps({'success': True,
                                      'result': IHttpRestView(compute).render_GET(request)},
                                     cls=SetEncoder))
            request.finish()

        def handle_pre_execute_hook_error(f, compute, principal):
            f.trap(Exception)
            self.add_log_event(principal,
                               'Creation of %s (%s) (via web) failed: %s: %s' % (compute.hostname, compute,
                                                                                 type(f.value).__name__,
                                                                                 f.value))
            request.write(json.dumps({'success': False,
                                      'errors': [{'id': 'vm', 'msg': str(f.value)}]}))
            request.finish()

        @db.data_integrity_validator
        def validate_db(r, compute):
            log = logging.getLogger('opennode.oms.zodb.db')
            log.debug('integrity: %s == %s', compute.__name__, list(self.context._items))
            assert compute.__name__ in self.context._items

        d = self.validate_hook(principal)
        d.addCallback(handle_success, compute, principal)
        d.addErrback(handle_pre_execute_hook_error, compute, principal)
        d.addCallback(validate_db, compute)
        return NOT_DONE_YET

    def add_log_event(self, principal, msg, *args, **kwargs):
        owner = self.context.__owner__
        ulog = UserLogger(principal=principal, subject=self.context, owner=owner)
        ulog.log(msg, *args, **kwargs)


class HangarView(ContainerView):
    context(Hangar)

    def render_POST(self, request):
        try:
            data = json.load(request.content)
        except ValueError:
            raise BadRequest("Input data could not be parsed")

        if not isinstance(data, dict):
            raise BadRequest("Input data must be a dictionary")

        form = RawDataValidatingFactory(data, VirtualizationContainer)

        if form.errors or not data.get('backend'):
            backend_error = [dict(id='backend', msg="missing value")] if not data.get('backend') else []
            return {'success': False,
                    'errors': [dict(id=k, msg=v) for k, v in form.error_dict().items()] + backend_error}

        vms = form.create()
        self.context.add(vms)

        return {'success': True, 'result': IHttpRestView(vms).render_GET(request)}


class ComputeView(ContainerView):
    context(Compute)

    def render_recursive(self, request, *args, **kwargs):
        ret = super(ComputeView, self).render_recursive(request, *args, **kwargs)
        ret.update({'uptime': self.context.uptime})
        ret.update({'owner': self.context.__owner__})
        return self.filter_attributes(request, ret)

    def blacklisted(self, item):
        return (super(ComputeView, self).blacklisted(item)
                or isinstance(item, ActionsContainer)
                or isinstance(item, Metrics))

    def put_filter_attributes(self, request, data):
        data = super(ComputeView, self).put_filter_attributes(request, data)
        if 'template' in data and not IVirtualCompute.providedBy(self.context):
            del data['template']
        return data
