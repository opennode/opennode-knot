import json

from grokcore.component import context
from zope.authentication.interfaces import IAuthentication
from zope.component import getUtility

from opennode.knot.model.compute import Compute, IVirtualCompute
from opennode.knot.model.machines import Machines
from opennode.knot.model.hangar import Hangar
from opennode.knot.model.virtualizationcontainer import VirtualizationContainer
from opennode.oms.model.model.actions import ActionsContainer
from opennode.oms.model.model.stream import Metrics
from opennode.oms.model.form import RawDataValidatingFactory
from opennode.oms.endpoint.httprest.view import ContainerView
from opennode.oms.endpoint.httprest.base import IHttpRestView
from opennode.oms.endpoint.httprest.root import BadRequest
from opennode.oms.security.checker import get_interaction


class MachinesView(ContainerView):
    context(Machines)

    def blacklisted(self, item):
        return super(MachinesView, self).blacklisted(item) or isinstance(item, Hangar)


class VirtualizationContainerView(ContainerView):
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
            data['state'] = 'active' if data['start_on_boot'] else 'inactive'

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
            data['autostart'] = data['start_on_boot']

        assert data['root_password'] == data['root_password_repeat']
        root_password = data['root_password']

        for k in ('dns1', 'dns2', 'root_password', 'root_password_repeat', 'network-type', 'start_on_boot'):
            if k in data:
                del data[k]

        form = RawDataValidatingFactory(data, Compute, marker=IVirtualCompute)

        if form.errors or not data.get('template'):
            template_error = [dict(id='template', msg="missing value")] if not data.get('template') else []
            return {'success': False,
                    'errors': [dict(id=k, msg=v) for k, v in form.error_dict().items()] + template_error}

        compute = form.create()

        interaction = get_interaction(self.context) or request.interaction
        if not interaction:
            auth = getUtility(IAuthentication, context=None)
            principal = auth.getPrincipal(None)
        else:
            principal = interaction.participations[0].principal

        compute.__owner__ = principal

        compute.root_password = root_password
        self.context.add(compute)

        data['id'] = compute.__name__

        return {'success': True, 'result': IHttpRestView(compute).render_GET(request)}


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
