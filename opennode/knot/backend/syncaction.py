import logging
import netaddr
import argparse

from twisted.internet import defer
from twisted.python import log
from zope.authentication.interfaces import IAuthentication
from zope.component import getUtility

from opennode.knot.backend.compute import any_stack_installed
from opennode.knot.backend.compute import ComputeAction
from opennode.knot.backend.operation import IAgentVersion
from opennode.knot.backend.operation import IListVMS
from opennode.knot.backend.operation import IGetComputeInfo
from opennode.knot.backend.operation import IGetVirtualizationContainers
from opennode.knot.backend.operation import IGetLocalTemplates
from opennode.knot.backend.operation import IGetRoutes
from opennode.knot.backend.operation import IGetHWUptime
from opennode.knot.backend.operation import IGetDiskUsage
from opennode.knot.backend.operation import ISetOwner
from opennode.knot.backend.operation import OperationRemoteError
from opennode.knot.backend.syncvmsaction import SyncVmsAction
from opennode.knot.backend.v12ncontainer import IVirtualizationContainerSubmitter
from opennode.knot.backend.v12ncontainer import backends
from opennode.knot.model.compute import IUndeployed, IDeployed, IDeploying
from opennode.knot.model.compute import IVirtualCompute
from opennode.knot.model.console import TtyConsole, SshConsole, OpenVzConsole, VncConsole
from opennode.knot.model.network import NetworkInterface, NetworkRoute
from opennode.knot.model.template import Template, Templates
from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer, VirtualizationContainer

from opennode.oms.endpoint.ssh.cmdline import VirtualConsoleArgumentParser
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.model.form import TmpObj
from opennode.oms.model.form import alsoProvides
from opennode.oms.model.form import noLongerProvides
from opennode.oms.model.model.actions import action
from opennode.oms.model.model.symlink import Symlink, follow_symlinks
from opennode.oms.model.traversal import canonical_path
from opennode.oms.util import get_u, get_i, get_f
from opennode.oms.zodb import db


class SyncAction(ComputeAction):
    """Force compute sync"""
    action('sync')

    _do_not_enqueue = True
    _additional_keys = tuple()
    _full = False

    @db.ro_transact(proxy=False)
    def subject(self, *args, **kwargs):
        return tuple((self.context,))

    def arguments(self):
        parser = VirtualConsoleArgumentParser()
        parser.add_argument('-f', '--full', action='store_true',
                           help='Perform full sync',
                           default=False)
        return parser

    @property
    def lock_keys(self):
        if IVirtualCompute.providedBy(self.context):
            return (canonical_path(self.context),
                    canonical_path(self.context.__parent__),
                    canonical_path(self.context.__parent__.__parent__)) + self._additional_keys
        else:
            return (canonical_path(self.context),) + self._additional_keys

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        # XXX for some strange reason args is object()
        if type(args) == argparse.Namespace:
            self._full = args.full
        log.msg('Executing SyncAction on %s (%s)' % (self.context, canonical_path(self.context)),
                 system='sync-action')

        if any_stack_installed(self.context):
            yield self.sync_agent_version(self._full)
            yield self.sync_hw(self._full)
            yield self.ensure_vms(self._full)
            if self._full:
                yield SyncTemplatesAction(self.context)._execute(DetachedProtocol(), object())
        else:
            log.msg('No stacks installed on %s: %s' % (self.context, self.context.features))

        default = yield self.default_console()
        yield self.sync_consoles()

        if IVirtualCompute.providedBy(self.context):
            yield self._sync_virtual()

        yield self._create_default_console(default)

        @db.transact
        def set_additional_keys():
            vms = follow_symlinks(self.context['vms'])
            if vms:
                self._additional_keys = (canonical_path(vms),)

        yield set_additional_keys()
        dl = yield self.reacquire()
        if dl is not None:
            return

        yield self.sync_vms()

    @defer.inlineCallbacks
    def sync_agent_version(self, full):
        if not full:
            return

        log.msg('Syncing version on %s...' % (self.context), system='sync-action')
        minion_v = (yield IAgentVersion(self.context).run()).split('.')
        # XXX: Salt-specific
        from opennode.knot.backend.salt import get_master_version
        master_v = (yield get_master_version()).split('.')

        if master_v[0] != minion_v[0]:
            @db.transact
            def set_failure():
                self.context.failure = True
            yield set_failure()
            log.msg('Major agent version mismatch: master %s != minion %s on %s'
                    % (master_v, minion_v, self.context),
                    system='sync-action', logLevel=logging.ERROR)
            raise Exception('Major agent version mismatch')

        if master_v[1] != minion_v[1]:
            @db.transact
            def set_suspicious():
                self.context.suspicious = True
            yield set_suspicious()
            log.msg('Minor agent version mismatch: master %s != minion %s on %s'
                    % (master_v, minion_v, self.context),
                    system='sync-action', logLevel=logging.WARNING)
        elif master_v != minion_v:
            log.msg('Release agent version mismatch: master %s != minion %s on %s'
                    % (master_v, minion_v, self.context),
                    system='sync-action')

    @db.ro_transact
    def default_console(self):
        return self._default_console()

    @db.assert_transact
    def _default_console(self):
        if self.context['consoles']:
            return

        default = self.context.consoles['default']
        if default:
            return default.target.__name__

    @defer.inlineCallbacks
    def _create_default_console(self, default):
        @db.ro_transact
        def check():
            return not default or not self.context.consoles[default]

        @db.transact
        def create():
            self.create_default_console(default)

        if (yield check()):
            yield create()

    @db.assert_transact
    def create_default_console(self, default):
        if not default or not self.context.consoles[default]:
            if (IVirtualizationContainer.providedBy(self.context.__parent__)
                    and self.context.__parent__.backend == 'openvz'
                    and self.context.consoles['tty0']):
                default = 'tty0'
            else:
                default = 'ssh'

            self.context.consoles.add(Symlink('default', self.context.consoles[default]))

    @db.transact
    def sync_consoles(self):
        return self._sync_consoles()

    @db.assert_transact
    def _sync_consoles(self):
        if self.context['consoles'] and self.context.consoles['ssh']:
            return self.fixup_console_ip(self.context.consoles['ssh'])

        address = self.context.hostname
        try:
            if self.context.ipv4_address:
                address = self.context.ipv4_address.split('/')[0]
        except Exception:
            log.err(system='sync-consoles')
        ssh_console = SshConsole('ssh', 'root', address, 22)
        self.context.consoles.add(ssh_console)

    def fixup_console_ip(self, console):
        if self.context.ipv4_address:
            address = self.context.ipv4_address.split('/')[0]
            if console.hostname != address:
                console.hostname = address

    @defer.inlineCallbacks
    def _sync_virtual(self):
        parent = yield db.get(self.context, '__parent__')
        name = yield db.get(self.context, '__name__')
        submitter = IVirtualizationContainerSubmitter(parent)
        vmlist = yield submitter.submit(IListVMS)
        for vm in vmlist:
            if vm['uuid'] == name:
                yield self.sync_owner(vm)
                yield self._sync_vm(vm)

    @db.transact
    def _sync_vm(self, vm):
        return self.sync_vm(vm)

    @defer.inlineCallbacks
    def sync_owner(self, vm):
        owner = yield db.get(self.context, '__owner__')
        parent = yield db.get(self.context, '__parent__')
        uuid = yield db.get(self.context, '__name__')

        # PUSH if owner is set, PULL only when OMS owner is not set
        if owner is not None:
            log.msg('Attempting to push owner (%s) of %s to agent' % (owner, self.context), system='sync')
            submitter = IVirtualizationContainerSubmitter(parent)
            yield submitter.submit(ISetOwner, uuid, owner)
            log.msg('Owner pushing for %s successful' % self.context, system='sync')
        elif vm.get('owner'):
            @db.transaft
            def pull_owner():
                compute = TmpObj(self.context)
                newowner = getUtility(IAuthentication).getPrincipal(vm['owner'])
                compute.__owner__ = newowner
                compute.apply()
            yield pull_owner()


    @db.assert_transact
    def sync_owner_transact(self, vm):
        owner = self.context.__owner__
        parent = self.context.__parent__
        uuid = self.context.__name__

        if vm.get('owner'):
            if owner != vm['owner']:
                compute = TmpObj(self.context)
                newowner = getUtility(IAuthentication).getPrincipal(vm['owner'])
                if newowner is not None:
                    log.msg('Modifying ownership of "%s" from %s to %s.'
                            % (compute, owner, newowner), system='sync')
                    compute.__owner__ = newowner
                    compute.apply()
                else:
                    log.msg('User not found: "%s" while restoring owner for %s. '
                            'Leaving as-is' % (vm['owner'], compute), system='sync')
        elif owner is not None:
            log.msg('Attempting to push owner (%s) of %s to agent' % (owner, self.context), system='sync')
            submitter = IVirtualizationContainerSubmitter(parent)
            d = submitter.submit(ISetOwner, uuid, owner)
            d.addCallback(lambda r: log.msg('Owner pushing for %s successful' % self.context, system='sync'))
            d.addErrback(log.err, system='sync')

    @db.assert_transact
    def sync_vm(self, vm):
        compute = TmpObj(self.context)
        compute.state = unicode(vm['state'])

        # Ensure IDeployed marker is set, unless not in another state
        if not IDeployed.providedBy(compute):
            noLongerProvides(self.context, IUndeployed)
            noLongerProvides(self.context, IDeploying)
            alsoProvides(self.context, IDeployed)

        if 'ctid' in vm:
            compute.ctid = vm['ctid'] if vm['ctid'] != '-' else -1

        for idx, console in enumerate(vm['consoles']):
            if console['type'] == 'pty' and not self.context.consoles['tty%s' % idx]:
                self.context.consoles.add(TtyConsole('tty%s' % idx, console['pty']))
            if console['type'] == 'openvz' and not self.context.consoles['tty%s' % idx]:
                self.context.consoles.add(OpenVzConsole('tty%s' % idx, console['cid']))
            if console['type'] == 'vnc' and not self.context.consoles['vnc']:
                self.context.consoles.add(VncConsole(
                    self.context.__parent__.__parent__.hostname, int(console['port'])))

        # XXX TODO: handle removal of consoles when they are no longer reported from upstream
        # networks
        for interface in vm['interfaces']:
            if not self.context.interfaces[interface['name']]:
                iface = NetworkInterface(interface['name'], None, interface['mac'], 'active')
                if 'ipv4_address' in interface:
                    iface.ipv4_address = interface['ipv4_address']
                self.context.interfaces.add(iface)

        # XXX TODO: handle removal of interfaces when they are no longer reported from upstream
        # XXX hack, openvz specific
        compute.cpu_info = self.context.__parent__.__parent__.cpu_info
        compute.memory = vm['memory']

        diskspace = dict((unicode(k), v) for k, v in vm['diskspace'].items())
        diskspace[u'total'] = sum([0.0] + vm['diskspace'].values())

        # round diskspace values
        for i in diskspace:
            diskspace[i] = round(diskspace[i], 2)

        compute.diskspace = diskspace

        if compute.state == 'active':
            compute.uptime = get_f(vm, 'uptime')
        else:
            compute.uptime = None

        compute.num_cores = vm['vcpu']
        compute.swap_size = vm.get('swap') or compute.swap_size
        compute.kernel = vm.get('kernel') or compute.kernel

        compute.apply()

    @defer.inlineCallbacks
    def sync_hw(self, full):
        if not any_stack_installed(self.context):
            return

        try:
            info = yield IGetComputeInfo(self.context).run()
            uptime = yield IGetHWUptime(self.context).run()
            disk_usage = yield IGetDiskUsage(self.context).run()
        except OperationRemoteError as e:
            log.msg(e.message, system='sync-hw')
            if e.remote_tb:
                log.msg(e.remote_tb, system='sync-hw')
            return

        # TODO: Improve error handling
        def disk_info(aspect):
            res = dict((unicode(k), round(float(v[aspect]) / 1024, 2))
                       for k, v in disk_usage.items()
                       if v['device'].startswith('/dev/'))
            res[u'total'] = sum([0.0] + res.values())
            return res

        routes = yield IGetRoutes(self.context).run() if full else []

        yield self._sync_hw(info, disk_info('total'), disk_info('used'), routes, uptime)

    @db.transact
    def _sync_hw(self, info, disk_space, disk_usage, routes, uptime):
        self.context.uptime = uptime

        if any((not info, 'cpuModel' not in info, 'kernelVersion' not in info)):
            log.msg('Nothing to update: info does not include required data', system='sync-hw')
            return

        if IVirtualCompute.providedBy(self.context):
            self.context.cpu_info = self.context.__parent__.__parent__.cpu_info
        else:
            self.context.cpu_info = unicode(info['cpuModel'])

        self.context.architecture = (unicode(info['platform']), u'linux', self.distro(info))
        self.context.kernel = unicode(info['kernelVersion'])
        self.context.memory = info['systemMemory']
        self.context.num_cores = info['numCpus']
        self.context.os_release = unicode(info['os'])
        self.context.swap_size = info['systemSwap']
        self.context.diskspace = disk_space
        self.context.diskspace_usage = disk_usage
        self.context.template = u'Hardware node'

        # XXX TODO: handle removal of routes
        for r in routes:
            destination = netaddr.IPNetwork('%s/%s' % (r['destination'], r['netmask']))
            route_name = str(destination.cidr).replace('/', '_')

            if self.context.routes[route_name]:
                continue

            gateway = netaddr.IPAddress(r['router'])

            route = NetworkRoute()
            route.destination = str(destination.cidr)
            route.gateway = str(gateway)
            route.flags = r['flags']
            route.metrics = int(r['metrics'])
            route.__name__ = route_name

            interface = self.context.interfaces[r['interface']]
            if interface:
                route.add(Symlink('interface', interface))

            self.context.routes.add(route)

    def distro(self, info):
        if 'os' in info:
            return unicode(info['os'].split()[0])
        else:
            return 'Unknown'

    @defer.inlineCallbacks
    def ensure_vms(self, full):
        if (not any_stack_installed(self.context) or
            # if not full sync and there are virtualization containers available
            (not full and len(filter(lambda n: 'vms' in n, self.context.listnames())) > 0)):
            return

        vms_types = yield IGetVirtualizationContainers(self.context).run()

        if not vms_types:
            return

        url_to_backend_type = dict((v, k) for k, v in backends.items())

        @db.transact
        def add_container(backend_type):
            log.msg('Adding backend %s' % backend_type, system='sync')
            vms = VirtualizationContainer(unicode(backend_type))

            if vms.__name__ in self.context.listnames():
                return

            self.context.add(vms)
            if not self.context['vms']:
                self.context.add(Symlink('vms', self.context[vms.__name__]))

        for vms_type in vms_types:
            backend_type = url_to_backend_type.get(vms_type)
            if not backend_type:
                log.msg('Unrecognized backend: %s. Skipping' % vms_type, system='sync')
                continue

            yield add_container(backend_type)

    @defer.inlineCallbacks
    def sync_vms(self):
        for vms in self.context.listcontent():
            if not IVirtualizationContainer.providedBy(vms):
                continue
            yield SyncVmsAction(vms)._execute(DetachedProtocol(), object())


class SyncTemplatesAction(ComputeAction):
    """Compute templates sync"""
    action('sync-templates')

    @db.ro_transact(proxy=False)
    def subject(self, *args, **kwargs):
        return tuple((self.context,))

    @defer.inlineCallbacks
    def _execute(self, cmd, args):
        if not any_stack_installed(self.context):
            return

        @db.transact
        def update_templates(container, templates):
            if not container['templates']:
                container.add(Templates())

            template_container = container['templates']

            for template in templates:
                name = template['template_name']

                if not template_container['by-name'][name]:
                    template_container.add(Template(unicode(name), get_u(template, 'domain_type')))

                t = template_container['by-name'][name].target
                t.cores = (get_i(template, 'vcpu_min'),
                           get_i(template, 'vcpu'),
                           max(-1, get_i(template, 'vcpu_max')))
                t.memory = (get_f(template, 'memory_min'),
                            get_f(template, 'memory'),
                            max(-1.0, get_f(template, 'memory_max')))
                t.swap = (get_f(template, 'swap_min'),
                          get_f(template, 'swap'),
                          max(-1.0, get_f(template, 'swap_max')))
                t.disk = (get_f(template, 'disk_min'),
                          get_f(template, 'disk'),
                          max(-1.0, get_f(template, 'disk_max')))
                t.nameserver = get_u(template, 'nameserver')

                t.username = get_u(template, 'username')
                t.password = get_u(template, 'passwd')

                t.cpu_limit = (get_i(template, 'vcpulimit_min'),
                               get_i(template, 'vcpulimit'))
                t.ip = get_u(template, 'ip_address')

            # delete templates no more offered upstream
            template_names = template_container['by-name'].listnames()
            vanished_template_names = set(template_names).difference(
                template['template_name'] for template in templates)

            for template in vanished_template_names:
                template_container.remove(follow_symlinks(template_container['by-name'][template]))

        for container in self.context.listcontent():
            if not IVirtualizationContainer.providedBy(container):
                continue

            submitter = IVirtualizationContainerSubmitter(container)
            templates = yield submitter.submit(IGetLocalTemplates)

            if not templates:
                log.msg('Did not find any templates on %s/%s' % (self.context, container),
                        system='sync-templates')
                continue

            log.msg('Synced templates on %s (%s). Updating %s templates' %
                    (self.context, container, len(templates)), system='sync-templates')

            yield update_templates(container, templates)
