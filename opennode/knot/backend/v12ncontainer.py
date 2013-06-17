from grokcore.component import Adapter, context, implements
from twisted.internet import defer
from twisted.python import log
from zope.interface import Interface

from opennode.knot.backend.operation import IListVMS, OperationRemoteError
from opennode.knot.model.virtualizationcontainer import IVirtualizationContainer
from opennode.oms.model.model.actions import Action, action
from opennode.oms.zodb import db


backends = {'test': 'test:///tmp/salt_vm_test_state.xml',
            'openvz': 'openvz:///system',
            'kvm': 'qemu:///system',
            'xen': 'xen:///'}


class IVirtualizationContainerSubmitter(Interface):
    def submit(job_interface):
        """Submits a job to the virtualization container"""


class VirtualizationContainerSubmitter(Adapter):
    implements(IVirtualizationContainerSubmitter)
    context(IVirtualizationContainer)

    @defer.inlineCallbacks
    def submit(self, job_interface, *args, **kwargs):

        # we cannot return a deferred from a db.transact
        @db.ro_transact
        def get_job():
            job = job_interface(self.context.__parent__)
            backend_uri = backends.get(self.context.backend, self.context.backend)
            return (job, backend_uri)

        job, backend_uri = yield get_job()
        d = job.run(backend_uri, *args, __killhook=kwargs.get('__killhook'))

        def on_remote_error(e):
            e.trap(OperationRemoteError)
            try:
                e.raiseException()
            except OperationRemoteError as ore:
                log.msg(e.getErrorMessage(), _why='Remote error', system='v12n-submitter')
                if ore.remote_tb:
                    log.msg(ore.remote_tb, system='v12n-submitter')
                raise

        def on_unexpected_error(e):
            e.trap(Exception)
            log.msg('Unexpected error! %s' % e.value, system='v12n-submitter')
            log.err(system='v12n-submitter')
            e.raiseException()

        d.addErrback(on_remote_error)
        d.addErrback(on_unexpected_error)
        res = yield d
        defer.returnValue(res)


class ListVirtualizationContainerAction(Action):
    """Lists the content of a virtualizationcontaineraction.
    Usually the zodb will be in sync, but it can be useful to see real time info (perhaps just for test)."""

    context(IVirtualizationContainer)
    action('list')

    @db.ro_transact(proxy=False)
    def subject(self, *args, **kwargs):
        return tuple((self.context,))

    @defer.inlineCallbacks
    def execute(self, cmd, args):
        cmd.write("listing virtual machines\n")
        db.assert_proxy(self.context)

        submitter = IVirtualizationContainerSubmitter(self.context)

        try:
            vms = yield submitter.submit(IListVMS)
        except Exception as e:
            cmd.write("%s\n" % (": ".join(str(msg) for msg in e.args
                                if (not isinstance(msg, str) or not msg.startswith('  File "/')))))
            return

        max_key_len = max([0] + [len(vm['name']) for vm in vms])

        for vm in vms:
            vm['name'] = vm['name'].ljust(max_key_len)
            cmd.write("%(name)s:  state=%(state)s, run_state=%(run_state)s, uuid=%(uuid)s, "
                      "memory=%(memory)s, template=%(template)s\n" % vm)

            if vm['diskspace']:
                cmd.write(" %s    storage:\n" % (' ' * max_key_len))
                for storage in vm['diskspace']:
                    cmd.write(" %s      %s = %s\n" % (' ' * max_key_len, storage, vm['diskspace'][storage]))

            if vm['consoles']:
                cmd.write(" %s    consoles:\n" % (' ' * max_key_len))

            for console in vm['consoles']:
                attrs = " ".join(["%s=%s" % pair for pair in console.items()])
                cmd.write(" %s      %s\n" % (' ' * max_key_len, attrs))


