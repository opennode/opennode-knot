import transaction
from grokcore.component import context

from opennode.knot.model.template import GlobalTemplates
from opennode.knot.model.template import ITemplate

from opennode.oms.endpoint.ssh.cmdline import VirtualConsoleArgumentParser
from opennode.oms.endpoint.ssh.cmd.security import SetAclMixin
from opennode.oms.endpoint.ssh.cmd.security import require_admins_only_action
from opennode.oms.model.model.actions import Action, action
from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.security.acl import NoSuchPermission
from opennode.oms.zodb import db


class SetGlobalTemplatePermissionsAction(Action, SetAclMixin):
    context(GlobalTemplates)
    action('set-template-perm')

    @db.ro_transact(proxy=False)
    def subject(self, *args, **kwargs):
        return tuple((follow_symlinks(args[0]),))

    def arguments(self):
        parser = VirtualConsoleArgumentParser()
        parser.add_argument('paths', nargs='+',
                            help='Prototype template to be used to find all similar ones')

        group = parser.add_mutually_exclusive_group(required=True)
        group.add_argument('-i', action='store_true',
                           help='Set object to inherit permissions from its parent(s)',
                           default=False)
        group.add_argument('-m', action='append',
                           help="add an Allow ace: {u:[user]:permspec|g:[group]:permspec}")
        group.add_argument('-d', action='append',
                           help="add an Deny ace: {u:[user]:permspec|g:[group]:permspec}")
        group.add_argument('-x', action='append',
                           help="remove an ace: {u:[user]:permspec|g:[group]:permspec}")

        return parser

    @require_admins_only_action
    @db.transact
    def execute(self, cmd, args):
        self.write = cmd.write
        try:
            gtemplates = db.get_root()['oms_root']['templates']
            for path in args.paths:
                proto = cmd.traverse(path)
                action_list = []
                for t in filter(ITemplate.providedBy, map(follow_symlinks, gtemplates.listcontent())):
                    if t.name == proto.name:
                        action_list.append(t)

                for t in action_list:
                    with cmd.protocol.interaction:
                        self.set_acl(t, args.i, args.m, args.d, args.x)
        except NoSuchPermission as e:
            self.write("No such permission '%s'\n" % (e.message))
            transaction.abort()
