import collections
import json
import yaml

from grokcore.component import implements
from twisted.internet import defer
from twisted.python import log
from zope.authentication.interfaces import IAuthentication
from zope.component import getUtility
from zope.schema.interfaces import WrongType, RequiredMissing
from zope.security.proxy import removeSecurityProxy
from zope.securitypolicy.interfaces import IPrincipalRoleManager
from zope.securitypolicy.rolepermission import rolePermissionManager

from opennode.oms.endpoint.ssh.cmd.base import Cmd
from opennode.oms.endpoint.ssh.cmd.directives import command
from opennode.oms.endpoint.ssh.cmdline import ICmdArgumentsSyntax
from opennode.oms.endpoint.ssh.cmdline import VirtualConsoleArgumentParser
from opennode.oms.endpoint.ssh.cmd.security import require_admins_only
from opennode.oms.endpoint.ssh.cmd.security import SetAclMixin
from opennode.oms.endpoint.ssh.detached import DetachedProtocol
from opennode.oms.model.form import RawDataApplier
from opennode.oms.model.form import RawDataValidatingFactory
from opennode.oms.model.form import NoSchemaFound
from opennode.oms.model.model.base import IContainer, IIncomplete
from opennode.oms.model.model.symlink import follow_symlinks
from opennode.oms.model.schema import model_to_dict
from opennode.oms.model.schema import get_schema_fields
from opennode.oms.model.traversal import canonical_path, traverse1
from opennode.oms.security.principals import Group
from opennode.oms.security.permissions import Role
from opennode.oms.zodb import db

from opennode.knot.model.compute import IVirtualCompute
from opennode.knot.backend.compute import ShutdownComputeAction


class StopAllVmsCmd(Cmd):
    implements(ICmdArgumentsSyntax)
    command('stopvms')

    def arguments(self):
        parser = VirtualConsoleArgumentParser()
        parser.add_argument('-u', help="Stop all VMs belonging to the user")
        return parser

    @db.ro_transact
    def get_computes(self, args):
        computes = db.get_root()['oms_root']['computes']
        user_vms = []
        for c in map(follow_symlinks, computes.listcontent()):
            if not IVirtualCompute.providedBy(c):
                continue
            if c.__owner__ == args.u:
                user_vms.append(c)
        return user_vms

    @require_admins_only
    @defer.inlineCallbacks
    def execute(self, args):
        log.msg('Stopping all VMs of "%s"...' % args.u, system='stopallvms')

        computes = yield self.get_computes(args)
        for c in computes:
            self.write("Stopping %s...\n" % c)
            yield ShutdownComputeAction(c).execute(DetachedProtocol(), object())

        self.write("Stopping done. %s VMs stopped\n" % (len(computes)))
        log.msg('Stopping done. %s VMs of "%s" stopped' % (len(computes), args.u), system='stopallvms')


class ExportMetadataCmd(Cmd, SetAclMixin):
    implements(ICmdArgumentsSyntax)
    command('importexport')

    serialize_action_map = {'json': json.dumps, 'yaml': yaml.dump}
    deserialize_action_map = {'json': json.loads, 'yaml': yaml.load}

    traverse_paths = (('/machines/', True), ('/ippools/', False), ('/templates/', False),
                      ('/home/', False))

    type_blacklist = ('IncomingMachines',
                      'ByNameContainer',
                      'ActionsContainer')

    def arguments(self):
        parser = VirtualConsoleArgumentParser()
        parser.add_argument('filename', help='OS file path where the data to import from or export to')
        parser.add_argument('-i', '--import-data', action='store_true', help='Import data')
        parser.add_argument('-f', '--format', choices=['json', 'yaml'], help='Input/output file format',
                            default='yaml')
        parser.add_argument('-p', '--full-path', action='store_true', help='Add full OMS paths (export-only)',
                            default=False)
        parser.add_argument('-m', '--max-depth', type=int, help='Max path recursion depth', default=5)
        parser.add_argument('-a', '--attributes', type=list, help='List of attributes to import/export',
                            default=[])
        return parser

    @require_admins_only
    @defer.inlineCallbacks
    def execute(self, args):
        log.msg('Exporting all object ownership data...')
        if args.import_data:
            yield self.import_data(args)
        else:
            yield self.export_data(args)

    @db.transact
    def import_data(self, args):
        with open(args.filename, 'r') as f:
            serialized = f.read()
            data = self.deserialize_action_map.get(args.format)(serialized)

        for path, recursive in self.traverse_paths:
            container = traverse1(path)
            pdata = data.get(path)
            if container and pdata:
                self.write('Importing %s (%s)...\n' % (path, 'recursive' if recursive else 'non-recursive'))
                self.traverse_level_set(pdata, container, args.attributes,
                                        recursive=recursive, maxlevel=args.max_depth)

    @db.assert_transact
    def traverse_level_set(self, data, container, attrs, recursive=False, maxlevel=5, level=0):
        def import_cls(module, name):
            mod = __import__(module)
            for comp in module.split('.')[1:]:
                mod = getattr(mod, comp)
            return getattr(mod, name)

        for name, di in data.iteritems():
            self.write('%s%s\n' % (' ' * level, name))
            element = container[name]

            if di['__classname__'] in self.type_blacklist:
                continue

            obj = import_cls(di['__module__'], di['__classname__']) if not element else element

            if obj.__transient__:
                continue

            cobj = self._do_create_or_set(di, obj, attrs=attrs,
                                          marker=getattr(container, '__contains__', None))

            if cobj is None:
                continue

            if not element:
                container.add(cobj)

            if IContainer.providedBy(cobj) and recursive and level < maxlevel:
                chdata = di.get('children')
                if chdata is not None:
                    self.traverse_level_set(chdata, cobj, attrs,
                                            recursive=recursive,
                                            maxlevel=maxlevel,
                                            level=level + 1)

    attr_blacklist = ('__module__',
                      '__name__',
                      '__classname__',
                      'children',
                      'ctime',
                      'features',
                      'module',
                      'mtime',
                      'owner',
                      'permissions',
                      'tags',
                      'type',)

    def apply_form(self, form_class, data, obj, action, marker=None):
        DoesNotExist = "<NOVALUE>"
        def format_error_message(errors):
            return ('ERROR: %s while importing data for %s\n' %
                    ([(attr, data.get(attr, DoesNotExist), err) for attr, err in errors],
                     obj))

        form = form_class(data, obj, marker=marker)

        if form.errors and any(map(lambda (f, err): isinstance(err, NoSchemaFound), form.errors)):
            self.write('WARNING: import of %s failed: no schema is defined\n' % (obj))
            return

        wrong_type_errors = filter(lambda (f, err): isinstance(err, WrongType) and
                                   data.get(f, DoesNotExist) is None, form.errors)

        fields = dict(map(lambda (f, t, i): (f, t), get_schema_fields(obj, marker=marker)))

        # Attempt to fix wrong type errors by setting default values of the respective types
        for field_name, err in wrong_type_errors:
            self.write('Attempting to fix field "%s" of %s with a WrontType error...  ' % (field_name, obj))
            try:
                field = fields[field_name]

                if isinstance(field._type, tuple):
                    default = field._type[0]()
                else:
                    default = field._type()

                data[field_name] = default
            except ValueError:
                self.write('Failed!\n')
            else:
                self.write('Done.\n')

        # List missing required fields
        missing_fields = filter(lambda (f, err): isinstance(err, RequiredMissing), form.errors)
        for field_name, err in missing_fields:
            self.write('Missing required field: %s %s in %s' % (obj, field_name, data.keys()))

        # Force renewal of the validation
        delattr(form, '_errors')

        assert not form.errors, format_error_message(form.errors)
        return getattr(form, action)(ignore_readonly=True)

    @db.assert_transact
    def _do_create_or_set(self, data, obj, attrs=[], marker=None):
        if 'features' in data:
            feature_set = data['features']
        else:
            feature_set = set()

        data_filtered = dict(filter(lambda (k, v): k not in self.attr_blacklist,
                                    data.iteritems()))

        if isinstance(obj, type):
            obj = self.apply_form(RawDataValidatingFactory, data_filtered, obj, 'create', marker=marker)

            if obj is None:
                return
        else:
            self.apply_form(RawDataApplier, data_filtered, obj, 'apply', marker=marker)

        if '__name__' in data:
            obj.__name__ = data['__name__']

        if 'owner' in attrs:
            obj.__owner__ = data['owner']

        obj.features = feature_set

        #item = data.get('permissions', [])
        #clear = []
        #self.write('%s%s\n' % ('  ', item))
        #self.set_acl(obj, False, item.get('allow', []), item.get('deny', []), clear)

        return obj

    @db.ro_transact
    def export_data(self, args):
        data = {}
        for path, recursive in self.traverse_paths:
            container = traverse1(path)
            if container:
                self.write('Exporting %s (%s)...\n' % (path, 'recursive' if recursive else 'non-recursive'))
                data[path] = self.traverse_level_get(container, args.attributes,
                                                 recursive=recursive, maxlevel=args.max_depth,
                                                 add_full_paths=args.full_path)

        serialized = self.serialize_action_map.get(args.format)(data)

        with open(args.filename, 'w') as f:
            f.write(serialized)

    @db.assert_transact
    def traverse_level_get(self, container, attrs, recursive=False, maxlevel=5, level=0,
                           add_full_paths=False):
        container_data = {}
        for element in container.listcontent():
            data = self._do_cat(element, attrs=attrs)
            if IContainer.providedBy(element):
                if recursive and level < maxlevel:
                    children = self.traverse_level_get(element, attrs,
                                                       recursive=recursive,
                                                       maxlevel=maxlevel,
                                                       level=level + 1,
                                                       add_full_paths=add_full_paths)
                    if children:
                        data.update({'children': children})

                if not data:
                    continue

                container_data[data['__name__']] = data

        return container_data

    def _do_cat_acl(self, obj):
        prinrole = IPrincipalRoleManager(obj)
        auth = getUtility(IAuthentication, context=None)

        user_allow = collections.defaultdict(list)
        user_deny = collections.defaultdict(list)
        users = set()
        for role, principal, setting in prinrole.getPrincipalsAndRoles():
            users.add(principal)
            if setting.getName() == 'Allow':
                user_allow[principal].append(role)
            else:
                user_deny[principal].append(role)

        acl = {'allow': [], 'deny': []}

        for principal in users:
            def formatted_perms(perms):
                prin = auth.getPrincipal(principal)
                typ = 'group' if isinstance(prin, Group) else 'user'
                def grants(i):
                    return ','.join('@%s' % i[0] for i in rolePermissionManager.getPermissionsForRole(i)
                                    if i[0] != 'oms.nothing')
                return (typ, principal, ''.join('%s' %
                                                (Role.role_to_nick.get(i, '(%s)' % i))
                                                for i in sorted(perms)))

            if principal in user_allow:
                acl['allow'].append('%s:%s:%s' % formatted_perms(user_allow[principal]))

            if principal in user_deny:
                acl['deny'].append('%s:%s:%s' % formatted_perms(user_deny[principal]))
        return acl

    @db.assert_transact
    def _do_cat(self, obj, attrs=[], add_full_paths=False):
        items = model_to_dict(obj).items()

        data = dict((key, value) for key, value in items if obj and (key in attrs or not attrs))

        data['__name__'] = obj.__name__

        if 'owner' in attrs or not attrs:
            data['owner'] = obj.__owner__

        cls = type(removeSecurityProxy(obj))
        data['__classname__'] = cls.__name__
        data['__module__'] = cls.__module__

        if 'permissions' in attrs or not attrs:
            data['permissions'] = self._do_cat_acl(obj)

        if 'tags' in data:
            data['tags'] = list(data['tags'])

        if len(data.keys()) == 0:
            return

        if add_full_paths:
            data.update({'path': canonical_path(obj)})

        if not attrs and IIncomplete.providedBy(obj):
            data.update({'incomplete': True})

        return data
