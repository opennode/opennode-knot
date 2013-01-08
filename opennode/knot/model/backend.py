from zope.interface import Interface


class IKeyManager(Interface):

    def get_accepted_machines(self):
        pass

    def import_machines(self, accepted):
        pass
