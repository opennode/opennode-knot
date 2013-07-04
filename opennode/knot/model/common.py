from zope.interface import Interface

class IInVirtualizationContainer(Interface):
    pass


class IPreDeployHook(Interface):

    def execute(self, *args, **kwargs):
        pass

class IPostUndeployHook(Interface):

    def execute(self, *args, **kwargs):
        pass
