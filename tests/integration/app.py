from faust_bootstrap.core.app import FaustApplication


class FaustAppTest(FaustApplication):

    def get_unique_app_id(self):
        return f"dummy-group"

    def setup_topics(self):
        ...

    def build_topology(self):
        ...
