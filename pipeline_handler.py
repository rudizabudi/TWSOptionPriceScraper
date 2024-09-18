class PipelineHandler:
    def __init__(self, core =None, tws_con=None, CC=None, DB=None):
        if None in (core, tws_con, CC, DB):
            raise Exception('<PipelineBuilder INIT> All parameters must be specified.')

        self.core = core
        self.tws_con = tws_con
        self.ContractContainer = ContractContainer
        self.db = DatabaseBroker

