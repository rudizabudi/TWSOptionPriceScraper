from datetime import datetime
from threading import Thread
import pickle
import time


from core import Core
from tws_api import TWSCon
from pipeline_builder import PipelineBuilder
from pipeline_handler import PipelineHandler
from contract_container import ContractContainer
from database_broker import DatabaseBroker


#def request(core):


if __name__ == '__main__':
    def main():
        core = Core()

        tws_con = TWSCon(core)
        #core.set_TWSCon(tws_con)
        pl_builder = PipelineBuilder(core=core, tws_con=tws_con, CC=ContractContainer, DB=DatabaseBroker)
        #tester = pl_builder.tester()
        pl_builder.build_all_contracts()

        ph = PipelineHandler(core=core, tws_con=tws_con, CC=ContractContainer, DB=DatabaseBroker)
        #ph_thread = Thread(target=ph.pipeline_handler, daemon=True, kwargs={core=core, tws_con=tws_con, ContractContainer=ContractContainer, DatabaseBroker=DatabaseBroker})
        #request(core)


    main()

"""
TODO: CHECK IF DOUBLE WRAPPED SQL QUERIES HANDLE SQL CONNECTION ACCORDINGLY
"""
