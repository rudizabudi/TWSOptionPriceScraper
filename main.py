from threading import Thread
import time

from contract_container import ContractContainer
from core import Core
from database_broker import DatabaseBroker
from pipeline_builder import PipelineBuilder
from pipeline_handler import PipelineHandler
from tws_api import TWSCon

from core import ConnectionStatus

if __name__ == '__main__':
    def main():
        core = Core(TWSCon)

        while not core.tws_con:
            time.sleep(.1)

        pl_builder = PipelineBuilder(core=core, CC=ContractContainer, DB=DatabaseBroker)

        pl_builder.startup_build_sequence()

        ph = None
        while True:
            if core.connection_status == ConnectionStatus.DISCONNECTED and ph:
                ph = None
            if core.connection_status == ConnectionStatus.CONNECTED and not ph:
                ph = PipelineHandler(core=core,  CC=ContractContainer, DB=DatabaseBroker)
            time.sleep(10)
    main()

"""
TODO: Make SQL query f-strings injection proof
TODO: Adapt local time conditions to UTC. Make TZ aware
TODO: Fix odd stocks like BRK.B or ABNB.
TODO: Improve logical load vs rebuild logic
TODO: Long-term: Switch from MSQL to InfluxDB
"""
