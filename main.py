from datetime import datetime, timedelta
from time import sleep

from contract_container import ContractContainer
from core import Core
from database_broker import DatabaseBroker
from pipeline_builder import PipelineBuilder
from pipeline_handler import PipelineHandler
from tws_api import TWSCon


if __name__ == '__main__':
    def main():
        core: Core = Core()
        tws_con: TWSCon = TWSCon(core=core)

        while not core.tws_con:
            sleep(.1)

        pl_builder: PipelineBuilder = PipelineBuilder(core=core, CC=ContractContainer, DB=DatabaseBroker)
        pl_builder.startup_build_sequence()

        pl_handler: PipelineHandler = PipelineHandler(core=core, CC=ContractContainer, DB=DatabaseBroker)

        while True:
            if not tws_con.isConnected():

                core.write_tws_connection(tws_con)

                tws_con = TWSCon(core=core)
                if core.last_request - core.last_receive > timedelta(seconds=core.glitch_detector_threshold):
                    raise Warning(f'TWS API might not send data any longer.')

                sleep(10)

            sleep(10)

    main()

"""
TODO: Make SQL query f-strings injection proof
TODO: Adapt local time conditions to UTC. Make TZ aware
TODO: Fix odd stocks like BRK.B or ABNB.
TODO: Improve logical load vs rebuild logic
TODO: Long-term: Switch from MSQL to InfluxDB/postgresql
TODO: Add TWS Gateway restart in main.py loop if it's not responding/glitching
TODO: Switch from threading to Python3.13 open GIL 
TODO: Make constituents check and option_list_creation (-> new SQL tables) periodical
TODO: Containerize anew
"""

