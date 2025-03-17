from datetime import datetime, timedelta
import psutil
from threading import Thread
import time

from contract_container import ContractContainer
from core import Core
from database_broker import DatabaseBroker
from pipeline_builder import PipelineBuilder
from pipeline_handler import PipelineHandler
from tws_api import TWSCon

from core import ConnectionStatus, tprint

if __name__ == '__main__':
    def main():
        core = Core()
        tws_con = TWSCon(core=core)

        while not core.tws_con:
            time.sleep(.1)

        pl_builder = PipelineBuilder(core=core, CC=ContractContainer, DB=DatabaseBroker)

        pl_builder.startup_build_sequence()
        PipelineHandler(core=core, CC=ContractContainer, DB=DatabaseBroker)

        while True:
            # if core.connection_status == ConnectionStatus.DISCONNECTED and ph:
            #     tprint('Main cond 1')
            #     ph = None
            # if core.connection_status == ConnectionStatus.CONNECTED and not ph:
            #     tprint('Main cond 2')
            #     ph = PipelineHandler(core=core,  CC=ContractContainer, DB=DatabaseBroker)

            if not tws_con.isConnected():
                # tprint('Waiting for ibgateway to launch...')
                # while len(list(filter(lambda x: x.name() == 'ibgateway.exe', psutil.process_iter()))) == 0:
                #     time.sleep(1)
                # tprint('Ibgateway has launched.')
                #
                # ibgt_process = list(filter(lambda x: x.name() == 'ibgateway.exe', psutil.process_iter()))[0]
                # launch_offset = 30
                #
                # wait_time = datetime.fromtimestamp(ibgt_process.create_time()) + timedelta(seconds=launch_offset)
                # tprint(f'Main cond 3.0 {tws_con_thread.is_alive()}, {tws_con.isConnected()}, {core.connection_status} ')
                #
                # while datetime.now() < wait_time:
                #     time.sleep(1)

                tws_con = None
                core.write_tws_connection(tws_con)

                tws_con = TWSCon(core=core)
                time.sleep(10)

            time.sleep(10)

    main()

"""
TODO: Make SQL query f-strings injection proof
TODO: Adapt local time conditions to UTC. Make TZ aware
TODO: Fix odd stocks like BRK.B or ABNB.
TODO: Improve logical load vs rebuild logic
TODO: Long-term: Switch from MSQL to InfluxDB
"""
