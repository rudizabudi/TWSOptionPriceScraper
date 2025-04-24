from datetime import timedelta
from time import sleep

from core import Core, tprint, kill_ibgateway, start_ibgateway
from pipeline_builder import PipelineBuilder
from pipeline_handler import PipelineHandler
from tws_api import TWSCon

if __name__ == '__main__':
    def main():
        core: Core = Core()

        if core.USE_IBC:
            start_ibgateway(core)

        tws_con: TWSCon = TWSCon(core=core)

        while not core.tws_con:
            sleep(.1)

        PipelineBuilder().startup_build_sequence()

        PipelineHandler()

        while core.startup:
            sleep(1)

        sleep(60)

        while True:
            if not tws_con.isConnected():
                tws_con = TWSCon(core=core)
                # core.write_tws_connection(tws_con)
                sleep(10)

            try:
                if core.last_request - core.last_receive > timedelta(seconds=core.GLITCH_DETECTOR_THRESHOLD):
                    tprint(f'TWS API might not send data any longer.')
                    if core.USE_IBC:
                        kill_ibgateway(core)
                        start_ibgateway(core)
            except TypeError:
                pass

            sleep(10)

    main()

"""
TODO: Make SQL query f-strings injection proof
TODO: Fix odd stocks like BRK.B or ABNB.
TODO: Improve logical load vs rebuild logic
TODO: Long-term: Switch from MSQL to InfluxDB/postgresql
TODO: Switch from threading to Python3.13 open GIL 
TODO: Make constituents check and option_list_creation (-> new SQL tables) periodical
TODO: Containerize anew
TODO: Add manual sql_maintenance.py maintenance functions to controller loop
TODO: Split exp_option date logic into creation and finish. 
TODO: Find a cleaner way to close IBC instead of killing JVM instance


DONE: Adapt local time conditions to UTC. Make TZ aware
DONE: Add TWS Gateway restart in main.py loop if it's not responding/glitching
DONE: Rebuild constituents loader.
DONE: .env input validation
"""
