from contract_container import ContractContainer
from core import Core
from database_broker import DatabaseBroker
from pipeline_builder import PipelineBuilder
from pipeline_handler import PipelineHandler
from tws_api import TWSCon


if __name__ == '__main__':
    def main():
        core = Core()

        tws_con = TWSCon(core)

        pl_builder = PipelineBuilder(core=core, tws_con=tws_con, CC=ContractContainer, DB=DatabaseBroker)
        pl_handler = PipelineHandler(core=core, tws_con=tws_con, CC=ContractContainer, DB=DatabaseBroker)

        pl_builder.startup_build_sequence()

    main()

"""
TODO: Make SQL query f-strings injection proof
TODO: Adapt local time conditions to UTC. Make TZ aware
TODO: Fix odd stocks like BRK.B or ABNB.
TODO: Improve logical load vs rebuild logic
TODO: Long-term: Switch from MSQL to InfluxDB
"""
