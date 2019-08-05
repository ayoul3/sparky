import argparse
from utils.common import whine
from utils.sparkSession import SparkSession


def main(results):
    target = results.spark_master
    port = int(results.spark_port)
    sparkSession = SparkSession(target, port)
    sparkSession.initContext()

    if results.info:
        whine("Gathering information about target %s:%d " % (target, port))
        print(sparkSession.getInfo())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sparky: a tool to pentest Spark clusters"
    )
    parser.add_argument("spark_master", help="The master node of a Spark cluster")
    parser.add_argument("spark_port", help="Spark port (usually 7077)", default=7077)

    group_info = parser.add_argument_group("General information")

    group_info.add_argument(
        "-i",
        "--info",
        help="Get information about the cluster: number of nodes, memory, etc.",
        action="store_true",
        default=False,
        dest="info",
    )

    results = parser.parse_args()
    main(results)
