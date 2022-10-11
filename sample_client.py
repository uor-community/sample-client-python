import logging
import grpc
import os

import fire
from google.protobuf.struct_pb2 import Struct

import manager_pb2_grpc as pb2_grpc
import manager_pb2 as pb2

logging.basicConfig(format="%(levelname)s: %(message)s", level=logging.DEBUG)

attributes_by_pattern = {
    "*.jpg": {
        "images": True,
    },
    "*.json": {
        "metadata": True,
    },
}


def create_collection() -> pb2.Collection:
    """
    Create a collection configuration for content publishing.
    """
    files_list = []
    for pattern, attr in attributes_by_pattern.items():
        attributes_struct = Struct()
        attributes_struct.update(attr)

        for key, value in attr.items():
            logging.info(
                "Applying attribute %s=%s to file pattern %s", key, value, pattern
            )

        file = pb2.File(file=pattern, attributes=attributes_struct)
        files_list.append(file)

    return pb2.Collection(files=files_list)


class SampleClient:
    """
    SampleClient is a cli that acts as a gRPC client
    with the UOR gRPC server to retrieve and publish collection content.
    """

    def __init__(self, socket: str):
        """
        Parameters
        ----------
        socket : string
            Location of Unix Domain Socket without the unix prefix.
        """
        self.socket = socket

    def pull(self, reference: str, workspace: str, attributes: dict = None) -> str:
        """
        Pull content from UOR collection to specified workspace.

        Parameters
        ----------
        reference : string
            The registry image path (e.g. localhost:5000/image:latest)
            for the source collection.
        workspace : string
            The relative path to the output directory
            to store collection content
        attributes : dict
            A dictionary of key,value pairs to filter
            collection content when pulling.


        Returns
        -------
        string
            Content digests that were pulled as part of the operation
        """
        attribute_struct = Struct()
        if attributes is not None:
            attribute_struct.update(attributes)

        abs_workspace = os.path.abspath(workspace)
        logging.debug("Using output directory %s", abs_workspace)

        req = pb2.Retrieve.Request(
            source=reference, destination=abs_workspace, filter=attribute_struct
        )

        with grpc.insecure_channel(f"unix://{self.socket}") as channel:
            stub = pb2_grpc.CollectionManagerStub(channel)

            resp = stub.RetrieveContent(req)

            if len(resp.digests) == 0:
                logging.info("No matching collections for %s", reference)
            else:
                all_digest = "\n".join(resp.digests)
                return all_digest

    def push(self, workspace: str, reference: str) -> str:
        """
        Push content to UOR collection from specified workspace.

        Parameters
        ----------
        workspace : string
            The relative path to the input directory
            containing collection content.
        reference : string
            The registry image path (e.g. localhost:5000/image:latest)
            for the destination collection.

        Returns
        -------
        string
            The digest of the published collection.
        """

        collection = create_collection()

        abs_workspace = os.path.abspath(workspace)
        logging.debug("Using workspace %s", abs_workspace)

        req = pb2.Publish.Request(
            source=abs_workspace, destination=reference, collection=collection
        )

        with grpc.insecure_channel(f"unix://{self.socket}") as channel:
            stub = pb2_grpc.CollectionManagerStub(channel)
            resp = stub.PublishContent(req)
            return resp.digest


if __name__ == "__main__":
    socket_address = os.getenv(
        "UOR_SOCKET_ADDRESS",
        "/var/run/uor.sock",
    )

    sample_client = SampleClient(socket_address)
    fire.Fire(sample_client)
