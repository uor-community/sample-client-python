import logging
import grpc
import os

import fire
from google.protobuf.struct_pb2 import Struct
from docker import auth

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


def create_auth_config(reference: str) -> pb2.AuthConfig:
    """
    Create an authentication configuration based on given
    reference and default configuration locations.
    This loads from ~/.docker/config.json.
    """
    auth_configs = auth.load_config()
    registry, _ = auth.resolve_repository_name(reference)
    creds = auth.resolve_authconfig(auth_configs, registry)
    if creds is not None:
        auth_config = pb2.AuthConfig(
            server_address=f"https://{registry}",
            username=creds["username"],
            password=creds["password"],
        )
        return auth_config


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

        auth_config = create_auth_config(reference)

        req = pb2.Retrieve.Request(
            source=reference,
            destination=abs_workspace,
            filter=attribute_struct,
            auth=auth_config,
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

        auth_config = create_auth_config(reference)

        req = pb2.Publish.Request(
            source=abs_workspace,
            destination=reference,
            collection=collection,
            auth=auth_config,
        )

        with grpc.insecure_channel(f"unix://{self.socket}") as channel:
            stub = pb2_grpc.CollectionManagerStub(channel)
            resp = stub.PublishContent(req)
            return resp.digest


if __name__ == "__main__":
    if os.getenv("UOR_SOCKET_ADDRESS", None):
        socket_address = os.getenv("UOR_SOCKET_ADDRESS")
    else:
        socket_address = "/var/run/uor.sock"
        logging.info(
            "Using socket location %s. To customize, use the UOR_SOCKET_ADDRESS environment variable.",
            socket_address,
        )

    sample_client = SampleClient(socket_address)
    fire.Fire(sample_client)
