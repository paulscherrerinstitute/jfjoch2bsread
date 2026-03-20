import math
import sys
import time

import cbor2
import numpy as np
import zmq
from bsread import CONNECT, PUSH, Sender
from dectris.compression import decompress


# Adapted from https://github.com/dectris/documentation/blob/main/stream_v2/examples/client.py
def decode_multi_dim_array(tag, order):
    dimensions, contents = tag.value
    if isinstance(contents, list):
        array = np.empty((len(contents),), dtype=object)
        array[:] = contents
    elif isinstance(contents, (np.ndarray, np.generic)):
        array = contents
    else:
        raise cbor2.CBORDecodeValueError("expected array or typed array")
    return array.reshape(dimensions, order=order)


def decode_typed_array(tag, dtype):
    if not isinstance(tag.value, bytes):
        raise cbor2.CBORDecodeValueError("expected byte string in typed array")
    return np.frombuffer(tag.value, dtype=dtype)


def decode_dectris_compression(tag):
    algorithm, elem_size, encoded = tag.value
    return decompress(encoded, algorithm, elem_size=elem_size)


tag_decoders = {
    40: lambda tag: decode_multi_dim_array(tag, order="C"),
    64: lambda tag: decode_typed_array(tag, dtype="u1"),
    65: lambda tag: decode_typed_array(tag, dtype=">u2"),
    66: lambda tag: decode_typed_array(tag, dtype=">u4"),
    67: lambda tag: decode_typed_array(tag, dtype=">u8"),
    68: lambda tag: decode_typed_array(tag, dtype="u1"),
    69: lambda tag: decode_typed_array(tag, dtype="<u2"),
    70: lambda tag: decode_typed_array(tag, dtype="<u4"),
    71: lambda tag: decode_typed_array(tag, dtype="<u8"),
    72: lambda tag: decode_typed_array(tag, dtype="i1"),
    73: lambda tag: decode_typed_array(tag, dtype=">i2"),
    74: lambda tag: decode_typed_array(tag, dtype=">i4"),
    75: lambda tag: decode_typed_array(tag, dtype=">i8"),
    77: lambda tag: decode_typed_array(tag, dtype="<i2"),
    78: lambda tag: decode_typed_array(tag, dtype="<i4"),
    79: lambda tag: decode_typed_array(tag, dtype="<i8"),
    80: lambda tag: decode_typed_array(tag, dtype=">f2"),
    81: lambda tag: decode_typed_array(tag, dtype=">f4"),
    82: lambda tag: decode_typed_array(tag, dtype=">f8"),
    83: lambda tag: decode_typed_array(tag, dtype=">f16"),
    84: lambda tag: decode_typed_array(tag, dtype="<f2"),
    85: lambda tag: decode_typed_array(tag, dtype="<f4"),
    86: lambda tag: decode_typed_array(tag, dtype="<f8"),
    87: lambda tag: decode_typed_array(tag, dtype="<f16"),
    1040: lambda tag: decode_multi_dim_array(tag, order="F"),
    56500: lambda tag: decode_dectris_compression(tag),
}


def tag_hook(decoder, tag):
    tag_decoder = tag_decoders.get(tag.tag)
    return tag_decoder(tag) if tag_decoder else tag


# Transform metadata stream from JFJoch format to bsread format
# the list of jfjoch metadata message fields is available at
# https://jungfraujoch.readthedocs.io/en/latest/CBOR.html#metadata-message
def transform_jfjoch2bsread_metadata(message):
    # bsread doesn't support bool values, so convert them to integers
    if "indexing_result" in message:
        message["indexing_result"] = int(message["indexing_result"])

    # bsread doesn't support nested dictionaries, so flatten them
    if "indexing_unit_cell" in message:
        indexing_unit_cell = message.pop("indexing_unit_cell")
        for key, value in indexing_unit_cell.items():
            message["indexing_unit_cell:" + key] = value

    # bsread needs a consistent shape for lists, so pad them to a fixed length, keep the original
    # length in a separate "az_int_profile_len" field
    if "az_int_profile" in message:
        message["az_int_profile_len"] = len(message["az_int_profile"])
        message["az_int_profile"] = np.pad(
            message["az_int_profile"], (0, 1000 - len(message["az_int_profile"]))
        )


def main():
    sender = Sender(
        port=8000,
        address="tcp://sf-daqsync-18",
        conn_type=CONNECT,
        mode=PUSH,
        queue_size=10,
        block=True,
        data_header_compression=None,
    )
    sender.open(no_client_action=None, no_client_timeout=sys.maxsize)

    zmq_context = zmq.Context(io_threads=1)
    zmq_socket = zmq_context.socket(zmq.SUB)
    zmq_socket.setsockopt_string(zmq.SUBSCRIBE, "")

    input_address = "tcp://sf-daq-2:5600"
    zmq_socket.connect(input_address)

    poller = zmq.Poller()
    poller.register(zmq_socket, zmq.POLLIN)

    time_0 = None
    pulse_0 = None

    while True:
        events = dict(poller.poll(1000))
        if zmq_socket not in events:
            continue

        message_raw = zmq_socket.recv(flags=0, copy=False, track=False)
        message = cbor2.loads(message_raw, tag_hook=tag_hook)

        if message["type"] != "metadata":
            # ignore "start" and "stop" messages, based on feedback from users
            continue

        # unpack "images" field and merge it with the rest of the message
        images = message.pop("images")
        for image in images:
            transform_jfjoch2bsread_metadata(image)
            message2send = {**message, **image}

            if time_0 is None:
                time_0 = time.time()
            if pulse_0 is None:
                pulse_0 = message2send["xfel_pulse_id"]

            # catch all nested dictionaries and remove them
            keys_to_del = []
            for key, val in message2send.items():
                if isinstance(val, dict):
                    print(key, val)
                    keys_to_del.append(key)

            for key in keys_to_del:
                del message2send[key]

            # this is a workaround to create a timestamp that is compatible with bsread
            pulse_id = message2send["xfel_pulse_id"]
            ts = time_0 + 0.01 * (pulse_id - pulse_0)
            t_secs = int(ts)
            t_nanos = int(round(math.modf(ts)[0], 3) * 1e9) + (pulse_id % 1000000)
            timestamp = (t_secs, t_nanos)

            sender.send(data=message2send, timestamp=timestamp, pulse_id=pulse_id, check_data=True)


if __name__ == "__main__":
    main()
