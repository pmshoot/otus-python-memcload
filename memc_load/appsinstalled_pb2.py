import sys

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database

_b = sys.version_info[0] < 3 and (lambda x: x) or (lambda x: x.encode('latin1'))

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()

DESCRIPTOR = _descriptor.FileDescriptor(
        name='appsinstalled.proto',
        package='',
        syntax='proto2',
        serialized_pb=_b(
            '\n\x13\x61ppsinstalled.proto\"2\n\x08UserApps\x12\x0c\n\x04\x61pps\x18\x01 \x03(\r\x12\x0b\n\x03lat\x18\x02 \x01(\x01\x12\x0b\n\x03lon\x18\x03 \x01(\x01')
)

_USERAPPS = _descriptor.Descriptor(
        name='UserApps',
        full_name='UserApps',
        filename=None,
        file=DESCRIPTOR,
        containing_type=None,
        fields=[
            _descriptor.FieldDescriptor(
                    name='apps', full_name='UserApps.apps', index=0,
                    number=1, type=13, cpp_type=3, label=3,
                    has_default_value=False, default_value=[],
                    message_type=None, enum_type=None, containing_type=None,
                    is_extension=False, extension_scope=None,
                    options=None),
            _descriptor.FieldDescriptor(
                    name='lat', full_name='UserApps.lat', index=1,
                    number=2, type=1, cpp_type=5, label=1,
                    has_default_value=False, default_value=float(0),
                    message_type=None, enum_type=None, containing_type=None,
                    is_extension=False, extension_scope=None,
                    options=None),
            _descriptor.FieldDescriptor(
                    name='lon', full_name='UserApps.lon', index=2,
                    number=3, type=1, cpp_type=5, label=1,
                    has_default_value=False, default_value=float(0),
                    message_type=None, enum_type=None, containing_type=None,
                    is_extension=False, extension_scope=None,
                    options=None),
        ],
        extensions=[
        ],
        nested_types=[],
        enum_types=[
        ],
        options=None,
        is_extendable=False,
        syntax='proto2',
        extension_ranges=[],
        oneofs=[
        ],
        serialized_start=23,
        serialized_end=73,
)

DESCRIPTOR.message_types_by_name['UserApps'] = _USERAPPS
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

UserApps = _reflection.GeneratedProtocolMessageType('UserApps', (_message.Message,), dict(
        DESCRIPTOR=_USERAPPS,
        __module__='appsinstalled_pb2'
        # @@protoc_insertion_point(class_scope:UserApps)
))
_sym_db.RegisterMessage(UserApps)

# @@protoc_insertion_point(module_scope)
