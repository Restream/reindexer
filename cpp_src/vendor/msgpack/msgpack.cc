#include "msgpack.h"

int msgpack_pack_string(msgpack_packer* pk, const char* str, size_t length) {
	msgpack_pack_str(pk, length);
	return msgpack_pack_str_body(pk, str, length);
}
