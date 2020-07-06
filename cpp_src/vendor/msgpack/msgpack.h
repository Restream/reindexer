/*
 * MessagePack for C
 *
 * Copyright (C) 2008-2009 FURUHASHI Sadayuki
 *
 *    Distributed under the Boost Software License, Version 1.0.
 *    (See accompanying file LICENSE_1_0.txt or copy at
 *    http://www.boost.org/LICENSE_1_0.txt)
 */
/**
 * @defgroup msgpack MessagePack C
 * @{
 * @}
 */

#ifndef MSGPACK_H_
#define MSGPACK_H_

#ifndef _MSC_VER
#ifdef __cplusplus
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
#endif
#else
#pragma warning(push)
#pragma warning(disable : 4267 4146)
#endif

#include "msgpack/object.h"
#include "msgpack/pack.h"
#include "msgpack/sbuffer.h"
#include "msgpack/unpack.h"
#include "msgpack/util.h"
#include "msgpack/version.h"
#include "msgpack/vrefbuffer.h"
#include "msgpack/zone.h"

int msgpack_pack_string(msgpack_packer* pk, const char* str, size_t length);

#endif
