
`CJSON` (Compact JSON) is Reindexer's binary format for representing JSON data internally.
Each field is encoded as a `ctag` followed by `data` — the payload bytes for that type.
In the `CJSON`-blob `ctag` is a varuint-encoded integer that contains the field type, the field name (`TagName`), and — when the value is also stored in the indexed payload — the matching payload field index.

CJSON has two closely related variants: **tuple** and **transport**. Transport CJSON (network or on-disk) inlines indexed values in the blob; `FieldIndex` always stays zero. **Tuple** CJSON (in-memory storage and queries) keeps indexed values in the flat `PayloadValue` buffer; the tuple holds only a `ctag` with `FieldIndex` pointing at the payload field (no inline bytes for scalar indexed fields; indexed arrays may carry a length prefix). Non-indexed fields still store their data inline in both variants.

## Ctag format

| Bits | Field      | Description                                                                                              |
|------|------------|----------------------------------------------------------------------------------------------------------|
| 3    | TypeTag0   | Type of field. One of TAG_XXX                                                                            |
| 12   | NameIndex  | `TagName` stored in the tag: dictionary index + 1; 0 means no name (array elements, nested objects)      |
| 10   | FieldIndex | Payload field index + 1; 0 means no indexed-field reference (`field` = -1 in the API)                    |
| 4    | Reserved   | Reserved for future use.                                                                                 |
| 3    | TypeTag1   | Additional high-order bits for the field type. Together with TypeTag0 they define the actual data type.  |


## Ctag type tag

| Name       | Value | Description                                      |
|------------|-------|--------------------------------------------------|
| TAG_VARINT | 0     | Signed integer: ZigZag and Varint                  |
| TAG_DOUBLE | 1     | IEEE 754 double (8 bytes)                        |
| TAG_STRING | 2     | Varint length + raw bytes (`string_pack`)        |
| TAG_BOOL   | 3     | Data is bool                                     |
| TAG_NULL   | 4     | Null                                             |
| TAG_ARRAY  | 5     | Data is array of elements                        |
| TAG_OBJECT | 6     | Data is object                                   |
| TAG_END    | 7     | End of object                                    |
| TAG_UUID   | 8     | 16-byte UUID                                     |
| TAG_FLOAT  | 9     | Data is number in float format (32 bits)         |


## Arrays

Arrays can be stored in 2 different ways:

- homogeneous array — all elements share one type (`TAG_ARRAY` + `atag` with element type and count; each element is written in that type's encoding, without per-element `ctag`)
- heterogeneous array — elements may differ (`TAG_ARRAY` + `atag(TAG_OBJECT, count)`; each element has its own `ctag` with empty name and no field reference)


### atag (array tag)

`atag` is a little-endian `uint32_t`: 6-bit element type in the high bits, 24-bit count in the low bits (`TTTTTTNNNNNNNNNNNNNNNNNNNNNNNN`).

| Bits | Field   | Description                                                                                              |
|------|---------|----------------------------------------------------------------------------------------------------------|
| 6    | TypeTag | Element type. `TAG_OBJECT` means a heterogeneous array; each element is prefixed with its own `ctag`    |
| 24   | Count   | Number of elements in the array                                                                          |


## Record format

```
record :=
  ctag := (TAG_OBJECT)
    [ cjson_field :=
        ctag := (TAG_VARINT,name,field) data := <zigzag + varint> |
        ctag := (TAG_DOUBLE,name,field) data := <8 bytes double> |
        ctag := (TAG_FLOAT,name,field) data := <4 bytes float> |
        ctag := (TAG_UUID,name,field) data := <16 bytes UUID> |
        ctag := (TAG_BOOL,name,field) data := <1 byte: 0 - false, 1 - true> |
        ctag := (TAG_STRING,name,field) data := <varint(length)>, <char array> |
        ctag := (TAG_NULL,name) |
        hom_array :=
          ctag := (TAG_ARRAY,name,field)
            atag := (TAG_VARINT|TAG_DOUBLE|TAG_FLOAT|TAG_UUID|TAG_BOOL|TAG_STRING, count)
              [ array_element := <scalar data encoding> ]
              ... |
        het_array :=
          ctag := (TAG_ARRAY,name,field)
            atag := (TAG_OBJECT, count)
              [ array_element := 
                ctag := (TAG_VARINT|TAG_DOUBLE|TAG_FLOAT|TAG_UUID|TAG_BOOL|TAG_STRING) data := <scalar data encoding> |
                ctag := (TAG_OBJECT) [cjson_subfield := cjson_field ...] |
                hom_array |
                het_array
              ]
              ...
        ctag := (TAG_OBJECT,name)
          [ cjson_subfield := cjson_field ]
          ...
        ctag := (TAG_END)
    ]
    ...
  ctag := (TAG_END)
```

## CJSON pack format

| # | Field                | Description                                                                     |
|---|----------------------|---------------------------------------------------------------------------------|
| 1 | Offset to names dict | `uint32_t` offset from the start of the blob; 0 if no embedded names dictionary |
| 2 | Records              | CJSON body: root `TAG_OBJECT`, closed with `TAG_END`                            |
| 3 | Names dictionary     | Present only when field 1 is non-zero; serialized `TagsMatcher` name list       |

```
names_dictionary :=
  <uint32(count)>
  [<varint(name length)>, <name bytes>]   // repeated `count` times
  ...
```

When the namespace `TagsMatcher` is bundled with the blob, the stream starts with `ctag(TAG_END)` and a `uint32_t` offset to the appended matcher update — see `ItemImpl::GetCJSON` / `FromCJSON`.


## Example of CJSON

Initial JSON:
```json
{
    "name": "Hello",
    "year": 2010,
    "articles": [1,2,3,4,5],
    "info": {
        "name" : "Info"
    }
}
```

Names dictionary (TagsMatcher; `ctag` uses `TagName` = index + 1):
```
0 - "name"     -> TagName 1
1 - "year"     -> TagName 2
2 - "articles" -> TagName 3
3 - "info"     -> TagName 4
```

In the examples below, all fields are non-indexed: `field` is 0 in the dump (no payload field reference; API value -1).

CJSON representation:
```
ctag(type: TAG_OBJECT, name: 0, field: 0)       06
  ctag(type: TAG_STRING, name: 1, field: 0)     0A
    5,"Hello"                                   05 48 65 6C 6C 6F
  ctag(type: TAG_VARINT, name: 2, field: 0)     10
    2010                                        B4 1F
  ctag(type: TAG_ARRAY, name: 3, field: 0)      1B
    atag(type: TAG_VARINT, len: 5)              05 00 00 00
      1 2 3 4 5                                 01 02 03 04 05
  ctag(type: TAG_OBJECT, name: 4, field: 0)     26
    ctag(type: TAG_STRING, name: 1, field: 0)   0A
      4,"Info"                                  04 49 6E 66 6F
  ctag(type: TAG_END, name: 0, field: 0)        07
ctag(type: TAG_END, name: 0, field: 0)          07
```



### Array representations

CJSON uses `atag` to describe an array's element type and length.

#### Homogeneous arrays

Initial JSON with homogeneous array:
```json
{
    "str_arr": ["hi","bro"]
}
```

Names dictionary:
```
0 - "str_arr" -> TagName 1
```

CJSON representation:
```
ctag(type: TAG_OBJECT, name: 0, field: 0)       06
  ctag(type: TAG_ARRAY, name: 1, field: 0)      0D
    atag(type: TAG_STRING, len: 2)              02 00 00 02
      2,"hi"                                    02 68 69
      3,"bro"                                   03 62 72 6F
ctag(type: TAG_END, name: 0, field: 0)          07
```

#### Heterogeneous arrays

To encode heterogeneous CJSON uses `atag` with `TAG_OBJECT` type and emits dedicated `ctag` for each array element.

Initial JSON with heterogeneous array:
```json
{
    "het_arr": ["hi", true, 123]
}
```

Names dictionary:
```
0 - "het_arr" -> TagName 1
```

CJSON representation:
```
ctag(type: TAG_OBJECT, name: 0, field: 0)       06
  ctag(type: TAG_ARRAY, name: 1, field: 0)      0D
    atag(type: TAG_OBJECT, len: 3)              03 00 00 06
      ctag(type: TAG_STRING, name: 0, field: 0) 02
        2,"hi"                                  02 68 69
      ctag(type: TAG_BOOL, name: 0, field: 0)   03
        1                                       01
      ctag(type: TAG_VARINT, name: 0, field: 0) 00
        123                                     F6 01
ctag(type: TAG_END, name: 0, field: 0)          07
```

Heterogeneous arrays cannot be indexed.

#### Nested arrays

Nested arrays are encoded as heterogeneous, i.e. with `TAG_OBJECT` in `atag` and dedicated `ctag` for each internal element.

JSON with nested array:
```json
{
    "mult_array": [99, [42, 2, 3], [24, 42]]
}
```

Names dictionary:
```
0 - "mult_array"` -> TagName 1
```

CJSON representation:
```
ctag(type: TAG_OBJECT, name: 0, field: 0)       06
  ctag(type: TAG_ARRAY, name: 1, field: 0)      0D
    atag(type: TAG_OBJECT, len: 3)              03 00 00 06
      ctag(type: TAG_VARINT, name: 0, field: 0) 00
        99                                      C6 01
      ctag(type: TAG_ARRAY, name: 0, field: 0)  05
        atag(type: TAG_VARINT, len: 3)          03 00 00 00
          42 2 3                                54 04 06
      ctag(type: TAG_ARRAY, name: 0, field: 0)  05
        atag(type: TAG_VARINT, len: 2)          02 00 00 00
          24 42                                 30 54
ctag(type: TAG_END, name: 0, field: 0)          07
```
