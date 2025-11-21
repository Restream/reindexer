
`CJSON` (Compact JSON) is binary internal reindexer format for transparently representing JSON data.
Each field of CJSON is encoded to `ctag` - varuint, which encodes type and name of field, and `data` binary representation of field data in format dependent of type.


## Ctag format

| Bits | Field      | Description                                                                                              |
|------|------------|----------------------------------------------------------------------------------------------------------|
| 3    | TypeTag0   | Type of field. One of TAG_XXX                                                                            |
| 12   | NameIndex  | Index of field's name in names dictionary. 0 - empty name                                                |
| 10   | FieldIndex | Field index reference in internal reindexer payload. 0 - no reference.                                   |
| 4    | Reserved   | Reserved for future use.                                                                                 |
| 3    | TypeTag1   | Additional high-order bits for the field type. Together with TypeTag0 they define the actual data type.  |


## Ctag type tag

| Name       | Value | Description                                      |
|------------|-------|--------------------------------------------------|
| TAG_VARINT | 0     | Data is number in varint format                  |
| TAG_DOUBLE | 1     | Data is number in double format (64 bits)        |
| TAG_STRING | 2     | Data is string with varint length                |
| TAG_BOOL   | 3     | Data is bool                                     |
| TAG_NULL   | 4     | Null                                             |
| TAG_ARRAY  | 5     | Data is array of elements                        |
| TAG_OBJECT | 6     | Data is object                                   |
| TAG_END    | 7     | End of object                                    |
| TAG_UUID   | 8     | Data in UUID format. High bit stored in TypeTag1 |
| TAG_FLOAT  | 9     | Data is number in float format (32 bits)         |


## Arrays

Arrays can be stored in 2 different ways:

- homogeneous array, with all elements of same type (Format: TAG_ARRAY + Atag(TAG_{item} + COUNT), every item write in item format)
- heterogeneous array, with elements of various types (Format: TAG_ARRAY + Atag(TAG_OBJECT + COUNT), every item write in item format with Ctag(TAG_{item} + NameIndex=0 + FieldIndex=0))


### Atag - array tag format

Atag is 4 byte int, which encodes type and count elements in array (TTTTTTTTNNNNNNNNNNNNNNNNNNNNNNNN)

| Bits | Field   | Description                                                                                             |
|------|---------|---------------------------------------------------------------------------------------------------------|
| 6    | TypeTag | Type of array's elements. If TAG_OBJECT, than array is mixed, and each element contains individual ctag |
| 24   | Count   | Count of elements in array                                                                              |


## Record format

````
record :=
  ctag := (TAG_OBJECT,name)
    [field :=
      ctag := (TAG_VARINT,name) data := <varint> |
      ctag := (TAG_DOUBLE,name) data := <8 byte double> |
      ctag := (TAG_BOOL,name) data := <1 byte: 0 - False, 1 - True> |
      ctag := (TAG_STRING,name) data := <varint(string length)>, <char array> |
      ctag := (TAG_NULL,name) |
      ctag := (TAG_ARRAY,name)
        data := atag := TAG_OBJECT|TAG_VARINT|TAG_DOUBLE|TAG_BOOL, count)
        array := [ctag := TAG_XXX] <data>,[[ctag := TAG_XXX>]<data>]] ... |
      ctag (TAG_OBJECT,name)>
        [subfield := field]
      ...
      ctag := (TAG_END)
    ]
    ...
  ctag := (TAG_END)
````

## CJSON pack format

| # | Field                | Description                                                                |
|---|----------------------|----------------------------------------------------------------------------|
| 1 | Offset to names dict | Offset to names dictionary. 0 if there are no new names dictionary in pack |
| 2 | Records              | Tree of records. Begins with TAG_OBJECT, end TAG_END                       |
| 3 | Names dictionary     | Dictionary of field names                                                  |
```
names_dictionary :=
  <varint(start_index)>
  <varint(count)>
  [[<varint(string length)>, <name char array>],
   [<varint(string length)>, <name char array>],
   ...
  ]
```


## Example of CJSON

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
```
(TAG_OBJECT)                                    06
  (TAG_STRING,1) 5,"Hello"                      0A 05 48 65 6C 6C 6F
  (TAG_VARINT,2) 2010                           10 B4 1F
  (TAG_ARRAY,3) (TAG_VARINT,5) 1 2 3 4 5        1B 05 00 00 00 01 02 03 04 05
  (TAG_OBJECT,4)                                26
     (TAG_STRING,1) 4,"Info"                    0A 04 49 6E 66 6F
  (TAG_END)                                     07
(TAG_END)                                       07
```


### Array representations

Thus, a heterogeneous array with three elements: the first string is "hello", the second boolean value is "true", the third one is nested array, can be encoded as follows:
```json
{
    "test": ["hi",true,[7,9]]
}
```
```
(TAG_ARRAY, field index) (TAG_OBJECT, array len) (TAG_STRING, string len, char array) (TAG_BOOL, value) (TAG_ARRAY, 0) (TAG_VARINT, array len) 7 9
```
```
\065\003\000\000\006\002\002hi\003\001\005\002\000\000\000\007\011\a
```
| Value            | Descripton                     |
|------------------|--------------------------------|
| \065             | Ctag(TAG_ARRAY)                |
| \003\000\000\006 | Atag(3 items TAG_OBJECT)       |
| \002             | Ctag(TAG_STRING)               |
| \002hi           | Item string, 2 character, "hi" |
| \003             | Ctag(TAG_BOOL)                 |
| \001             | Item boolean, 'true'           |
| \005             | Ctag(TAG_ARRAY)                |
| \002\000\000\000 | Atag(2 items TAG_VARINT)       |
| \007             | Item varint, 7                 |
| \011             | Item varint, 9                 |


Homogeneous array

```json
{
    "test": ["hi","bro"]
}
```
```
(TAG_ARRAY, field index) (TAG_STRING, array len) (string len, char array) (string len, char array)
```
```
\065\002\000\000\002\002hi\003bro\a
```
| Value            | Descripton                      |
-------------------|---------------------------------|
| \065             | Ctag(TAG_ARRAY)                 |
| \002\000\000\002 | Atag(2 item TAG_STRING)         |
| \002hi           | Item string, 2 character, "hi"  |
| \003bro          | Item string, 3 character, "bro" |
